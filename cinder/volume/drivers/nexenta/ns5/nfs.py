# Copyright 2021 Nexenta by DDN, Inc. All rights reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import ipaddress
import os
import posixpath
import sys
import uuid

from os_brick.remotefs import remotefs
from oslo_concurrency import processutils
from oslo_log import log as logging
from oslo_utils import strutils
from oslo_utils import units
import six

from cinder import coordination
from cinder import exception
from cinder.i18n import _
from cinder.image import image_utils
from cinder import interface
from cinder import objects
from cinder.privsep import fs
from cinder import utils as cinder_utils
from cinder.volume.drivers.nexenta import image
from cinder.volume.drivers.nexenta.ns5 import jsonrpc
from cinder.volume.drivers.nexenta import options
from cinder.volume.drivers.nexenta import utils
from cinder.volume.drivers import nfs
from cinder.volume import volume_types
from cinder.volume import volume_utils

LOG = logging.getLogger(__name__)


@interface.volumedriver
class NexentaNfsDriver(nfs.NfsDriver):
    """Executes volume driver commands on Nexenta Appliance.

    Version history:

    .. code-block:: none

        1.0.0 - Initial driver version.
        1.1.0 - Support for extend volume.
        1.2.0 - Added HTTPS support.
              - Added use of sessions for REST calls.
              - Added abandoned volumes and snapshots cleanup.
        1.3.0 - Failover support.
        1.4.0 - Migrate volume support and new NEF API calls.
        1.5.0 - Revert to snapshot support.
        1.6.0 - Get mountPoint from API to support old style mount points.
              - Mount and umount shares on each operation to avoid mass
                mounts on controller. Clean up mount folders on delete.
        1.6.1 - Fixed volume from image creation.
        1.6.2 - Removed redundant share mount from initialize_connection.
        1.6.3 - Adapted NexentaException for the latest Cinder.
        1.6.4 - Fixed volume mount/unmount.
        1.6.5 - Added driver_ssl_cert_verify for HA failover.
        1.6.6 - Destroy unused snapshots after deletion of it's last clone.
        1.6.7 - Fixed volume migration for HA environment.
        1.6.8 - Added deferred deletion for snapshots.
        1.6.9 - Fixed race between volume/clone deletion.
        1.7.0 - Added consistency group support.
        1.7.1 - Removed redundant hpr/activate call from initialize_connection.
        1.7.2 - Merged upstream changes for umount.
        1.8.0 - Refactored NFS driver.
              - Added pagination support.
              - Added configuration parameters for REST API connect/read
                timeouts, connection retries and backoff factor.
              - Fixed HA failover.
              - Added retries on EBUSY errors.
              - Fixed HTTP authentication.
              - Disabled non-blocking mandatory locks.
              - Added coordination for dataset operations.
        1.8.1 - Support for NexentaStor tenants.
        1.8.2 - Added manage/unmanage/manageable-list volume/snapshot support.
        1.8.3 - Added consistency group capability to generic volume group.
        1.8.4 - Refactored storage assisted volume migration.
              - Added support for volume retype.
              - Added support for volume type extra specs.
              - Added vendor capabilities support.
        1.8.5 - Fixed NFS protocol version for generic volume migration.
        1.8.6 - Fixed post-migration volume mount.
        1.8.7 - Added workaround for pagination.
        1.8.8 - Improved error messages.
              - Improved compatibility with initial driver versions.
              - Added throttle for storage assisted volume migration.
        1.8.9 - Added support for different volume formats.
              - Added space reservation for thick provisioned volumes.
              - Fixed renaming volume after generic migration.
              - Fixed properties for volumes created from snapshot.
              - Allow retype volume to another provisioning type.
        1.9.0 - Added image caching using clone_image method.
        1.9.1 - Added flag backend_state to report backend status.
              - Added retry on driver initialization failure.
              - Added QoS support in terms of I/O throttling rate.
        1.9.2 - Added support for NexentaStor5 vSolution API.
        1.9.3 - Added support for NAS secure operations.
        1.9.4 - Added support for nohide NFS option.
              - Fixed concurrency issues.
        1.9.5 - Fixed issue with retries when mounting an NFS share.
        1.9.6 - Added support for encrypted volumes.
    """

    VERSION = '1.9.6'
    CI_WIKI_NAME = "Nexenta_CI"

    vendor_name = 'Nexenta'
    product_name = 'NexentaStor5'
    storage_protocol = 'NFS'
    driver_volume_type = 'nfs'

    def __init__(self, execute=processutils.execute, *args, **kwargs):
        self._remotefsclient = None
        super(NexentaNfsDriver, self).__init__(*args, **kwargs)
        if not self.configuration:
            code = 'ENODATA'
            message = (_('%(product_name)s %(storage_protocol)s '
                         'backend configuration not found')
                       % {'product_name': self.product_name,
                          'storage_protocol': self.storage_protocol})
            raise jsonrpc.NefException(code=code, message=message)
        self.configuration.append_config_values(options.NEXENTASTOR5_NFS_OPTS)
        root_helper = cinder_utils.get_root_helper()
        mount_point_base = self.configuration.nexenta_mount_point_base
        self.mount_point_base = os.path.realpath(mount_point_base)
        nfs_options = self.configuration.safe_get('nfs_mount_options')
        nas_options = self.configuration.safe_get('nas_mount_options')
        if nfs_options and nas_options:
            LOG.debug('Overriding NFS mount options: '
                      'nfs_mount_options = "%(nfs_options)s" with '
                      'nas_mount_options = "%(nas_options)s"',
                      {'nfs_options': nfs_options,
                       'nas_options': nas_options})
        if nas_options:
            self.mount_options = nas_options
        elif nfs_options:
            self.mount_options = nfs_options
        else:
            self.mount_options = None
        self._remotefsclient = remotefs.RemoteFsClient(
            self.driver_volume_type,
            root_helper, execute=execute,
            nfs_mount_point_base=self.mount_point_base,
            nfs_mount_options=self.mount_options)
        self.nef = None
        self.ctxt = None
        self.nas_stat = None
        self.nas_share = None
        self.nas_nohide = False
        self.nas_mntpoint = None
        self.backend_name = self._get_backend_name()
        self.nas_driver = self.__class__.__name__
        self.nas_host = self.configuration.nas_host
        self.nas_path = self.configuration.nas_share_path
        self.nas_pool = self.nas_path.split(posixpath.sep)[0]
        self.image_cache = self.configuration.nexenta_image_cache
        self.nbmand = self.configuration.nexenta_nbmand
        self.smart_compression = (
            self.configuration.nexenta_smart_compression)
        self.group_snapshot_template = (
            self.configuration.nexenta_group_snapshot_template)
        self.origin_snapshot_template = (
            self.configuration.nexenta_origin_snapshot_template)
        self.cache_image_template = (
            self.configuration.nexenta_cache_image_template)
        self.cache_snapshot_template = (
            self.configuration.nexenta_cache_snapshot_template)
        self.migration_service_prefix = (
            self.configuration.nexenta_migration_service_prefix)
        self.migration_throttle = (
            self.configuration.nexenta_migration_throttle)
        self.nas_secure_file_operations = (
            self.configuration.nas_secure_file_operations.lower())
        self.nas_secure_file_permissions = (
            self.configuration.nas_secure_file_permissions.lower())

    @staticmethod
    def get_driver_options():
        return options.NEXENTASTOR5_NFS_OPTS

    def do_setup(self, ctxt):
        self.ctxt = ctxt
        retries = 0
        while not self._do_setup():
            retries += 1
            self.nef.delay(retries)

    def _do_setup(self):
        try:
            self.nef = jsonrpc.NefProxy(self.driver_volume_type,
                                        self.nas_pool,
                                        self.nas_path,
                                        self.backend_name,
                                        self.configuration)
        except jsonrpc.NefException as error:
            LOG.error('Failed to initialize RESTful API for backend '
                      '%(backend_name)s on host %(host)s: %(error)s',
                      {'backend_name': self.backend_name,
                       'host': self.host,
                       'error': error})
            return False
        return True

    def check_for_setup_error(self):
        secure_options = {
            'nas_secure_file_operations': self.nas_secure_file_operations,
            'nas_secure_file_permissions': self.nas_secure_file_permissions
        }
        for option_name, option_value in secure_options.items():
            if option_value not in ['auto', 'true', 'false']:
                code = 'EINVAL'
                message = (_('Invalid value %(option_value)s for '
                             'configuration option %(option_name)s '
                             'defined for backend %(backend_name)s')
                           % {'option_value': option_value,
                              'option_name': option_name,
                              'backend_name': self.backend_name})
                raise jsonrpc.NefException(code=code, message=message)
        self.set_nas_security_options(self._is_voldb_empty_at_startup)
        retries = 0
        while not self._check_for_setup_error():
            retries += 1
            self.nef.delay(retries)

    def _check_for_setup_error(self):
        """Check root filesystem, NFS service and NFS share."""
        payload = {'fields': 'path'}
        try:
            self.nef.filesystems.get(self.nas_pool, payload)
        except jsonrpc.NefException as error:
            LOG.error('Failed to get stat of NAS pool %(nas_pool)s: %(error)s',
                      {'nas_pool': self.nas_pool, 'error': error})
            return False
        items = self.nef.filesystems.properties
        names = [item['api'] for item in items if 'api' in item]
        names += ['mountPoint', 'isMounted']
        fields = ','.join(names)
        payload = {'fields': fields}
        try:
            self.nas_stat = self.nef.filesystems.get(self.nas_path, payload)
        except jsonrpc.NefException as error:
            LOG.error('Failed to get stat of NAS path %(nas_path)s: %(error)s',
                      {'nas_path': self.nas_path, 'error': error})
            return False
        if self.nas_stat['mountPoint'] == 'none':
            LOG.error('NAS path %(nas_path)s is not writable',
                      {'nas_path': self.nas_path})
            return False
        if not self.nas_stat['isMounted']:
            LOG.error('NAS path %(nas_path)s is not mounted',
                      {'nas_path': self.nas_path})
            return False
        payload = {'fields': 'state'}
        try:
            service = self.nef.services.get('nfs', payload)
        except jsonrpc.NefException as error:
            LOG.error('Failed to get state of NFS service: %(error)s',
                      {'error': error})
            return False
        if service['state'] != 'online':
            LOG.error('NFS service is not online: %(state)s',
                      {'state': service['state']})
            return False
        payload = {'fields': 'nohide,shareState'}
        try:
            share = self.nef.nfs.get(self.nas_path, payload)
        except jsonrpc.NefException as error:
            LOG.error('Failed to get state of NFS share %(share)s: %(error)s',
                      {'share': self.nas_path, 'error': error})
            return False
        if share['shareState'] != 'online':
            LOG.error('NFS share %(share)s is not online: %(state)s',
                      {'share': self.nas_path,
                       'state': share['shareState']})
            return False
        self.nas_nohide = share['nohide']
        self.nas_share = '%(host)s:%(path)s' % {
            'host': self.nas_host,
            'path': self.nas_stat['mountPoint']
        }
        if self.nas_nohide:
            try:
                self.nas_mntpoint = self._mount_share(self.nas_share)
            except Exception as error:
                LOG.error('Failed to enable nohide feature: %(error)s',
                          {'error': error})
                self.nas_nohide = False
        payload = {}
        if self.nas_stat['nonBlockingMandatoryMode'] != self.nbmand:
            payload['nonBlockingMandatoryMode'] = self.nbmand
        if self.nas_stat['smartCompression'] != self.smart_compression:
            payload['smartCompression'] = self.smart_compression
        if not payload:
            return True
        try:
            self.nef.filesystems.set(self.nas_path, payload)
        except jsonrpc.NefException as error:
            LOG.error('Failed to set NAS path %(nas_path)s '
                      'properties %{payload}s: %(error)s',
                      {'nas_path': self.nas_path,
                       'payload': payload, 'error': error})
            return False
        self.nas_stat.update(payload)
        return True

    def set_nas_security_options(self, is_new_cinder_install):
        """Determine the setting to use for Secure NAS options.

        Value of each NAS Security option is checked and updated. If the
        option is currently 'auto', then it is set to either true or false
        based upon if this is a new Cinder installation. The RemoteFS variable
        '_execute_as_root' will be updated for this driver.

        :param is_new_cinder_install: bool indication of new Cinder install
        """
        if self.nas_secure_file_operations in ['auto', 'true']:
            self.nas_secure_file_operations = True
            self._execute_as_root = False
        else:
            self.nas_secure_file_operations = False
            self._execute_as_root = True
        if self.nas_secure_file_permissions in ['auto', 'false']:
            self.nas_secure_file_permissions = False
        else:
            self.nas_secure_file_permissions = True
        LOG.debug('Configured NAS security options for backend %(backend)s: '
                  'nas_secure_file_operations: %(secure_file_operations)s, '
                  'nas_secure_file_permissions: %(secure_file_permissions)s, '
                  'execute_as_root: %(execute_as_root)s',
                  {'backend': self.backend_name,
                   'secure_file_operations': self.nas_secure_file_operations,
                   'secure_file_permissions': self.nas_secure_file_permissions,
                   'execute_as_root': self._execute_as_root})

    def secure_file_operations_enabled(self):
        """Determine if driver is operating in Secure File Operations mode.

        The Cinder Volume driver needs to query if this driver is operating
        in a secure file mode; check our nas_secure_file_operations flag.
        """
        return self.nas_secure_file_operations

    def _update_volume_props(self, volume, volume_type=None):
        """Updates the existing volume properties.

        :param volume: volume reference
        :param volume_type: new volume type
        """
        volume_path = self._get_volume_path(volume)
        items = self.nef.filesystems.properties
        names = [item['api'] for item in items if 'api' in item]
        # Workaround for NEX-21595
        names += ['referencedReservationSize', 'source']
        fields = ','.join(names)
        payload = {'fields': fields, 'source': True}
        props = self.nef.filesystems.get(volume_path, payload)
        src = props['source']
        reservation = props['referencedReservationSize']
        specs = self._get_volume_specs(volume, volume_type)
        payload = {}
        for item in items:
            if 'api' not in item:
                continue
            api = item['api']
            if api in specs:
                value = specs[api]
                if props[api] == value:
                    continue
                payload[api] = value
            elif src[api] in ['local', 'received']:
                if props[api] == item['default']:
                    continue
                if 'inherit' in item:
                    LOG.debug('Unable to inherit property %(name)s '
                              'for volume %(volume)s. %(reason)s',
                              {'name': item['name'],
                               'volume': volume['name'],
                               'reason': item['inherit']})
                    continue
                payload[api] = None
        if payload:
            self.nef.filesystems.set(volume_path, payload)
        specs = self._get_image_specs(volume, volume_type)
        file_size = volume['size'] * units.Gi
        file_format = specs['format']
        file_sparse = specs['sparse']
        volume_image = image.VolumeImage(self, volume, specs)
        volume_image.reload(file_size=True, file_format=True)
        volume_image.change(file_size=file_size, file_format=file_format)
        if file_sparse:
            file_size = 0
        if reservation != file_size:
            self._set_volume_reservation(volume, file_size, file_format)

    def _get_volume_reservation(self, volume, file_size, file_format):
        """Calculates the correct reservation size for given volume size.

        Its purpose is to reserve additional space for volume metadata
        so volume don't unexpectedly run out of room. This function is
        a copy of the volsize_to_reservation function in libzfs_dataset.c
        and qcow2_calc_prealloc_size function in qcow2.c

        :param volume: volume reference
        :param file_size: volume file size in bytes
        :param file_format: volume file format
        :returns: reservation size
        """
        volume_path = self._get_volume_path(volume)
        payload = {'fields': 'recordSize,dataCopies'}
        props = self.nef.filesystems.get(volume_path, payload)
        blocksize = props['recordSize']
        ncopies = props['dataCopies']
        reservation = file_size
        numdb = 7
        dn_max_indblkshift = 17
        spa_blkptrshift = 7
        spa_dvas_per_bp = 3
        dnodes_per_level_shift = dn_max_indblkshift - spa_blkptrshift
        dnodes_per_level = 1 << dnodes_per_level_shift
        nblocks = file_size // blocksize
        while nblocks > 1:
            nblocks += dnodes_per_level - 1
            nblocks //= dnodes_per_level
            numdb += nblocks
        numdb *= min(spa_dvas_per_bp, ncopies + 1)
        reservation *= ncopies
        numdb *= 1 << dn_max_indblkshift
        reservation += numdb
        if file_format == image.FORMAT_RAW:
            file_meta = 0
        elif file_format == image.FORMAT_QCOW:
            file_meta = 48 + 4 * file_size // units.Mi
        elif file_format == image.FORMAT_QCOW2:
            cluster_size = 64 * units.Ki
            refcount_size = 4
            int_size = (sys.maxsize.bit_length() + 1) // 8
            file_meta = 0
            aligned_file_size = utils.roundup(file_size, cluster_size)
            file_meta += cluster_size
            blocks_per_table = cluster_size // int_size
            clusters = aligned_file_size // cluster_size
            nl2e = utils.roundup(clusters, blocks_per_table)
            file_meta += nl2e * int_size
            clusters = nl2e * int_size // cluster_size
            nl1e = utils.roundup(clusters, blocks_per_table)
            file_meta += nl1e * int_size
            clusters = (aligned_file_size + file_meta) // cluster_size
            refcounts_per_block = 8 * cluster_size // (1 << refcount_size)
            table = blocks = first = 0
            last = 1
            while first != last:
                last = first
                first = clusters + blocks + table
                blocks = utils.divup(first, refcounts_per_block)
                table = utils.divup(blocks, blocks_per_table)
                first = clusters + blocks + table
            file_meta += (blocks + table) * cluster_size
        elif file_format == image.FORMAT_PARALLELS:
            file_meta = (1 + file_size // units.Gi // 256) * units.Mi
        elif file_format == image.FORMAT_VDI:
            file_meta = 512 + 4 * file_size // units.Mi
        elif file_format == image.FORMAT_VHDX:
            file_meta = 8 * units.Mi
        elif file_format == image.FORMAT_VMDK:
            file_meta = 192 * (units.Ki + file_size // units.Mi)
        elif file_format == image.FORMAT_VPC:
            file_meta = 512 + 2 * (units.Ki + file_size // units.Mi)
        elif file_format == image.FORMAT_QED:
            file_meta = 320 * units.Ki
        else:
            code = 'EINVAL'
            message = (_('Volume format %(format)s is not supported')
                       % {'format': file_format})
            raise jsonrpc.NefException(code=code, message=message)
        zfs_meta = reservation - file_size
        reservation += file_meta
        LOG.debug('Reservation for %(format)s volume %(volume)s: '
                  '%(reservation)s, volume file size: %(file_size)s, '
                  'volume metadata size: %(zfs_meta)s and '
                  'volume file metadata size: %(file_meta)s',
                  {'format': file_format, 'volume': volume['name'],
                   'reservation': reservation, 'file_size': file_size,
                   'zfs_meta': zfs_meta, 'file_meta': file_meta})
        return reservation

    def _set_volume_reservation(self, volume, file_size, file_format):
        reservation = 0
        if file_size:
            reservation = self._get_volume_reservation(
                volume, file_size, file_format)
        volume_path = self._get_volume_path(volume)
        payload = {'referencedReservationSize': reservation}
        try:
            self.nef.filesystems.set(volume_path, payload)
        except jsonrpc.NefException as error:
            LOG.error('Failed to set %(format)s volume %(volume)s '
                      'reservation to %(reservation)s: %(error)s',
                      {'format': file_format,
                       'volume': volume['name'],
                       'reservation': reservation,
                       'error': error})
            raise

    def _create_volume(self, volume):
        volume_path = self._get_volume_path(volume)
        payload = self._get_volume_specs(volume)
        payload['path'] = volume_path
        self.nef.filesystems.create(payload)
        self._set_volume_acl(volume)
        return volume_path

    def create_volume(self, volume):
        """Creates a volume.

        :param volume: volume reference
        """
        volume_path = self._create_volume(volume)
        specs = self._get_image_specs(volume)
        file_size = volume['size'] * units.Gi
        file_format = specs['format']
        file_sparse = specs['sparse']
        file_vsolution = specs['vsolution']
        if not file_sparse:
            self._set_volume_reservation(volume, file_size, file_format)
        payload = {'size': file_size}
        if file_vsolution and file_format == image.FORMAT_RAW:
            if self.nas_secure_file_permissions:
                payload['mode'] = '660'
            self.nef.vsolutions.create(volume_path, image.FILE_NAME, payload)
        else:
            volume_image = image.VolumeImage(self, volume, specs)
            volume_image.create()

    @coordination.synchronized('{self.nef.lock}-{volume[id]}')
    def copy_image_to_volume(self, ctxt, volume, image_service, image_id):
        specs = self._get_image_specs(volume)
        LOG.debug('Copy image %(image)s to %(format)s volume %(volume)s',
                  {'image': image_id, 'format': specs['format'],
                   'volume': volume['name']})
        volume_image = image.VolumeImage(self, volume, specs)
        volume_image.download(ctxt, image_service, image_id)

    @coordination.synchronized('{self.nef.lock}-{volume[id]}')
    def copy_image_to_encrypted_volume(self, ctxt, volume, image_service,
                                       image_id):
        specs = self._get_image_specs(volume)
        LOG.debug('Copy image %(image)s to %(format)s encrypted '
                  'volume %(volume)s',
                  {'image': image_id, 'format': specs['format'],
                   'volume': volume['name']})
        volume_image = image.VolumeImage(self, volume, specs)
        volume_image.download(ctxt, image_service, image_id)

    @coordination.synchronized('{self.nef.lock}-{image_meta[id]}')
    def copy_volume_to_image(self, ctxt, volume, image_service, image_meta):
        specs = self._get_image_specs(volume)
        LOG.debug('Copy %(format)s volume %(volume)s to image %(image)s',
                  {'format': specs['format'], 'volume': volume['name'],
                   'image': image_meta['id']})
        volume_image = image.VolumeImage(self, volume, specs)
        volume_image.reload(file_format=True)
        volume_image.upload(ctxt, image_service, image_meta)

    @coordination.synchronized('{self.nef.lock}-{cache_name}')
    def _delete_cache(self, cache_name, cache_path, snapshot_path):
        """Delete a image cache.

        :param cache_name: cache volume name
        :param cache_path: cache volume path
        :param snapshot_path: cache snapshot path
        """
        payload = {'fields': 'clones'}
        try:
            props = self.nef.snapshots.get(snapshot_path, payload)
        except jsonrpc.NefException as error:
            LOG.error('Failed to get clones of image cache '
                      '%(name)s: %(error)s',
                      {'name': cache_name, 'error': error})
            return
        if props['clones']:
            clones = props['clones']
            count = len(clones)
            LOG.debug('Image cache %(name)s is still in use, '
                      '%(count)s clone(s) was found: %(clones)s',
                      {'name': cache_name, 'count': count,
                       'clones': clones})
            return
        payload = {'snapshots': True, 'force': True}
        try:
            self.nef.filesystems.delete(cache_path, payload)
        except jsonrpc.NefException as error:
            LOG.error('Failed to delete image cache %(name)s: %(error)s',
                      {'name': cache_name, 'error': error})

    def _verify_cache(self, cache, snapshot):
        cache_path = self._get_volume_path(cache)
        payload = {'fields': 'referencedReservationSize'}
        try:
            props = self.nef.filesystems.get(cache_path, payload)
        except jsonrpc.NefException as error:
            if error.code == 'ENOENT':
                return cache, snapshot
            raise
        cache_size = props['referencedReservationSize']
        cache['size'] = utils.roundgb(cache_size)
        snapshot_path = self._get_snapshot_path(snapshot)
        payload = {'fields': 'path'}
        try:
            self.nef.snapshots.get(snapshot_path, payload)
        except jsonrpc.NefException as error:
            if error.code == 'ENOENT':
                return cache, snapshot
            raise
        snapshot['volume_size'] = cache['size']
        return cache, snapshot

    @coordination.synchronized('{self.nef.lock}-{cache[name]}')
    def _create_cache(self, ctxt, cache, image_id, image_service):
        snapshot = {
            'id': cache['id'],
            'name': self.cache_snapshot_template % cache['id'],
            'volume_id': cache['id'],
            'volume_name': cache['name']
        }
        cache, snapshot = self._verify_cache(cache, snapshot)
        if snapshot.get('volume_size', 0) > 0:
            return snapshot
        if 'size' in cache:
            cache_path = self._get_volume_path(cache)
            new_uuid = six.text_type(uuid.uuid4())
            new_name = self.cache_image_template % new_uuid
            new_cache = {'name': new_name}
            new_path = self._get_volume_path(new_cache)
            payload = {'newPath': new_path}
            self.nef.filesystems.rename(cache_path, payload)
        cache['size'] = 0
        cache_path = self._create_volume(cache)
        specs = self._get_image_specs(cache)
        volume_image = image.VolumeImage(self, cache, specs)
        volume_image.fetch(ctxt, image_service, image_id)
        payload = {'referencedReservationSize': volume_image.file_size}
        self.nef.filesystems.set(cache_path, payload)
        cache['size'] = volume_image.size
        snapshot['volume_size'] = cache['size']
        self.create_snapshot(snapshot)
        return snapshot

    def clone_image(self, ctxt, volume, image_location, image_meta,
                    image_service):
        """Create a volume efficiently from an existing image.

        image_location is a string whose format depends on the
        image service backend in use. The driver should use it
        to determine whether cloning is possible.

        image_meta is a dictionary that includes 'disk_format' (e.g.
        raw, qcow2) and other image attributes that allow drivers to
        decide whether they can clone the image without first requiring
        conversion.

        image_service is the reference of the image_service to use.
        Note that this is needed to be passed here for drivers that
        will want to fetch images from the image service directly.

        Returns a dict of volume properties eg. provider_location,
        boolean indicating whether cloning occurred.
        """
        if not self.image_cache:
            return None, False
        specs = self._get_image_specs(volume)
        image_id = image_meta['id']
        image_checksum = image_meta['checksum']
        image_format = specs['format']
        compound = '%(id)s:%(checksum)s:%(format)s' % {
            'id': image_id,
            'checksum': image_checksum,
            'format': image_format
        }
        namespace = uuid.UUID(image_id, version=4)
        name = utils.native_string(compound)
        cache_uuid = uuid.uuid5(namespace, name)
        cache_id = six.text_type(cache_uuid)
        cache_name = self.cache_image_template % cache_id
        cache_type_id = volume['volume_type_id']
        cache = {
            'id': cache_id,
            'name': cache_name,
            'volume_type_id': cache_type_id
        }

        try:
            snapshot = self._create_cache(ctxt, cache,
                                          image_id,
                                          image_service)
        except Exception as error:
            LOG.error('Failed to create cache %(cache)s '
                      'for image %(image)s: %(error)s',
                      {'cache': cache_name,
                       'image': image_id,
                       'error': error})
            return None, False

        if snapshot['volume_size'] > volume['size']:
            code = 'ENOSPC'
            message = (_('Unable to clone cache %(cache)s for '
                         'image %(image)s to volume %(volume)s: '
                         'cache size %(cache_size)sGB is larger '
                         'than volume size %(volume_size)sGB')
                       % {'cache': cache_name, 'image': image_id,
                          'volume': volume['name'],
                          'cache_size': snapshot['volume_size'],
                          'volume_size': volume['size']})
            raise jsonrpc.NefException(code=code, message=message)

        try:
            self.create_volume_from_snapshot(volume, snapshot)
        except Exception as error:
            LOG.error('Failed to clone cache %(cache)s for image '
                      '%(image)s to volume %(volume)s: %(error)s',
                      {'cache': cache['name'], 'image': image_id,
                       'volume': volume['name'], 'error': error})
            return None, False
        return None, True

    def _mount_share(self, share):
        """Ensure that share is mounted on the host.

        :param share: nfs share
        :returns: mount point
        """
        attempts = max(1, self.configuration.nfs_mount_attempts)
        for attempt in range(1, attempts + 1):
            try:
                self._remotefsclient.mount(share)
            except Exception as error:
                LOG.debug('Mount attempt %(attempt)s failed: %(error)s, '
                          'retrying mount NFS share %(share)s',
                          {'attempt': attempt, 'error': error,
                           'share': share})
                if attempt == attempts:
                    LOG.error('Failed to mount NFS share %(share)s '
                              'after %(attempt)s attempts: %(error)s',
                              {'share': share, 'attempt': attempt,
                               'error': error})
                    raise
                self.nef.delay(attempt)
            else:
                mntpoint = self._get_mount_point_for_share(share)
                LOG.debug('NFS share %(share)s has been mounted at '
                          '%(mntpoint)s after %(attempt)s attempts',
                          {'share': share, 'mntpoint': mntpoint,
                           'attempt': attempt})
                return mntpoint

    def _unmount_share(self, share, mntpoint=None):
        """Ensure that NFS share is unmounted on the host.

        :param share: nfs share
        :param mntpoint: mount point
        """
        if not mntpoint:
            mntpoint = self._get_mount_point_for_share(share)
        mntpoints = self._remotefsclient._read_mounts()
        if mntpoint not in mntpoints:
            LOG.debug('NFS share %(share)s is not mounted at %(mntpoint)s',
                      {'share': share, 'mntpoint': mntpoint})
            return
        attempts = max(1, self.configuration.nfs_mount_attempts)
        for attempt in range(1, attempts + 1):
            try:
                fs.umount(mntpoint)
            except processutils.ProcessExecutionError as error:
                LOG.debug('Unmount attempt %(attempt)s failed: %(error)s, '
                          'retrying unmount NFS share %(share)s mounted '
                          'at %(mntpoint)s',
                          {'attempt': attempt, 'error': error,
                           'share': share, 'mntpoint': mntpoint})
                if attempt == attempts:
                    LOG.error('Failed to unmount NFS share %(share)s '
                              'mounted at %(mntpoint)s after %(attempts)s '
                              'attempts: %(error)s',
                              {'share': share, 'mntpoint': mntpoint,
                               'attempts': attempts, 'error': error})
                    raise
                self.nef.delay(attempt)
            else:
                LOG.debug('NFS share %(share)s has been unmounted at '
                          '%(mntpoint)s after %(attempt)s attempts',
                          {'share': share, 'mntpoint': mntpoint,
                           'attempt': attempt})
                break
        self._delete(mntpoint)

    def _migrate_volume(self, volume, scheme, hosts, port, path):
        """Storage assisted volume migration."""
        src_hosts = self._get_host_addresses()
        src_path = self._get_volume_path(volume)
        dst_path = posixpath.join(path, volume['name'])
        for dst_host in hosts:
            if dst_host in src_hosts and src_path == dst_path:
                LOG.info('Skip local migration for host %(dst_host)s: '
                         'source volume %(src_path)s and destination '
                         'volume %(dst_path)s are the same volume',
                         {'dst_host': dst_host, 'src_path': src_path,
                          'dst_path': dst_path})
                return True
        payload = {'fields': 'name'}
        try:
            self.nef.hpr.list(payload)
        except jsonrpc.NefException as error:
            LOG.error('Storage assisted volume migration '
                      'is unavailable: %(error)s',
                      {'error': error})
            return False
        service_name = '%(prefix)s-%(volume)s' % {
            'prefix': self.migration_service_prefix,
            'volume': volume['name']
        }
        service_created = False
        for dst_host in hosts:
            payload = {
                'name': service_name,
                'sourceDataset': src_path,
                'destinationDataset': dst_path,
                'type': 'scheduled'
            }
            if dst_host not in src_hosts:
                payload['isSource'] = True
                payload['remoteNode'] = {
                    'host': dst_host,
                    'port': port,
                    'proto': scheme
                }
                if self.migration_throttle:
                    payload['transportOptions'] = {
                        'throttle': self.migration_throttle * units.Mi
                    }
            try:
                self.nef.hpr.create(payload)
                service_created = True
                break
            except jsonrpc.NefException as error:
                LOG.error('Failed to create migration service '
                          'with payload %(payload)s: %(error)s',
                          {'payload': payload, 'error': error})
        service_running = False
        if service_created:
            try:
                self.nef.hpr.start(service_name)
                service_running = True
            except jsonrpc.NefException as error:
                LOG.error('Failed to start migration service '
                          '%(service_name)s: %(error)s',
                          {'service_name': service_name,
                           'error': error})
        service_success = False
        service_retries = 0
        while service_running:
            service_retries += 1
            self.nef.delay(service_retries)
            payload = {'fields': 'state,progress,runNumber,lastError'}
            try:
                service = self.nef.hpr.get(service_name, payload)
            except jsonrpc.NefException as error:
                LOG.error('Failed to stat migration service '
                          '%(service_name)s: %(error)s',
                          {'service_name': service_name,
                           'error': error})
                if service_retries > self.nef.retries:
                    break
            service_state = service['state']
            service_counter = service['runNumber']
            service_progress = service['progress']
            if service_state == 'faulted':
                service_error = service['lastError']
                LOG.error('Migration service %(service_name)s '
                          'failed with error: %(service_error)s',
                          {'service_name': service_name,
                           'service_error': service_error})
                service_running = False
            elif service_state == 'disabled' and service_counter > 0:
                LOG.info('Migration service %(service_name)s '
                         'successfully replicated %(src_path)s '
                         'to %(dst_host)s:%(dst_path)s',
                         {'service_name': service_name,
                          'src_path': src_path,
                          'dst_host': dst_host,
                          'dst_path': dst_path})
                service_running = False
                service_success = True
            else:
                LOG.info('Migration service %(service_name)s '
                         'is %(service_state)s, progress '
                         '%(service_progress)s%%',
                         {'service_name': service_name,
                          'service_state': service_state,
                          'service_progress': service_progress})
        if service_created:
            payload = {
                'destroySourceSnapshots': True,
                'destroyDestinationSnapshots': True,
                'force': True
            }
            try:
                self.nef.hpr.delete(service_name, payload)
            except jsonrpc.NefException as error:
                LOG.error('Failed to delete migration service '
                          '%(service_name)s: %(error)s',
                          {'service_name': service_name,
                           'error': error})
        if not service_success:
            return False
        try:
            self.delete_volume(volume)
        except jsonrpc.NefException as error:
            LOG.error('Failed to delete source '
                      'volume %(volume)s: %(error)s',
                      {'volume': volume['name'],
                       'error': error})
        return True

    def migrate_volume(self, ctxt, volume, host):
        """Migrate the volume to the specified host.

        Returns a boolean indicating whether the migration occurred,
        as well as model_update.

        :param ctxt: Security context of the caller.
        :param volume: A dictionary describing the volume to migrate
        :param host: A dictionary describing the host to migrate to, where
                     host['host'] is its name, and host['capabilities'] is a
                     dictionary of its reported capabilities.
        """
        LOG.info('Start storage assisted volume migration '
                 'for volume %(volume)s to host %(host)s',
                 {'volume': volume['name'],
                  'host': host['host']})
        false_ret = (False, None)
        if 'capabilities' not in host:
            LOG.error('No host capabilities found for '
                      'the destination host %(host)s',
                      {'host': host['host']})
            return false_ret
        capabilities = host['capabilities']
        required_capabilities = [
            'vendor_name',
            'location_info',
            'storage_protocol',
            'free_capacity_gb'
        ]
        for capability in required_capabilities:
            if not (capability in capabilities and capabilities[capability]):
                LOG.error('Required host capability %(capability)s not '
                          'found for the destination host %(host)s',
                          {'capability': capability, 'host': host['host']})
                return false_ret
        vendor = capabilities['vendor_name']
        if vendor != self.vendor_name:
            LOG.error('Unsupported vendor %(vendor)s found '
                      'for the destination host %(host)s',
                      {'vendor': vendor, 'host': host['host']})
            return false_ret
        location = capabilities['location_info']
        try:
            nas_driver, nas_host, nas_path = location.split(':')
        except ValueError as error:
            LOG.error('Failed to parse location info %(location)s '
                      'for the destination host %(host)s: %(error)s',
                      {'location': location, 'host': host['host'],
                       'error': error})
            return false_ret
        if not (nas_driver and nas_host and nas_path):
            LOG.error('Incomplete location info %(location)s '
                      'found for the destination host %(host)s',
                      {'location': location, 'host': host['host']})
            return false_ret
        if nas_driver != self.nas_driver:
            LOG.error('Unsupported storage driver %(nas_driver)s '
                      'found for the destination host %(host)s',
                      {'nas_driver': nas_driver,
                       'host': host['host']})
            return false_ret
        storage_protocol = capabilities['storage_protocol']
        if storage_protocol != self.storage_protocol:
            LOG.error('Unsupported storage protocol %(protocol)s '
                      'found for the destination host %(host)s',
                      {'protocol': storage_protocol,
                       'host': host['host']})
            return false_ret
        free_capacity_gb = capabilities['free_capacity_gb']
        if free_capacity_gb < volume['size']:
            LOG.error('There is not enough space available on the '
                      'destination host %(host)s to migrate volume '
                      '%(volume)s, available space: %(free)sG, '
                      'required space: %(required)sG',
                      {'host': host['host'],
                       'volume': volume['name'],
                       'free': free_capacity_gb,
                       'required': volume['size']})
            return false_ret
        nef_scheme = None
        nef_hosts = []
        nef_port = None
        if 'nef_hosts' in capabilities and capabilities['nef_hosts']:
            for nef_host in capabilities['nef_hosts'].split(','):
                nef_host = nef_host.strip()
                if nef_host:
                    nef_hosts.append(nef_host)
        elif 'nef_url' in capabilities and capabilities['nef_url']:
            url = six.moves.urllib.parse.urlparse(capabilities['nef_url'])
            if url.scheme and url.hostname and url.port:
                nef_scheme = url.scheme
                nef_hosts.append(url.hostname)
                nef_port = url.port
            else:
                for nef_host in capabilities['nef_url'].split(','):
                    nef_host = nef_host.strip()
                    if nef_host:
                        nef_hosts.append(nef_host)
        if not nef_hosts:
            LOG.error('NEF management address not found for the '
                      'destination host %(host)s: %(capabilities)s',
                      {'host': host['host'],
                       'capabilities': capabilities})
            return false_ret
        if not nef_scheme:
            if 'nef_scheme' in capabilities and capabilities['nef_scheme']:
                nef_scheme = capabilities['nef_scheme']
            else:
                nef_scheme = self.nef.scheme
        if not nef_port:
            if 'nef_port' in capabilities and capabilities['nef_port']:
                nef_port = capabilities['nef_port']
            else:
                nef_port = self.nef.port
        if self._migrate_volume(volume, nef_scheme, nef_hosts, nef_port,
                                nas_path):
            return (True, None)
        return false_ret

    def create_export(self, ctxt, volume, connector):
        """Driver entry point to get the export info for a new volume."""
        pass

    def ensure_export(self, ctxt, volume):
        """Driver entry point to get the export info for an existing volume."""
        pass

    @coordination.synchronized('{self.nef.lock}-{volume[id]}')
    def remove_export(self, ctxt, volume):
        """Driver entry point to remove an export for a volume."""
        if not self.nas_nohide:
            share = os.path.join(self.nas_share, volume['name'])
            self._unmount_share(share)

    def terminate_connection(self, volume, connector, **kwargs):
        """Terminate a connection to a volume.

        :param volume: a volume object
        :param connector: a connector object
        :returns: dictionary of connection information
        """
        pass

    @coordination.synchronized('{self.nef.lock}-{volume[id]}')
    def initialize_connection(self, volume, connector):
        """Allow connection to connector and return connection info.

        :param volume: a volume object
        :param connector: a connector object
        :returns: dictionary of connection information
        """
        LOG.debug('Initialize volume connection for %(volume)s '
                  'and connector %(connector)s',
                  {'volume': volume['name'],
                   'connector': connector})
        specs = self._get_image_specs(volume)
        volume_image = image.VolumeImage(self, volume, specs)
        volume_image.reload(file_format=True)
        data = {
            'export': volume_image.share,
            'format': volume_image.file_format,
            'name': volume_image.file_name
        }
        if self.mount_options:
            data['options'] = '-o %s' % self.mount_options
        connection_info = {
            'driver_volume_type': self.driver_volume_type,
            'mount_point_base': self.mount_point_base,
            'data': data
        }
        return connection_info

    def _demote_volume(self, volume, volume_origin):
        """Demote a volume.

        :param volume: volume reference
        :param volume_origin: volume origin path
        """
        volume_path = self._get_volume_path(volume)
        payload = {'parent': volume_path, 'fields': 'path'}
        try:
            snapshots = self.nef.snapshots.list(payload)
        except jsonrpc.NefException as error:
            if error.code == 'ENOENT':
                return volume_origin
            raise
        origin_txg = 0
        origin_path = None
        clone_path = None
        for snapshot in snapshots:
            snapshot_path = snapshot['path']
            payload = {'fields': 'clones,creationTxg'}
            try:
                props = self.nef.snapshots.get(snapshot_path, payload)
            except jsonrpc.NefException as error:
                if error.code == 'ENOENT':
                    continue
                raise
            snapshot_clones = props['clones']
            # Workaround for NEX-22763
            snapshot_txg = int(props['creationTxg'])
            if snapshot_clones and snapshot_txg > origin_txg:
                clone_path = snapshot_clones[0]
                origin_txg = snapshot_txg
                origin_path = snapshot_path
        if clone_path:
            try:
                self.nef.filesystems.promote(clone_path)
            except jsonrpc.NefException as error:
                if error.code in ['ENOENT', 'EBADARG']:
                    return volume_origin
                raise
            return origin_path
        return volume_origin

    def delete_volume(self, volume):
        """Deletes a volume.

        :param volume: volume reference
        """
        volume_path = self._get_volume_path(volume)
        payload = {'fields': 'originalSnapshot'}
        try:
            props = self.nef.filesystems.get(volume_path, payload)
        except jsonrpc.NefException as error:
            if error.code == 'ENOENT':
                return
            raise
        volume_exist = True
        origin = props['originalSnapshot']
        payload = {'snapshots': True, 'force': True}
        while volume_exist:
            try:
                self.nef.filesystems.delete(volume_path, payload)
            except jsonrpc.NefException as error:
                if error.code == 'EEXIST':
                    origin = self._demote_volume(volume, origin)
                    continue
                raise
            volume_exist = False
        if not origin:
            return
        origin_path, snapshot_name = origin.split('@')
        origin_name = posixpath.basename(origin_path)
        templates = {
            self.cache_snapshot_template: snapshot_name,
            self.cache_image_template: origin_name
        }
        for item, name in templates.items():
            if not utils.match_template(item, name):
                return
        self._delete_cache(origin_name, origin_path, origin)

    def _delete(self, mntpoint):
        """Override parent method for safe remove mount point."""
        try:
            self._execute('rm', '-d', mntpoint,
                          run_as_root=self._execute_as_root)
        except processutils.ProcessExecutionError as error:
            LOG.error('Failed to remove mountpoint %(mntpoint)s: %(error)s',
                      {'mntpoint': mntpoint, 'error': error})
        else:
            LOG.debug('The mount point %(mntpoint)s has been removed',
                      {'mntpoint': mntpoint})

    def extend_volume(self, volume, new_size):
        """Extend an existing volume.

        :param volume: volume reference
        :param new_size: volume new size in GB
        """
        if self._is_volume_attached(volume):
            reason = (_('Cannot extend volume %s while it is attached.')
                      % volume['id'])
            raise exception.ExtendVolumeError(reason=reason)
        specs = self._get_image_specs(volume)
        file_sparse = specs['sparse']
        file_vsolution = specs['vsolution']
        volume_image = image.VolumeImage(self, volume, specs)
        volume_image.reload(file_size=True, file_format=True)
        file_size = new_size * units.Gi
        file_format = volume_image.file_format
        if not file_sparse:
            self._set_volume_reservation(volume, file_size, file_format)
        if file_vsolution and file_format == image.FORMAT_RAW:
            volume_path = self._get_volume_path(volume)
            payload = {'size': file_size}
            self.nef.vsolutions.resize(volume_path, image.FILE_NAME, payload)
        else:
            volume_image.change(file_size=file_size)

    def create_snapshot(self, snapshot):
        """Creates a snapshot.

        :param snapshot: snapshot reference
        """
        snapshot_path = self._get_snapshot_path(snapshot)
        payload = {'path': snapshot_path}
        self.nef.snapshots.create(payload)

    def delete_snapshot(self, snapshot):
        """Deletes a snapshot.

        :param snapshot: snapshot reference
        """
        snapshot_path = self._get_snapshot_path(snapshot)
        payload = {'defer': True}
        self.nef.snapshots.delete(snapshot_path, payload)

    def snapshot_revert_use_temp_snapshot(self):
        # Considering that NexentaStor based drivers use COW images
        # for storing snapshots, having chains of such images,
        # creating a backup snapshot when reverting one is not
        # actually helpful.
        return False

    def revert_to_snapshot(self, ctxt, volume, snapshot):
        """Revert a volume to a snapshot.

        Note: the revert process should not change the volume's
        current size, that means if the driver shrank
        the volume during the process, it should extend the
        volume internally.
        """
        volume_path = self._get_volume_path(volume)
        snapshot_name = snapshot['name']
        payload = {'snapshot': snapshot_name}
        self.nef.filesystems.rollback(volume_path, payload)
        self._update_volume_props(volume)

    def create_volume_from_snapshot(self, volume, snapshot):
        """Create new volume from other's snapshot on appliance.

        :param volume: reference of volume to be created
        :param snapshot: reference of source snapshot
        """
        volume_path = self._get_volume_path(volume)
        snapshot_path = self._get_snapshot_path(snapshot)
        payload = {'targetPath': volume_path}
        self.nef.snapshots.clone(snapshot_path, payload)
        # Workaround for NEX-16457
        if self.nef.version_less('5.2.0.17'):
            self.nef.filesystems.unmount(volume_path)
            self.nef.filesystems.mount(volume_path)
        self._update_volume_props(volume)

    def create_cloned_volume(self, volume, src_vref):
        """Creates a clone of the specified volume.

        :param volume: new volume reference
        :param src_vref: source volume reference
        """
        snapshot = {
            'name': self.origin_snapshot_template % volume['id'],
            'volume_id': src_vref['id'],
            'volume_name': src_vref['name'],
            'volume_size': src_vref['size']
        }
        self.create_snapshot(snapshot)
        try:
            self.create_volume_from_snapshot(volume, snapshot)
        finally:
            self.delete_snapshot(snapshot)

    def create_consistencygroup(self, ctxt, group):
        """Creates a consistency group.

        :param ctxt: the context of the caller.
        :param group: the dictionary of the consistency group to be created.
        :returns: group_model_update
        """
        group_model_update = {}
        return group_model_update

    def create_group(self, ctxt, group):
        """Creates a group.

        :param ctxt: the context of the caller.
        :param group: the group object.
        :returns: model_update
        """
        return self.create_consistencygroup(ctxt, group)

    def delete_consistencygroup(self, ctxt, group, volumes):
        """Deletes a consistency group.

        :param ctxt: the context of the caller.
        :param group: the dictionary of the consistency group to be deleted.
        :param volumes: a list of volume dictionaries in the group.
        :returns: group_model_update, volumes_model_update
        """
        group_model_update = {}
        volumes_model_update = []
        for volume in volumes:
            self.delete_volume(volume)
        return group_model_update, volumes_model_update

    def delete_group(self, ctxt, group, volumes):
        """Deletes a group.

        :param ctxt: the context of the caller.
        :param group: the group object.
        :param volumes: a list of volume objects in the group.
        :returns: model_update, volumes_model_update
        """
        return self.delete_consistencygroup(ctxt, group, volumes)

    def update_consistencygroup(self, ctxt, group, add_volumes=None,
                                remove_volumes=None):
        """Updates a consistency group.

        :param ctxt: the context of the caller.
        :param group: the dictionary of the consistency group to be updated.
        :param add_volumes: a list of volume dictionaries to be added.
        :param remove_volumes: a list of volume dictionaries to be removed.
        :returns: group_model_update, add_volumes_update, remove_volumes_update
        """
        group_model_update = {}
        add_volumes_update = []
        remove_volumes_update = []
        return group_model_update, add_volumes_update, remove_volumes_update

    def update_group(self, ctxt, group, add_volumes=None,
                     remove_volumes=None):
        """Updates a group.

        :param ctxt: the context of the caller.
        :param group: the group object.
        :param add_volumes: a list of volume objects to be added.
        :param remove_volumes: a list of volume objects to be removed.
        :returns: model_update, add_volumes_update, remove_volumes_update
        """
        return self.update_consistencygroup(ctxt, group, add_volumes,
                                            remove_volumes)

    def create_cgsnapshot(self, ctxt, cgsnapshot, snapshots):
        """Creates a consistency group snapshot.

        :param ctxt: the context of the caller.
        :param cgsnapshot: the dictionary of the cgsnapshot to be created.
        :param snapshots: a list of snapshot dictionaries in the cgsnapshot.
        :returns: group_model_update, snapshots_model_update
        """
        group_model_update = {}
        snapshots_model_update = []
        cgsnapshot_name = self.group_snapshot_template % cgsnapshot['id']
        cgsnapshot_path = '%s@%s' % (self.nas_path, cgsnapshot_name)
        create_payload = {'path': cgsnapshot_path, 'recursive': True}
        self.nef.snapshots.create(create_payload)
        for snapshot in snapshots:
            volume_name = snapshot['volume_name']
            volume_path = posixpath.join(self.nas_path, volume_name)
            snapshot_name = snapshot['name']
            snapshot_path = '%s@%s' % (volume_path, cgsnapshot_name)
            rename_payload = {'newName': snapshot_name}
            self.nef.snapshots.rename(snapshot_path, rename_payload)
        delete_payload = {'defer': True, 'recursive': True}
        self.nef.snapshots.delete(cgsnapshot_path, delete_payload)
        return group_model_update, snapshots_model_update

    def create_group_snapshot(self, ctxt, group_snapshot, snapshots):
        """Creates a group_snapshot.

        :param ctxt: the context of the caller.
        :param group_snapshot: the GroupSnapshot object to be created.
        :param snapshots: a list of Snapshot objects in the group_snapshot.
        :returns: model_update, snapshots_model_update
        """
        return self.create_cgsnapshot(ctxt, group_snapshot, snapshots)

    def delete_cgsnapshot(self, ctxt, cgsnapshot, snapshots):
        """Deletes a consistency group snapshot.

        :param ctxt: the context of the caller.
        :param cgsnapshot: the dictionary of the cgsnapshot to be created.
        :param snapshots: a list of snapshot dictionaries in the cgsnapshot.
        :returns: group_model_update, snapshots_model_update
        """
        group_model_update = {}
        snapshots_model_update = []
        for snapshot in snapshots:
            self.delete_snapshot(snapshot)
        return group_model_update, snapshots_model_update

    def delete_group_snapshot(self, ctxt, group_snapshot, snapshots):
        """Deletes a group_snapshot.

        :param ctxt: the context of the caller.
        :param group_snapshot: the GroupSnapshot object to be deleted.
        :param snapshots: a list of snapshot objects in the group_snapshot.
        :returns: model_update, snapshots_model_update
        """
        return self.delete_cgsnapshot(ctxt, group_snapshot, snapshots)

    def create_consistencygroup_from_src(self, ctxt, group, volumes,
                                         cgsnapshot=None, snapshots=None,
                                         source_cg=None, source_vols=None):
        """Creates a consistency group from source.

        :param ctxt: the context of the caller.
        :param group: the dictionary of the consistency group to be created.
        :param volumes: a list of volume dictionaries in the group.
        :param cgsnapshot: the dictionary of the cgsnapshot as source.
        :param snapshots: a list of snapshot dictionaries in the cgsnapshot.
        :param source_cg: the dictionary of a consistency group as source.
        :param source_vols: a list of volume dictionaries in the source_cg.
        :returns: group_model_update, volumes_model_update
        """
        group_model_update = {}
        volumes_model_update = []
        if cgsnapshot and snapshots:
            for volume, snapshot in zip(volumes, snapshots):
                self.create_volume_from_snapshot(volume, snapshot)
        elif source_cg and source_vols:
            snapshot_name = self.origin_snapshot_template % group['id']
            snapshot_path = '%s@%s' % (self.nas_path, snapshot_name)
            create_payload = {'path': snapshot_path, 'recursive': True}
            self.nef.snapshots.create(create_payload)
            for volume, source_vol in zip(volumes, source_vols):
                snapshot = {
                    'name': snapshot_name,
                    'volume_id': source_vol['id'],
                    'volume_name': source_vol['name'],
                    'volume_size': source_vol['size']
                }
                self.create_volume_from_snapshot(volume, snapshot)
            delete_payload = {'defer': True, 'recursive': True}
            self.nef.snapshots.delete(snapshot_path, delete_payload)
        return group_model_update, volumes_model_update

    def create_group_from_src(self, ctxt, group, volumes,
                              group_snapshot=None, snapshots=None,
                              source_group=None, source_vols=None):
        """Creates a group from source.

        :param ctxt: the context of the caller.
        :param group: the Group object to be created.
        :param volumes: a list of Volume objects in the group.
        :param group_snapshot: the GroupSnapshot object as source.
        :param snapshots: a list of snapshot objects in group_snapshot.
        :param source_group: the Group object as source.
        :param source_vols: a list of volume objects in the source_group.
        :returns: model_update, volumes_model_update
        """
        return self.create_consistencygroup_from_src(
            ctxt, group, volumes, group_snapshot,
            snapshots, source_group, source_vols)

    def _set_volume_acl(self, volume):
        """Sets access permissions for given volume.

        :param volume: volume reference
        """
        volume_path = self._get_volume_path(volume)
        payload = {
            'type': 'allow',
            'principal': 'everyone@',
            'permissions': [
                'full_set'
            ],
            'flags': [
                'file_inherit',
                'dir_inherit'
            ]
        }
        self.nef.filesystems.acl(volume_path, payload)

    def _get_volume_share(self, volume):
        """Return NFS share path for the volume."""
        specs = self._get_volume_specs(volume)
        volume_path = self._get_volume_path(volume)
        payload = {'fields': 'isMounted,mountPoint,nonBlockingMandatoryMode'}
        props = self.nef.filesystems.get(volume_path, payload)
        nbmand = specs.get('nonBlockingMandatoryMode', self.nbmand)
        if props['nonBlockingMandatoryMode'] != nbmand:
            payload = {'nonBlockingMandatoryMode': nbmand}
            self.nef.filesystems.set(volume_path, payload)
            if props['isMounted']:
                self.nef.filesystems.unmount(volume_path)
                props['isMounted'] = False
        if props['mountPoint'] == 'none':
            self.nef.hpr.activate(volume_path)
            props = self.nef.filesystems.get(volume_path, payload)
        if not props['isMounted']:
            self.nef.filesystems.mount(volume_path)
        if self.nas_nohide:
            share = self.nas_share
        else:
            share = '%(host)s:%(mntpoint)s' % {
                'host': self.nas_host,
                'mntpoint': props['mountPoint']
            }
        return share

    def _get_volume_path(self, volume):
        """Return ZFS dataset path for the volume."""
        volume_name = volume['name']
        volume_path = posixpath.join(self.nas_path, volume_name)
        return volume_path

    def _get_snapshot_path(self, snapshot):
        """Return ZFS snapshot path for the snapshot."""
        volume_name = snapshot['volume_name']
        snapshot_name = snapshot['name']
        volume_path = posixpath.join(self.nas_path, volume_name)
        snapshot_path = '%(volume_path)s@%(snapshot_name)s' % {
            'volume_path': volume_path,
            'snapshot_name': snapshot_name
        }
        return snapshot_path

    def get_volume_stats(self, refresh=False):
        """Get volume stats.

        If 'refresh' is True, update the stats first.
        """
        if refresh or not self._stats:
            self._update_volume_stats()
        return self._stats

    def _update_volume_stats(self):
        """Retrieve stats info for NexentaStor Appliance."""
        provisioned_capacity_gb = total_volumes = total_snapshots = 0
        volumes = objects.VolumeList.get_all_by_host(self.ctxt, self.host)
        for volume in volumes:
            provisioned_capacity_gb += volume['size']
            total_volumes += 1
        snapshots = objects.SnapshotList.get_by_host(self.ctxt, self.host)
        for snapshot in snapshots:
            provisioned_capacity_gb += snapshot['volume_size']
            total_snapshots += 1
        description = (
            self.configuration.safe_get('nexenta_dataset_description'))
        if not description:
            description = '%(product)s %(host)s:%(path)s' % {
                'product': self.product_name,
                'host': self.nas_host,
                'path': self.nas_path
            }
        max_over_subscription_ratio = (
            self.configuration.safe_get('max_over_subscription_ratio'))
        reserved_percentage = (
            self.configuration.safe_get('reserved_percentage'))
        if reserved_percentage is None:
            reserved_percentage = 0
        location_info = '%(driver)s:%(host)s:%(path)s' % {
            'driver': self.nas_driver,
            'host': self.nas_host,
            'path': self.nas_path
        }
        display_name = 'Capabilities of %(product)s %(protocol)s driver' % {
            'product': self.product_name,
            'protocol': self.storage_protocol
        }
        stats = {
            'backend_state': 'down',
            'driver_version': self.VERSION,
            'vendor_name': self.vendor_name,
            'storage_protocol': self.storage_protocol,
            'volume_backend_name': self.backend_name,
            'location_info': location_info,
            'description': description,
            'display_name': display_name,
            'pool_name': self.nas_pool,
            'multiattach': True,
            'QoS_support': True,
            'consistencygroup_support': True,
            'consistent_group_snapshot_enabled': True,
            'online_extend_support': True,
            'sparse_copy_volume': True,
            'thin_provisioning_support': True,
            'thick_provisioning_support': True,
            'total_capacity_gb': 'unknown',
            'allocated_capacity_gb': 'unknown',
            'free_capacity_gb': 'unknown',
            'provisioned_capacity_gb': provisioned_capacity_gb,
            'total_volumes': total_volumes,
            'total_snapshots': total_snapshots,
            'max_over_subscription_ratio': max_over_subscription_ratio,
            'reserved_percentage': reserved_percentage,
            'nef_scheme': self.nef.scheme,
            'nef_hosts': ','.join(self.nef.hosts),
            'nef_port': self.nef.port,
            'nef_url': self.nef.url()
        }
        payload = {'fields': 'bytesAvailable,bytesUsed'}
        try:
            nas_stat = self.nef.filesystems.get(self.nas_path, payload)
        except jsonrpc.NefException as error:
            LOG.error('Failed to get backend statistics for host %(host)s '
                      'and volume backend %(backend_name)s: %(error)s',
                      {'host': self.host,
                       'backend_name': self.backend_name,
                       'error': error})
        else:
            available = nas_stat['bytesAvailable'] // units.Gi
            used = nas_stat['bytesUsed'] // units.Gi
            stats['free_capacity_gb'] = available
            stats['allocated_capacity_gb'] = used
            stats['total_capacity_gb'] = available + used
            stats['backend_state'] = 'up'
        self._stats = stats
        LOG.debug('Updated volume backend statistics for host %(host)s '
                  'and volume backend %(backend_name)s: %(stats)s',
                  {'host': self.host,
                   'backend_name': self.backend_name,
                   'stats': self._stats})

    def _get_existing_volume(self, existing_ref):
        types = {
            'source-name': 'path',
            'source-guid': 'guid'
        }
        if not any(key in types for key in existing_ref):
            code = 'EINVAL'
            keys = ', '.join(types.keys())
            message = (_('Manage existing volume failed '
                         'due to invalid backend reference. '
                         'Volume reference must contain '
                         'at least one valid key: %(keys)s')
                       % {'keys': keys})
            raise jsonrpc.NefException(code=code, message=message)
        payload = {
            'parent': self.nas_path,
            'fields': 'path,bytesReferenced',
            'recursive': False
        }
        for key, value in types.items():
            if key in existing_ref:
                payload[value] = existing_ref[key]
        # Workaround for NEX-22773
        if 'path' in payload:
            name = payload['path']
            path = posixpath.join(self.nas_path, name)
            payload['path'] = path
        existing_volumes = self.nef.filesystems.list(payload)
        if len(existing_volumes) == 1:
            refsize = existing_volumes[0]['bytesReferenced']
            volume_path = existing_volumes[0]['path']
            volume_name = posixpath.basename(volume_path)
            volume_size = utils.roundgb(refsize)
            existing_volume = {
                'name': volume_name,
                'path': volume_path,
                'size': volume_size
            }
            vid = volume_utils.extract_id_from_volume_name(volume_name)
            if volume_utils.check_already_managed_volume(vid):
                code = 'EEXIST'
                message = (_('Volume %(name)s already managed')
                           % {'name': volume_name})
                raise jsonrpc.NefException(code=code, message=message)
            return existing_volume
        elif not existing_volumes:
            code = 'ENOENT'
            reason = _('no matching volumes were found')
        else:
            code = 'EINVAL'
            reason = _('too many volumes were found')
        message = (_('Unable to manage existing volume by '
                     'reference %(reference)s: %(reason)s')
                   % {'reference': existing_ref, 'reason': reason})
        raise jsonrpc.NefException(code=code, message=message)

    def _check_already_managed_snapshot(self, snapshot_id):
        """Check cinder database for already managed snapshot.

        :param snapshot_id: snapshot id parameter
        :returns: return True, if database entry with specified
                  snapshot id exists, otherwise return False
        """
        if not isinstance(snapshot_id, six.string_types):
            return False
        try:
            uuid.UUID(snapshot_id, version=4)
        except ValueError:
            return False
        return objects.Snapshot.exists(self.ctxt, snapshot_id)

    def _get_existing_snapshot(self, snapshot, existing_ref):
        types = {
            'source-name': 'name',
            'source-guid': 'guid'
        }
        if not any(key in types for key in existing_ref):
            keys = ', '.join(types.keys())
            code = 'EINVAL'
            message = (_('Manage existing snapshot failed '
                         'due to invalid backend reference. '
                         'Snapshot reference must contain '
                         'at least one valid key: %(keys)s')
                       % {'keys': keys})
            raise jsonrpc.NefException(code=code, message=message)
        volume_name = snapshot['volume_name']
        volume_size = snapshot['volume_size']
        volume = {'name': volume_name}
        volume_path = self._get_volume_path(volume)
        payload = {
            'parent': volume_path,
            'fields': 'name,path',
            'recursive': False
        }
        for key, value in types.items():
            if key in existing_ref:
                payload[value] = existing_ref[key]
        existing_snapshots = self.nef.snapshots.list(payload)
        if len(existing_snapshots) == 1:
            name = existing_snapshots[0]['name']
            path = existing_snapshots[0]['path']
            existing_snapshot = {
                'name': name,
                'path': path,
                'volume_name': volume_name,
                'volume_size': volume_size
            }
            sid = volume_utils.extract_id_from_snapshot_name(name)
            if self._check_already_managed_snapshot(sid):
                code = 'EEXIST'
                message = (_('Snapshot %(name)s already managed')
                           % {'name': name})
                raise jsonrpc.NefException(code=code, message=message)
            return existing_snapshot
        elif not existing_snapshots:
            code = 'ENOENT'
            reason = _('no matching snapshots were found')
        else:
            code = 'EINVAL'
            reason = _('too many snapshots were found')
        message = (_('Unable to manage existing snapshot by '
                     'reference %(reference)s: %(reason)s')
                   % {'reference': existing_ref, 'reason': reason})
        raise jsonrpc.NefException(code=code, message=message)

    def manage_existing(self, volume, existing_ref):
        """Brings an existing backend storage object under Cinder management.

        existing_ref is passed straight through from the API request's
        manage_existing_ref value, and it is up to the driver how this should
        be interpreted.  It should be sufficient to identify a storage object
        that the driver should somehow associate with the newly-created cinder
        volume structure.

        There are two ways to do this:

        1. Rename the backend storage object so that it matches the,
           volume['name'] which is how drivers traditionally map between a
           cinder volume and the associated backend storage object.

        2. Place some metadata on the volume, or somewhere in the backend, that
           allows other driver requests (e.g. delete, clone, attach, detach...)
           to locate the backend storage object when required.

        If the existing_ref doesn't make sense, or doesn't refer to an existing
        backend storage object, raise a ManageExistingInvalidReference
        exception.

        The volume may have a volume_type, and the driver can inspect that and
        compare against the properties of the referenced backend storage
        object.  If they are incompatible, raise a
        ManageExistingVolumeTypeMismatch, specifying a reason for the failure.

        :param volume:       Cinder volume to manage
        :param existing_ref: Driver-specific information used to identify a
                             volume
        """
        existing_volume = self._get_existing_volume(existing_ref)
        existing_volume_path = existing_volume['path']
        if existing_volume['name'] != volume['name']:
            volume_path = self._get_volume_path(volume)
            payload = {'newPath': volume_path}
            self.nef.filesystems.rename(existing_volume_path, payload)
        self._update_volume_props(volume)

    def manage_existing_get_size(self, volume, existing_ref):
        """Return size of volume to be managed by manage_existing.

        When calculating the size, round up to the next GB.

        :param volume:       Cinder volume to manage
        :param existing_ref: Driver-specific information used to identify a
                             volume
        :returns size:       Volume size in GiB (integer)
        """
        existing_volume = self._get_existing_volume(existing_ref)
        self._set_volume_acl(existing_volume)
        specs = self._get_image_specs(existing_volume)
        volume_image = image.VolumeImage(self, existing_volume, specs)
        volume_image.reload(file_size=True)
        return volume_image.size

    def get_manageable_volumes(self, cinder_volumes, marker, limit, offset,
                               sort_keys, sort_dirs):
        """List volumes on the backend available for management by Cinder.

        Returns a list of dictionaries, each specifying a volume in the host,
        with the following keys:
        - reference (dictionary): The reference for a volume, which can be
          passed to "manage_existing".
        - size (int): The size of the volume according to the storage
          backend, rounded up to the nearest GB.
        - safe_to_manage (boolean): Whether or not this volume is safe to
          manage according to the storage backend. For example, is the volume
          in use or invalid for any reason.
        - reason_not_safe (string): If safe_to_manage is False, the reason why.
        - cinder_id (string): If already managed, provide the Cinder ID.
        - extra_info (string): Any extra information to return to the user

        :param cinder_volumes: A list of volumes in this host that Cinder
                               currently manages, used to determine if
                               a volume is manageable or not.
        :param marker:    The last item of the previous page; we return the
                          next results after this value (after sorting)
        :param limit:     Maximum number of items to return
        :param offset:    Number of items to skip after marker
        :param sort_keys: List of keys to sort results by (valid keys are
                          'identifier' and 'size')
        :param sort_dirs: List of directions to sort by, corresponding to
                          sort_keys (valid directions are 'asc' and 'desc')
        """
        manageable_volumes = []
        cinder_volume_names = {}
        for cinder_volume in cinder_volumes:
            key = cinder_volume['name']
            value = cinder_volume['id']
            cinder_volume_names[key] = value
        payload = {
            'parent': self.nas_path,
            'fields': 'name,guid,parent,path,bytesReferenced',
            'recursive': False
        }
        volumes = self.nef.filesystems.list(payload)
        for volume in volumes:
            safe_to_manage = True
            reason_not_safe = None
            cinder_id = None
            extra_info = None
            path = volume['path']
            guid = volume['guid']
            parent = volume['parent']
            refsize = volume['bytesReferenced']
            size = utils.roundgb(refsize)
            name = volume['name']
            if path == self.nas_path:
                continue
            if parent != self.nas_path:
                continue
            if utils.match_template(self.cache_image_template, name):
                continue
            if name in cinder_volume_names:
                cinder_id = cinder_volume_names[name]
                safe_to_manage = False
                reason_not_safe = _('Volume already managed')
            reference = {
                'source-name': name,
                'source-guid': guid
            }
            manageable_volumes.append({
                'reference': reference,
                'size': size,
                'safe_to_manage': safe_to_manage,
                'reason_not_safe': reason_not_safe,
                'cinder_id': cinder_id,
                'extra_info': extra_info
            })
        return volume_utils.paginate_entries_list(
            manageable_volumes, marker, limit,
            offset, sort_keys, sort_dirs)

    def unmanage(self, volume):
        """Removes the specified volume from Cinder management.

        Does not delete the underlying backend storage object.

        For most drivers, this will not need to do anything.  However, some
        drivers might use this call as an opportunity to clean up any
        Cinder-specific configuration that they have associated with the
        backend storage object.

        :param volume: Cinder volume to unmanage
        """
        pass

    def manage_existing_snapshot(self, snapshot, existing_ref):
        """Brings an existing backend storage object under Cinder management.

        existing_ref is passed straight through from the API request's
        manage_existing_ref value, and it is up to the driver how this should
        be interpreted.  It should be sufficient to identify a storage object
        that the driver should somehow associate with the newly-created cinder
        snapshot structure.

        There are two ways to do this:

        1. Rename the backend storage object so that it matches the
           snapshot['name'] which is how drivers traditionally map between a
           cinder snapshot and the associated backend storage object.

        2. Place some metadata on the snapshot, or somewhere in the backend,
           that allows other driver requests (e.g. delete) to locate the
           backend storage object when required.

        If the existing_ref doesn't make sense, or doesn't refer to an existing
        backend storage object, raise a ManageExistingInvalidReference
        exception.

        :param snapshot:     Cinder volume snapshot to manage
        :param existing_ref: Driver-specific information used to identify a
                             volume snapshot
        """
        existing_snapshot = self._get_existing_snapshot(snapshot, existing_ref)
        existing_snapshot_path = existing_snapshot['path']
        if existing_snapshot['name'] != snapshot['name']:
            payload = {'newName': snapshot['name']}
            self.nef.snapshots.rename(existing_snapshot_path, payload)

    def manage_existing_snapshot_get_size(self, snapshot, existing_ref):
        """Return size of snapshot to be managed by manage_existing.

        When calculating the size, round up to the next GB.

        :param snapshot:     Cinder volume snapshot to manage
        :param existing_ref: Driver-specific information used to identify a
                             volume snapshot
        :returns size:       Volume snapshot size in GiB (integer)
        """
        existing_snapshot = self._get_existing_snapshot(snapshot, existing_ref)
        return existing_snapshot['volume_size']

    def get_manageable_snapshots(self, cinder_snapshots, marker, limit, offset,
                                 sort_keys, sort_dirs):
        """List snapshots on the backend available for management by Cinder.

        Returns a list of dictionaries, each specifying a snapshot in the host,
        with the following keys:
        - reference (dictionary): The reference for a snapshot, which can be
          passed to "manage_existing_snapshot".
        - size (int): The size of the snapshot according to the storage
          backend, rounded up to the nearest GB.
        - safe_to_manage (boolean): Whether or not this snapshot is safe to
          manage according to the storage backend. For example, is the snapshot
          in use or invalid for any reason.
        - reason_not_safe (string): If safe_to_manage is False, the reason why.
        - cinder_id (string): If already managed, provide the Cinder ID.
        - extra_info (string): Any extra information to return to the user
        - source_reference (string): Similar to "reference", but for the
          snapshot's source volume.

        :param cinder_snapshots: A list of snapshots in this host that Cinder
                                 currently manages, used to determine if
                                 a snapshot is manageable or not.
        :param marker:    The last item of the previous page; we return the
                          next results after this value (after sorting)
        :param limit:     Maximum number of items to return
        :param offset:    Number of items to skip after marker
        :param sort_keys: List of keys to sort results by (valid keys are
                          'identifier' and 'size')
        :param sort_dirs: List of directions to sort by, corresponding to
                          sort_keys (valid directions are 'asc' and 'desc')

        """
        temporary_snapshots = {
            self.cache_snapshot_template: 'image cache',
            self.origin_snapshot_template: 'temporary origin',
            self.group_snapshot_template: 'temporary group'
        }
        service_snapshots = {
            'hprService': 'replication',
            'snaplistId': 'snapping'
        }
        manageable_snapshots = []
        cinder_volume_names = {}
        cinder_snapshot_names = {}
        cinder_volumes = objects.VolumeList.get_all_by_host(
            self.ctxt, self.host)
        for cinder_volume in cinder_volumes:
            key = self._get_volume_path(cinder_volume)
            value = {
                'name': cinder_volume['name'],
                'size': cinder_volume['size']
            }
            cinder_volume_names[key] = value
        for cinder_snapshot in cinder_snapshots:
            key = cinder_snapshot['name']
            value = {
                'id': cinder_snapshot['id'],
                'size': cinder_snapshot['volume_size'],
                'parent': cinder_snapshot['volume_name']
            }
            cinder_snapshot_names[key] = value
        payload = {
            'parent': self.nas_path,
            'fields': 'name,guid,path,parent,hprService,snaplistId',
            'recursive': True
        }
        snapshots = self.nef.snapshots.list(payload)
        for snapshot in snapshots:
            safe_to_manage = True
            reason_not_safe = None
            cinder_id = None
            extra_info = None
            name = snapshot['name']
            guid = snapshot['guid']
            path = snapshot['path']
            parent = snapshot['parent']
            if parent not in cinder_volume_names:
                LOG.debug('Skip snapshot %(path)s: parent '
                          'volume %(parent)s is unmanaged',
                          {'path': path, 'parent': parent})
                continue
            for item, desc in temporary_snapshots.items():
                if utils.match_template(item, name):
                    LOG.debug('Skip %(desc)s snapshot %(path)s',
                              {'desc': desc, 'path': path})
                    continue
            for item, desc in service_snapshots.items():
                if snapshot[item]:
                    LOG.debug('Skip %(desc)s snapshot %(path)s',
                              {'desc': desc, 'path': path})
                    continue
            if name in cinder_snapshot_names:
                size = cinder_snapshot_names[name]['size']
                cinder_id = cinder_snapshot_names[name]['id']
                safe_to_manage = False
                reason_not_safe = _('Snapshot already managed')
            else:
                size = cinder_volume_names[parent]['size']
                payload = {'fields': 'clones'}
                props = self.nef.snapshots.get(path, payload)
                clones = props['clones']
                unmanaged_clones = []
                for clone in clones:
                    if clone not in cinder_volume_names:
                        unmanaged_clones.append(clone)
                if unmanaged_clones:
                    safe_to_manage = False
                    dependent_clones = ', '.join(unmanaged_clones)
                    reason_not_safe = (_('Snapshot has unmanaged '
                                         'dependent clone(s) %(clones)s')
                                       % {'clones': dependent_clones})
            reference = {
                'source-name': name,
                'source-guid': guid
            }
            source_reference = {
                'name': cinder_volume_names[parent]['name']
            }
            manageable_snapshots.append({
                'reference': reference,
                'size': size,
                'safe_to_manage': safe_to_manage,
                'reason_not_safe': reason_not_safe,
                'cinder_id': cinder_id,
                'extra_info': extra_info,
                'source_reference': source_reference
            })
        return volume_utils.paginate_entries_list(
            manageable_snapshots, marker, limit,
            offset, sort_keys, sort_dirs)

    def unmanage_snapshot(self, snapshot):
        """Removes the specified snapshot from Cinder management.

        Does not delete the underlying backend storage object.

        For most drivers, this will not need to do anything. However, some
        drivers might use this call as an opportunity to clean up any
        Cinder-specific configuration that they have associated with the
        backend storage object.

        :param snapshot: Cinder volume snapshot to unmanage
        """
        pass

    def update_migrated_volume(self, ctxt, volume, new_volume,
                               original_volume_status):
        """Return model update for migrated volume.

        This method should rename the back-end volume name on the
        destination host back to its original name on the source host.

        :param ctxt: The context of the caller
        :param volume: The original volume that was migrated to this backend
        :param new_volume: The migration volume object that was created on
                           this backend as part of the migration process
        :param original_volume_status: The status of the original volume
        :returns: model_update to update DB with any needed changes
        """
        volume_renamed = False
        volume_path = self._get_volume_path(volume)
        new_volume_path = self._get_volume_path(new_volume)
        bak_volume_path = '%s-backup' % volume_path
        if volume['host'] == new_volume['host']:
            volume['_name_id'] = new_volume['id']
            payload = {'newPath': bak_volume_path}
            try:
                self.nef.filesystems.rename(volume_path, payload)
            except jsonrpc.NefException as error:
                LOG.error('Failed to create backup copy of original '
                          'volume %(volume)s: %(error)s',
                          {'volume': volume['name'],
                           'error': error})
                if error.code != 'ENOENT':
                    raise
            else:
                volume_renamed = True
        payload = {'newPath': volume_path}
        try:
            self.nef.filesystems.rename(new_volume_path, payload)
        except jsonrpc.NefException as rename_error:
            LOG.error('Failed to rename temporary volume %(new_volume)s '
                      'to original %(volume)s after migration: %(error)s',
                      {'new_volume': new_volume['name'],
                       'volume': volume['name'],
                       'error': rename_error})
            if volume_renamed:
                payload = {'newPath': volume_path}
                try:
                    self.nef.filesystems.rename(bak_volume_path, payload)
                except jsonrpc.NefException as restore_error:
                    LOG.error('Failed to restore backup copy of original '
                              'volume %(volume)s: %(error)s',
                              {'volume': volume['name'],
                               'error': restore_error})
            raise rename_error
        if volume_renamed:
            payload = {'newPath': new_volume_path}
            try:
                self.nef.filesystems.rename(bak_volume_path, payload)
            except jsonrpc.NefException as error:
                LOG.error('Failed to rename backup copy of original '
                          'volume %(volume)s to temporary volume '
                          '%(new_volume)s: %(error)s',
                          {'volume': volume['name'],
                           'new_volume': new_volume['name'],
                           'error': error})
        return {'_name_id': None, 'provider_location': None}

    def before_volume_copy(self, ctxt, src_volume, dst_volume, remote=None):
        """Driver-specific actions before copy volume data.

        This method will be called before _copy_volume_data during volume
        migration
        """
        connector_properties = cinder_utils.brick_get_connector_properties()
        attach_info, dst_volume = self._attach_volume(
            ctxt, dst_volume, connector_properties, remote=True)
        dst_file_path = attach_info['device']['path']
        dst_image = image_utils.qemu_img_info(
            dst_file_path, force_share=True,
            run_as_root=self._execute_as_root)
        self._detach_volume(ctxt, attach_info, dst_volume,
                            connector_properties, force=True)
        src_specs = self._get_image_specs(src_volume)
        src_image = image.VolumeImage(self, src_volume, src_specs)
        src_image.reload(file_format=True)
        if src_image.file_format != dst_image.file_format:
            src_image.change(file_format=dst_image.file_format)

    def retype(self, ctxt, volume, new_type, diff, host):
        """Retype from one volume type to another."""
        LOG.debug('Retype volume %(volume)s on host %(host)s '
                  'and volume type %(type)s with diff %(diff)s',
                  {'volume': volume['name'], 'host': host,
                   'type': new_type['name'], 'diff': diff})
        self._update_volume_props(volume, new_type)
        return True, None

    def _init_vendor_properties(self):
        """Create a dictionary of vendor unique properties.

        This method creates a dictionary of vendor unique properties
        and returns both created dictionary and vendor name.
        Returned vendor name is used to check for name of vendor
        unique properties.

        - Vendor name shouldn't include colon(:) because of the separator
          and it is automatically replaced by underscore(_).
          ex. abc:d -> abc_d
        - Vendor prefix is equal to vendor name.
          ex. abcd
        - Vendor unique properties must start with vendor prefix + ':'.
          ex. abcd:maxIOPS

        Each backend driver needs to override this method to expose
        its own properties using _set_property() like this:

        self._set_property(
            properties,
            "vendorPrefix:specific_property",
            "Title of property",
            _("Description of property"),
            "type")

        : return dictionary of vendor unique properties
        : return vendor name
        """
        vendor_properties = {}
        namespace = self.nef.filesystems.namespace
        items = self.nef.filesystems.properties
        keys = ['enum', 'default', 'minimum', 'maximum']
        for item in items:
            spec = {}
            for key in keys:
                if key in item:
                    spec[key] = item[key]
            if 'cfg' in item:
                key = item['cfg']
                value = self.configuration.safe_get(key)
                if value not in [None, '']:
                    spec['default'] = value
            elif 'api' in item:
                api = item['api']
                if api in self.nas_stat:
                    value = self.nas_stat[api]
                    spec['default'] = value
            LOG.debug('Initialize vendor capabilities for '
                      '%(product)s %(protocol)s backend: '
                      '%(type)s %(name)s property %(spec)s',
                      {'product': self.product_name,
                       'protocol': self.storage_protocol,
                       'type': item['type'],
                       'name': item['name'],
                       'spec': spec})
            self._set_property(
                vendor_properties,
                item['name'],
                item['title'],
                item['description'],
                item['type'],
                **spec
            )
        return vendor_properties, namespace

    def _get_volume_type_specs(self, volume, volume_type=None):
        if volume_type and 'id' in volume_type:
            type_id = volume_type['id']
        elif 'volume_type_id' in volume:
            type_id = volume['volume_type_id']
        else:
            type_id = None
        if type_id:
            return volume_types.get_volume_type_extra_specs(type_id)
        return {}

    def _get_image_specs(self, volume, volume_type=None):
        payload = {}
        items = self.nef.filesystems.properties
        specs = self._get_volume_type_specs(volume, volume_type)
        for item in items:
            if 'img' not in item:
                continue
            img = item['img']
            name = item['name']
            if name in specs:
                spec = specs[name]
                value = self._check_volume_spec(spec, item)
            elif 'cfg' in item:
                key = item['cfg']
                value = self.configuration.safe_get(key)
                if value in [None, '']:
                    continue
            else:
                continue
            payload[img] = value
        LOG.debug('Image properties for %(volume)s: %(payload)s',
                  {'volume': volume['name'], 'payload': payload})
        return payload

    def _get_volume_specs(self, volume, volume_type=None):
        payload = {}
        items = self.nef.filesystems.properties
        specs = self._get_volume_type_specs(volume, volume_type)
        for item in items:
            if 'api' not in item:
                continue
            api = item['api']
            name = item['name']
            if name in specs:
                spec = specs[name]
                value = self._check_volume_spec(spec, item)
            elif 'cfg' in item:
                key = item['cfg']
                value = self.configuration.safe_get(key)
                if value in [None, '']:
                    continue
            elif volume_type and api in self.nas_stat:
                value = self.nas_stat[api]
            else:
                continue
            payload[api] = value
        LOG.debug('Volume properties for %(volume)s: %(payload)s',
                  {'volume': volume['name'], 'payload': payload})
        return payload

    def _check_volume_spec(self, value, prop):
        name = prop['name']
        code = 'EINVAL'
        if prop['type'] == 'integer':
            try:
                value = int(value)
            except ValueError:
                message = (_('Invalid non-integer value %(value)s for '
                             'vendor property name %(name)s')
                           % {'value': value, 'name': name})
                raise jsonrpc.NefException(code=code, message=message)
            if 'minimum' in prop:
                minimum = prop['minimum']
                if value < minimum:
                    message = (_('Integer value %(value)s is less than '
                                 'allowed minimum %(minimum)s for vendor '
                                 'property name %(name)s')
                               % {'value': value, 'minimum': minimum,
                                  'name': name})
                    raise jsonrpc.NefException(code=code, message=message)
            if 'maximum' in prop:
                maximum = prop['maximum']
                if value > maximum:
                    message = (_('Integer value %(value)s is greater than '
                                 'allowed maximum %(maximum)s for vendor '
                                 'property name %(name)s')
                               % {'value': value, 'maximum': maximum,
                                  'name': name})
                    raise jsonrpc.NefException(code=code, message=message)
        elif prop['type'] == 'string':
            try:
                value = str(value)
            except UnicodeEncodeError:
                message = (_('Invalid non-ASCII value %(value)s for vendor '
                             'property name %(name)s')
                           % {'value': value, 'name': name})
                raise jsonrpc.NefException(code=code, message=message)
        elif prop['type'] == 'boolean':
            words = value.split()
            if len(words) == 2 and words[0] == '<is>':
                value = words[1]
            try:
                value = strutils.bool_from_string(value, strict=True)
            except ValueError:
                message = (_('Invalid non-boolean value %(value)s for vendor '
                             'property name %(name)s')
                           % {'value': value, 'name': name})
                raise jsonrpc.NefException(code=code, message=message)
        if 'enum' in prop:
            enum = prop['enum']
            if value not in enum:
                message = (_('Value %(value)s is out of allowed enumeration '
                             '%(enum)s for vendor property name %(name)s')
                           % {'value': value, 'enum': enum, 'name': name})
                raise jsonrpc.NefException(code=code, message=message)
        return value

    def _get_host_addresses(self):
        """Returns NexentaStor IP addresses list."""
        addresses = []
        netaddrs = self.nef.netaddrs.list()
        for netaddr in netaddrs:
            cidr = six.text_type(netaddr['address'])
            ip = cidr.split('/')[0]
            instance = ipaddress.ip_address(ip)
            if not instance.is_loopback:
                addresses.append(instance.exploded)
        LOG.debug('Configured IP addresses: %(addresses)s',
                  {'addresses': addresses})
        return addresses

    def _get_backend_name(self):
        backend_name = self.configuration.safe_get('volume_backend_name')
        if not backend_name:
            LOG.error('Failed to get configured volume backend name')
            backend_name = '%(product)s_%(protocol)s' % {
                'product': self.product_name,
                'protocol': self.storage_protocol
            }
        return backend_name

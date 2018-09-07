# Copyright 2018 Nexenta Systems, Inc. All Rights Reserved.
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

import hashlib
import ipaddress
import json
import random
import six
import uuid

from oslo_log import log as logging
from oslo_utils import excutils

from cinder import exception
from cinder.i18n import _, _LE, _LI, _LW
from cinder import interface
from cinder.volume import driver
from cinder.volume.drivers.nexenta import jsonrpc
from cinder.volume.drivers.nexenta import options
from cinder.volume.drivers.nexenta import utils

VERSION = '1.3.6'
LOG = logging.getLogger(__name__)


@interface.volumedriver
class NexentaISCSIDriver(driver.ISCSIDriver):
    """Executes volume driver commands on Nexenta Appliance.

    Version history:

    .. code-block:: none

        1.0.0 - Initial driver version.
        1.0.1 - Fixed bug #1236626: catch "does not exist" exception of
                lu_exists.
        1.1.0 - Changed class name to NexentaISCSIDriver.
        1.1.1 - Ignore "does not exist" exception of nms.snapshot.destroy.
        1.1.2 - Optimized create_cloned_volume, replaced zfs send recv with zfs
                clone.
        1.1.3 - Extended volume stats provided by _update_volume_stats method.
        1.2.0 - Added volume migration with storage assist method.
        1.2.1 - Fixed bug #1263258: now migrate_volume update provider_location
                of migrated volume; after migrating volume migrate_volume
                destroy snapshot on migration destination.
        1.3.0 - Added retype method.
        1.3.0.1 - Target creation refactor.
        1.3.1 - Added ZFS cleanup.
        1.3.2 - Added support for target_portal_group and zvol folder.
        1.3.3 - Added synchronization for Comstar API calls.
        1.3.4 - Fixed automatic mode for nexenta_rest_protocol, improved logging.
        1.3.5 - Fixed compatibility with initial driver version.
                Fixed deletion of temporary snapshots.
                Fixed creation of volumes with enabled compression option.
                Fixed collection of backend statistics.
                Refactored LUN creation, use host group for LUN mappings.
                Added deferred deletion for snapshots.
        1.3.6 - Added consistency group support.
    """

    VERSION = VERSION

    # ThirdPartySystems wiki page
    CI_WIKI_NAME = "Nexenta_CI"

    def __init__(self, *args, **kwargs):
        super(NexentaISCSIDriver, self).__init__(*args, **kwargs)
        self.nms = None
        self.mappings = {}
        if self.configuration:
            self.configuration.append_config_values(
                options.NEXENTA_CONNECTION_OPTS)
            self.configuration.append_config_values(
                options.NEXENTA_ISCSI_OPTS)
            self.configuration.append_config_values(
                options.NEXENTA_DATASET_OPTS)
            self.configuration.append_config_values(
                options.NEXENTA_RRMGR_OPTS)
        self.verify_ssl = self.configuration.driver_ssl_cert_verify
        self.target_prefix = self.configuration.nexenta_target_prefix
        self.target_group_prefix = (
            self.configuration.nexenta_target_group_prefix)
        self.host_group_prefix = self.configuration.nexenta_host_group_prefix
        self.lpt = self.configuration.nexenta_luns_per_target
        self.nms_protocol = self.configuration.nexenta_rest_protocol
        self.nms_host = self.configuration.nexenta_host
        self.nms_port = self.configuration.nexenta_rest_port
        self.nms_user = self.configuration.nexenta_user
        self.nms_password = self.configuration.nexenta_password
        self.nms_path = options.DEFAULT_NMS_PATH
        self.volume = self.configuration.nexenta_volume
        self.folder = self.configuration.nexenta_folder
        self.tpgs = self.configuration.nexenta_iscsi_target_portal_groups
        self.volume_blocksize = self.configuration.nexenta_blocksize
        self.volume_sparse = self.configuration.nexenta_sparse
        self.volume_compression = (
            self.configuration.nexenta_dataset_compression)
        self.volume_deduplication = self.configuration.nexenta_dataset_dedup
        self.volume_description = (
            self.configuration.nexenta_dataset_description)
        self.rrmgr_compression = self.configuration.nexenta_rrmgr_compression
        self.rrmgr_tcp_buf_size = self.configuration.nexenta_rrmgr_tcp_buf_size
        self.rrmgr_connections = self.configuration.nexenta_rrmgr_connections
        self.portal_port = self.configuration.nexenta_iscsi_target_portal_port
        self.lock = hashlib.md5(options.DEFAULT_NMS_LOCK).hexdigest()
        if self.folder:
            self.dataset = '%s/%s' % (self.volume, self.folder)
        else:
            self.dataset = self.volume

    @property
    def backend_name(self):
        backend_name = None
        if self.configuration:
            backend_name = self.configuration.safe_get('volume_backend_name')
        if not backend_name:
            backend_name = self.__class__.__name__
        return backend_name

    def do_setup(self, context):
        url = '%s://%s:%s@%s:%s%s' % (self.nms_protocol, self.nms_user,
                                      self.nms_password, self.nms_host,
                                      self.nms_port, self.nms_path)
        self.nms = self.get_nms_for_url(url)

    def check_for_setup_error(self):
        """Verify that the volume and folder exists."""
        if not self.nms.volume.object_exists(self.volume):
            msg = (_('Volume %(volume)s not found')
                   % {'volume': self.volume})
            raise exception.NexentaException(msg)
        if self.folder:
            if not self.nms.folder.object_exists(self.dataset):
                msg = (_('Folder %(folder)s not found')
                       % {'folder': self.folder})
                raise exception.NexentaException(msg)

    def _get_zvol_path(self, volume_name):
        """Return zvol path that corresponds given volume name."""
        return '%s/%s' % (self.dataset, volume_name)

    def _create_target_group(self, group, target):
        """Create a new target group with target member.

        :param group: group name
        :param target: group member
        """
        if not self._target_exists(target):
            LOG.debug('Create new iSCSI target %(target)s',
                      {'target': target})
            try:
                self.nms.iscsitarget.create_target({
                    'target_name': target,
                    'tpgs': self.tpgs})
            except exception.NexentaException as ex:
                if 'already configured' not in ex.args[0]:
                    raise ex
        if not self._target_group_exists(group):
            LOG.debug('Create new target group %(group)s',
                      {'group': group})
            try:
                self.nms.stmf.create_targetgroup(group)
            except exception.NexentaException as ex:
                if 'already exists' not in ex.args[0]:
                    raise ex
        if not self._target_member_in_target_group(group, target):
            LOG.debug('Add target group member %(target)s '
                      'to target group %(group)s',
                      {'target': target, 'group': group})
            try:
                self.nms.stmf.add_targetgroup_member(group, target)
            except exception.NexentaException as ex:
                if 'already exists' not in ex.args[0]:
                    raise ex

    @staticmethod
    def _get_clone_snapshot_name(volume_id):
        """Return name for snapshot that will be used to clone the volume."""
        return 'cinder-clone-snapshot-%s' % volume_id

    @staticmethod
    def _is_clone_snapshot_name(snapshot_path):
        """Check if snapshot is created for cloning."""
        volume_path, snapshot_name = snapshot_path.split('@')
        return snapshot_name.startswith('cinder-clone-snapshot-')

    def create_volume(self, volume):
        """Create a zvol on appliance.

        :param volume: volume reference
        :return: model update dict for volume reference
        """
        LOG.debug('Create volume %(volume)s',
                  {'volume': volume['name']})
        zvol_path = self._get_zvol_path(volume['name'])
        zvol_size = '%sG' % volume['size']
        zvol_bs = six.text_type(self.volume_blocksize)
        zvol_sparse = self.volume_sparse
        zvol_comp = self.volume_compression
        try:
            self.nms.zvol.create(zvol_path, zvol_size,
                                 zvol_bs, zvol_sparse)
        except exception.NexentaException as ex:
            if 'already exists' not in ex.args[0]:
                raise ex
        self.nms.zvol.set_child_prop(zvol_path, 'compression', zvol_comp)

    def extend_volume(self, volume, new_size):
        """Extend an existing volume.

        :param volume: volume reference
        :param new_size: volume new size in GB
        """
        LOG.debug('Extend volume %(volume)s, new size: %(size)sGB',
                  {'volume': volume['name'], 'size': new_size})
        zvol_path = self._get_zvol_path(volume['name'])
        self.nms.zvol.set_child_prop(zvol_path, 'volsize', '%sG' % new_size)

    def delete_volume(self, volume):
        """Deletes a logical volume.

        :param volume: volume reference
        """
        LOG.debug('Delete volume %(volume)s',
                  {'volume': volume['name']})
        zvol_path = self._get_zvol_path(volume['name'])
        try:
            origin = self.nms.zvol.get_child_props(
                zvol_path, 'origin').get('origin')
            self.nms.zvol.destroy(zvol_path, '-r')
        except exception.NexentaException as ex:
            if 'does not exist' in ex.args[0]:
                LOG.debug('Volume %(volume)s does not exist',
                          {'volume': volume['name']})
                return
            if 'has children' in ex.args[0]:
                LOG.debug('Volume %(volume)s will be deleted later',
                          {'volume': volume['name']})
                return
            raise ex
        if origin and self._is_clone_snapshot_name(origin):
            try:
                self.nms.snapshot.destroy(origin, '-d')
            except exception.NexentaException as ex:
                if 'does not exist' not in ex.args[0]:
                    LOG.debug('Snapshot %(origin)s does not exist',
                              {'origin': origin})
                    raise

    def create_cloned_volume(self, volume, src_vref):
        """Creates a clone of the specified volume.

        :param volume: new volume reference
        :param src_vref: source volume reference
        """
        snapshot_name = self._get_clone_snapshot_name(volume['id'])
        snapshot = {'name': snapshot_name,
                    'volume_id': src_vref['id'],
                    'volume_name': src_vref['name'],
                    'volume_size': src_vref['size']}
        LOG.debug('Create temporary snapshot %(snapshot)s '
                  'for the original volume %(volume)s',
                  {'snapshot': snapshot['name'],
                   'volume': snapshot['volume_name']})
        self.create_snapshot(snapshot)
        LOG.debug('Create clone %(clone)s from '
                  'temporary snapshot %(snapshot)s',
                  {'clone': volume['name'],
                   'snapshot': snapshot['name']})
        try:
            self.create_volume_from_snapshot(volume, snapshot)
        except exception.NexentaException as ex:
            LOG.debug('Volume creation failed, deleting temporary '
                      'snapshot %(volume)s@%(snapshot)s',
                      {'volume': snapshot['volume_name'],
                       'snapshot': snapshot['name']})
            try:
                self.delete_snapshot(snapshot)
            except (exception.NexentaException, exception.SnapshotIsBusy):
                LOG.debug('Failed to delete temporary snapshot '
                          '%(volume)s@%(snapshot)s',
                          {'volume': snapshot['volume_name'],
                           'snapshot': snapshot['name']})
            raise ex

    def _get_zfs_send_recv_cmd(self, src, dst):
        """Returns rrmgr command for source and destination."""
        return utils.get_rrmgr_cmd(src, dst,
                                   compression=self.rrmgr_compression,
                                   tcp_buf_size=self.rrmgr_tcp_buf_size,
                                   connections=self.rrmgr_connections)

    def get_nms_for_url(self, url):
        """Returns initialized nms object for url."""
        parsed_url = utils.parse_nms_url(url)
        args = parsed_url + (self.verify_ssl, self.lock)
        nms = jsonrpc.NexentaJSONProxy(*args)
        license = nms.appliance.get_license_info()
        signature = license.get('machine_sig')
        LOG.debug('NexentaStor Host Signature: %(signature)s',
                  {'signature': signature})
        plugin = 'nms-rsf-cluster'
        plugins = nms.plugin.get_names('')
        if isinstance(plugins, list) and plugin in plugins:
            names = nms.rsf_plugin.get_names('')
            if isinstance(names, list) and len(names) == 1:
                name = names[0]
                prop = 'machinesigs'
                props = nms.rsf_plugin.get_child_props(name, '')
                if isinstance(props, dict) and prop in props:
                    signatures = json.loads(props.get(prop))
                    if (isinstance(signatures, dict) and
                        signature in signatures.values()):
                        signature = ':'.join(sorted(signatures.values()))
                        LOG.debug('NexentaStor HA Cluster Signature: '
                                  '%(signature)s',
                                  {'signature': signature})
                    else:
                        LOG.debug('HA Cluster plugin %(plugin)s is not '
                                  'configured for NexentaStor Host '
                                  '%(signature)s: %(signatures)s',
                                  {'plugin': plugin,
                                   'signature': signature,
                                   'signatures': signatures})
                else:
                    LOG.debug('HA Cluster plugin %(plugin)s is misconfigured',
                              {'plugin': plugin})
            else:
                LOG.debug('HA Cluster plugin %(plugin)s is not configured '
                          'or is misconfigured',
                          {'plugin': plugin})
        else:
            LOG.debug('HA Cluster plugin %(plugin)s is not installed',
                      {'plugin': plugin})

        lock = hashlib.md5(signature).hexdigest()
        LOG.debug('NMS coordination lock: %(lock)s',
                  {'lock': lock})
        args = parsed_url + (self.verify_ssl, lock)
        return jsonrpc.NexentaJSONProxy(*args)

    def migrate_volume(self, ctxt, volume, host):
        """Migrate if volume and host are managed by Nexenta appliance.

        :param ctxt: context
        :param volume: a dictionary describing the volume to migrate
        :param host: a dictionary describing the host to migrate to
        """
        LOG.debug('Migrate volume %(volume)s to host %(host)s',
                  {'volume': volume['name'],
                   'host': host})

        false_ret = (False, None)

        if volume['status'] not in ('available', 'retyping'):
            return false_ret

        if 'capabilities' not in host:
            return false_ret

        capabilities = host['capabilities']

        if ('location_info' not in capabilities or
                'iscsi_target_portal_port' not in capabilities or
                'nms_url' not in capabilities):
            return false_ret

        nms_url = capabilities['nms_url']
        dst_parts = capabilities['location_info'].split(':')

        if (capabilities.get('vendor_name') != 'Nexenta' or
                dst_parts[0] != self.__class__.__name__ or
                capabilities['free_capacity_gb'] < volume['size']):
            return false_ret

        dst_host, dst_volume = dst_parts[1:]

        ssh_bound = False
        ssh_bindings = self.nms.appliance.ssh_list_bindings()
        for bind in ssh_bindings:
            if dst_host.startswith(bind.split('@')[1].split(':')[0]):
                ssh_bound = True
                break
        if not ssh_bound:
            LOG.warning(_LW('Remote NexentaStor appliance at %s should be '
                            'SSH-bound.'), dst_host)

        # Create temporary snapshot of volume on NexentaStor Appliance.
        snapshot = {
            'volume_name': volume['name'],
            'name': utils.get_migrate_snapshot_name(volume)
        }
        self.create_snapshot(snapshot)

        src = '%(volume)s/%(zvol)s@%(snapshot)s' % {
            'volume': self.volume,
            'zvol': volume['name'],
            'snapshot': snapshot['name']
        }
        dst = ':'.join([dst_host, dst_volume])

        try:
            self.nms.appliance.execute(self._get_zfs_send_recv_cmd(src, dst))
        except exception.NexentaException as exc:
            LOG.warning(_LW('Cannot send source snapshot %(src)s to '
                            'destination %(dst)s. Reason: %(exc)s'),
                        {'src': src, 'dst': dst, 'exc': exc})
            return false_ret
        finally:
            try:
                self.delete_snapshot(snapshot)
            except exception.NexentaException as exc:
                LOG.warning(_LW('Cannot delete temporary source snapshot '
                                '%(src)s on NexentaStor Appliance: %(exc)s'),
                            {'src': src, 'exc': exc})
        try:
            self.delete_volume(volume)
        except exception.NexentaException as exc:
            LOG.warning(_LW('Cannot delete source volume %(volume)s on '
                            'NexentaStor Appliance: %(exc)s'),
                        {'volume': volume['name'], 'exc': exc})

        dst_nms = self.get_nms_for_url(nms_url)
        dst_snapshot = '%s/%s@%s' % (dst_volume, volume['name'],
                                     snapshot['name'])
        try:
            dst_nms.snapshot.destroy(dst_snapshot, '')
        except exception.NexentaException as exc:
            LOG.warning(_LW('Cannot delete temporary destination snapshot '
                            '%(dst)s on NexentaStor Appliance: %(exc)s'),
                        {'dst': dst_snapshot, 'exc': exc})
        return True, None

    def retype(self, context, volume, new_type, diff, host):
        """Convert the volume to be of the new type.

        :param ctxt: Context
        :param volume: A dictionary describing the volume to migrate
        :param new_type: A dictionary describing the volume type to convert to
        :param diff: A dictionary with the difference between the two types
        :param host: A dictionary describing the host to migrate to, where
                     host['host'] is its name, and host['capabilities'] is a
                     dictionary of its reported capabilities.
        """
        LOG.debug('Retype volume request %(vol)s to be %(type)s '
                  '(host: %(host)s), diff %(diff)s.',
                  {'vol': volume['name'],
                   'type': new_type,
                   'host': host,
                   'diff': diff})

        options = dict(
            compression='compression',
            dedup='dedup',
            description='nms:description'
        )

        retyped = False
        migrated = False

        capabilities = host['capabilities']
        src_backend = self.__class__.__name__
        dst_backend = capabilities['location_info'].split(':')[0]
        if src_backend != dst_backend:
            LOG.warning(_LW('Cannot retype from %(src_backend)s to '
                            '%(dst_backend)s.'),
                        {
                        'src_backend': src_backend,
                        'dst_backend': dst_backend,
                        })
            return False

        hosts = (volume['host'], host['host'])
        old, new = hosts
        if old != new:
            migrated, provider_location = self.migrate_volume(
                context, volume, host)

        if not migrated:
            nms = self.nms
        else:
            nms_url = capabilities['nms_url']
            nms = self.get_nms_for_url(nms_url)

        zvol = '%s/%s' % (
            capabilities['location_info'].split(':')[-1], volume['name'])

        for opt in options:
            old, new = diff.get('extra_specs').get(opt, (False, False))
            if old != new:
                LOG.debug('Changing %(opt)s from %(old)s to %(new)s.',
                          {'opt': opt, 'old': old, 'new': new})
                try:
                    nms.zvol.set_child_prop(
                        zvol, options[opt], new)
                    retyped = True
                except exception.NexentaException:
                    LOG.error(_LE('Error trying to change %(opt)s'
                                  ' from %(old)s to %(new)s'),
                              {'opt': opt, 'old': old, 'new': new})
                    return False, None
        return retyped or migrated, None

    def create_snapshot(self, snapshot):
        """Create snapshot of existing zvol on appliance.

        :param snapshot: snapshot reference
        """
        LOG.debug('Create snapshot %(snapshot)s for volume %(volume)s',
                  {'snapshot': snapshot['name'],
                   'volume': snapshot['volume_name']})
        zvol_path = self._get_zvol_path(snapshot['volume_name'])
        self.nms.zvol.create_snapshot(zvol_path, snapshot['name'], '')

    def create_volume_from_snapshot(self, volume, snapshot):
        """Create new volume from other's snapshot on appliance.

        :param volume: reference of volume to be created
        :param snapshot: reference of source snapshot
        """
        LOG.debug('Create volume %(volume)s from snapshot: %(snapshot)s',
                 {'volume': volume['name'],
                  'snapshot': snapshot['name']})
        src_zvol_path = self._get_zvol_path(snapshot['volume_name'])
        snapshot_path = '%s@%s' % (src_zvol_path, snapshot['name'])
        dst_zvol_path = self._get_zvol_path(volume['name'])
        self.nms.zvol.clone(snapshot_path, dst_zvol_path)
        if (('size' in volume) and (
                volume['size'] > snapshot['volume_size'])):
            self.extend_volume(volume, volume['size'])

    def delete_snapshot(self, snapshot):
        """Delete volume's snapshot on appliance.

        :param snapshot: snapshot reference
        """
        LOG.debug('Delete snapshot: %(snapshot)s',
                  {'snapshot': snapshot['name']})
        zvol_path = self._get_zvol_path(snapshot['volume_name'])
        snapshot_name = '%s@%s' % (zvol_path, snapshot['name'])
        try:
            self.nms.snapshot.destroy(snapshot_name, '-d')
        except exception.NexentaException as ex:
            if 'does not exist' not in ex.args[0]:
                LOG.debug('Snapshot %(snapshot)s does not exist',
                          {'snapshot': snapshot['name']})
                raise ex

    def local_path(self, volume):
        """Return local path to existing local volume.

        We never have local volumes, so it raises NotImplementedError.

        :raise: :py:exc:`NotImplementedError`
        """
        raise NotImplementedError

    def _target_exists(self, target):
        """Check if iSCSI target exist.

        :param target: target name
        :return: True if target exist, else False
        """
        targets = self.nms.stmf.list_targets()
        return target in targets

    def _target_group_exists(self, target_group):
        """Check if target group exist.

        :param target_group: target group
        :return: True if target group exist, else False
        """
        target_groups = self.nms.stmf.list_targetgroups()
        return target_group in target_groups

    def _target_member_in_target_group(self, target_group, target):
        """Check if target member in target group.

        :param target_group: target group
        :param target: target member
        :return: True if target member in target group, else False
        :raises: NexentaException if target group doesn't exist
        """
        targets = self.nms.stmf.list_targetgroup_members(target_group)
        return target in targets

    def _lu_exists(self, zvol_path):
        """Check if LU exists.

        :param zvol_path: zvol path
        :raises: NexentaException if zvol not exists
        :return: True if LU exists, else False
        """
        try:
            return bool(self.nms.scsidisk.lu_exists(zvol_path))
        except exception.NexentaException as ex:
            if 'does not exist' not in ex.args[0]:
                raise
            return False

    def _is_lu_shared(self, zvol_path):
        """Check if LU exists and shared.

        :param zvol_path: zvol path
        :raises: NexentaException if zvol not exist
        :return: True if LU exists and shared, else False
        """
        try:
            return self.nms.scsidisk.lu_shared(zvol_path) > 0
        except exception.NexentaException as ex:
            if 'does not exist for zvol' not in ex.args[0]:
                raise ex
            return False

    def create_export(self, _ctx, volume, connector):
        """Export a volume."""
        pass

    def ensure_export(self, _ctx, volume):
        """Synchronously recreate an export for a volume."""
        pass

    def remove_export(self, _ctx, volume):
        """Remove an export for a volume."""
        pass

    def _get_host_addresses(self):
        """Return NexentaStor IP addresses list."""
        addresses = []
        items = self.nms.appliance.execute('ipadm show-addr -p -o addr')
        for item in items:
            cidr = six.text_type(item)
            addr, mask = cidr.split('/')
            obj = ipaddress.ip_address(addr)
            if obj.is_loopback:
                LOG.debug('Skip loopback IP address %(addr)s',
                          {'addr': addr})
                continue
            addresses.append(obj.exploded)
        LOG.debug('Configured IP addresses: %(addresses)s',
                  {'addresses': addresses})
        return addresses

    def _get_host_portals(self):
        """Return NexentaStor iSCSI portals list."""
        portals = []
        addresses = self._get_host_addresses()
        for address in addresses:
            portal = '%s:%s' % (address, self.portal_port)
            portals.append(portal)
        LOG.debug('Configured iSCSI portals: %(portals)s',
                  {'portals': portals})
        return portals

    def _get_host_portal_groups(self):
        """Return NexentaStor iSCSI portal groups dictionary."""
        portal_groups = {}
        default_portal = '%s:%s' % (self.nms_host, self.portal_port)
        host_portals = self._get_host_portals()
        host_portal_groups = self.nms.iscsitarget.list_tpg()
        conf_portal_groups = []
        if self.tpgs:
            conf_portal_groups = self.tpgs.split(',')
        else:
            LOG.debug('Cinder target portal groups '
                      'parameter is not configured')
        for group in host_portal_groups:
            group_portals = host_portal_groups[group]
            if not conf_portal_groups:
                if default_portal in group_portals:
                    LOG.debug('Use default portal %(portal)s '
                              'for portal group %(group)s',
                              {'portal': default_portal,
                               'group': group})
                    portal_groups[group] = [default_portal]
                continue
            if group not in conf_portal_groups:
                LOG.debug('Skip existing but not configured '
                          'target portal group %(group)s',
                          {'group': group})
                continue
            portals = []
            for portal in group_portals:
                if portal not in host_portals:
                    LOG.debug('Skip non-existing '
                              'portal %(portal)s',
                              {'portal': portal})
                    continue
                portals.append(portal)
            if portals:
                portal_groups[group] = portals
        LOG.debug('Configured host target '
                  'portal groups: %(groups)s',
                  {'groups': portal_groups})
        return portal_groups

    def _get_host_targets(self):
        """Return NexentaStor iSCSI targets dictionary."""
        targets = {}
        default_portal = '%s:%s' % (self.nms_host, self.portal_port)
        host_portal_groups = self._get_host_portal_groups()
        stmf_targets = self.nms.stmf.list_targets()
        for name in stmf_targets:
            if not name.startswith(self.target_prefix):
                LOG.debug('Skip not a cinder target %(name)s',
                          {'name': name})
                continue
            target = stmf_targets[name]
            if not ('protocol' in target and target['protocol'] == 'iSCSI'):
                LOG.debug('Skip non-iSCSI target %(target)s',
                          {'target': target})
                continue
            if not ('status' in target and target['status'] == 'Online'):
                LOG.debug('Skip non-online iSCSI target %(target)s',
                          {'target': target})
                continue
            target_portals = []
            props = self.nms.iscsitarget.get_target_props(name)
            if 'tpgs' in props:
                target_portal_groups = props['tpgs'].split(',')
                for group in target_portal_groups:
                    if group not in host_portal_groups:
                        LOG.debug('Skip existing but unsuitable target portal '
                                  'group %(group)s for iSCSI target %(name)s',
                                  {'group': group, 'name': name})
                        continue
                    portals = host_portal_groups[group]
                    target_portals += portals
            else:
                portals = [default_portal]
                target_portals += portals
            if target_portals:
                targets[name] = target_portals
        LOG.debug('Configured iSCSI targets: %(targets)s',
                  {'targets': targets})
        return targets

    def _target_group_props(self, group_name, host_targets):
        """Check existing targets/portals for given target group.

        :param group_name: target group name
        :param host_targets: host targets dictionary
        :returns: dictionary of portals per target
        """
        if not group_name.startswith(self.target_group_prefix):
            LOG.debug('Skip not a cinder target group %(group)s',
                      {'group': group_name})
            return {}
        group_targets = self.nms.stmf.list_targetgroup_members(group_name)
        if not group_targets:
            LOG.debug('Skip target group %(group)s: group has no members',
                      {'group': group_name})
            return {}
        group_props = {}
        for target_name in group_targets:
            if target_name not in host_targets:
                LOG.debug('Skip existing but unsuitable member '
                          'of target group %(group)s: %(target)s',
                          {'group': group_name,
                           'target': target_name})
                continue
            portals = host_targets[target_name]
            if not portals:
                LOG.debug('Skip existing but unsuitable member '
                          'of target group %(group)s: %(target)s',
                          {'group': group_name,
                           'target': target_name})
                continue
            LOG.debug('Found member of target group %(group)s: '
                      'iSCSI target %(target)s listening on '
                      'portals %(portals)s',
                      {'group': group_name,
                       'target': target_name,
                       'portals': portals})
            group_props[target_name] = portals
        LOG.debug('Target group %(group)s members: %(members)s',
                  {'group': group_name,
                   'members': group_props})
        return group_props

    def initialize_connection(self, volume, connector):
        """Do all steps to get zfs volume exported at separate target.
        :param volume: volume reference
        :param connector: connector reference
        :returns: dictionary of connection information
        """
        zvol_path = self._get_zvol_path(volume['name'])
        host_iqn = connector.get('initiator')
        LOG.debug('Initialize connection for volume: %(volume)s '
                  'and initiator: %(initiator)s',
                  {'volume': volume['name'],
                   'initiator': host_iqn})

        suffix = uuid.uuid4().hex
        host_targets = self._get_host_targets()
        host_groups = ['All']
        host_group = self._get_host_group(host_iqn)
        if host_group:
            host_groups.append(host_group)

        props_portals = []
        props_iqns = []
        props_luns = []
        mappings = []

        if self._is_lu_shared(zvol_path):
            mappings = self.nms.scsidisk.list_lun_mapping_entries(zvol_path)

        for mapping in mappings:
            mapping_lu = int(mapping['lun'])
            mapping_hg = mapping['host_group']
            mapping_tg = mapping['target_group']
            mapping_id = mapping['entry_number']
            if mapping_tg == 'All':
                LOG.debug('Delete LUN mapping %(id)s '
                          'for target group %(tg)s',
                          {'id': mapping_id,
                           'tg': mapping_tg})
                self.nms.scsidisk.remove_lun_mapping_entry(zvol_path,
                                                           mapping_id)
                continue
            if mapping_hg not in host_groups:
                LOG.debug('Skip LUN mapping %(id)s '
                          'for host group %(hg)s',
                          {'id': mapping_id,
                           'hg': mapping_hg})
                continue
            group_props = self._target_group_props(mapping_tg,
                                                   host_targets)
            if not group_props:
                LOG.debug('Skip LUN mapping %(id)s'
                          'for target group %(tg)s',
                          {'id': mapping_id,
                           'tg': mapping_tg})
                continue
            for target_iqn in group_props:
                target_portals = group_props[target_iqn]
                props_portals += target_portals
                props_iqns += [target_iqn] * len(target_portals)
                props_luns += [mapping_lu] * len(target_portals)

        props = {}
        props['target_discovered'] = False
        props['encrypted'] = False
        props['qos_specs'] = None
        props['volume_id'] = volume['id']
        props['access_mode'] = 'rw'
        multipath = connector.get('multipath', False)

        if props_luns:
            if multipath:
                props['target_portals'] = props_portals
                props['target_iqns'] = props_iqns
                props['target_luns'] = props_luns
            else:
                index = random.randrange(0, len(props_luns))
                props['target_portal'] = props_portals[index]
                props['target_iqn'] = props_iqns[index]
                props['target_lun'] = props_luns[index]
            LOG.debug('Use existing LUN mapping %(props)s',
                      {'props': props})
            return {'driver_volume_type': 'iscsi', 'data': props}

        if host_group is None:
            host_group = '%s-%s' % (self.host_group_prefix, suffix)
            self._create_host_group(host_group, host_iqn)
        else:
            LOG.debug('Use existing host group %(group)s',
                      {'group': host_group})

        mappings_stat = {}
        group_targets = {}
        target_groups = self.nms.stmf.list_targetgroups()
        for target_group in target_groups:
            if (target_group in self.mappings and
                    self.mappings[target_group] >= self.lpt):
                LOG.debug('Skip target group %(group)s: '
                          'mappings limit exceeded %(count)s',
                          {'group': target_group,
                           'count': self.mappings[target_group]})
                continue
            group_props = self._target_group_props(target_group,
                                                   host_targets)
            if not group_props:
                LOG.debug('Skip unsuitable target group %(group)s',
                          {'group': target_group})
                continue
            group_targets[target_group] = group_props
            if target_group in self.mappings:
                mappings_stat[target_group] = self.mappings[target_group]
            else:
                mappings_stat[target_group] = self.mappings[target_group] = 0

        if mappings_stat:
            target_group = min(mappings_stat, key=mappings_stat.get)
            LOG.debug('Use existing target group %(group)s '
                      'with number of mappings %(count)s',
                      {'group': target_group,
                       'count': mappings_stat[target_group]})
        else:
            target = '%s:%s' % (self.target_prefix, suffix)
            target_group = '%s-%s' % (self.target_group_prefix, suffix)
            self._create_target_group(target_group, target)
            host_targets = self._get_host_targets()
            group_props = self._target_group_props(target_group, host_targets)
            group_targets[target_group] = group_props
            self.mappings[target_group] = 0

        if not self._lu_exists(zvol_path):
            try:
                self.nms.scsidisk.create_lu(zvol_path, {})
            except exception.NexentaException as ex:
                if 'in use' not in ex.args[0]:
                    raise ex

        entry = self.nms.scsidisk.add_lun_mapping_entry(
            zvol_path, {'target_group': target_group,
                        'host_group': host_group})

        self.mappings[target_group] += 1
        lun = int(entry['lun'])

        targets = group_targets[target_group]
        for target in targets:
            portals = targets[target]
            props_portals += portals
            props_iqns += [target] * len(portals)
            props_luns += [lun] * len(portals)

        if multipath:
            props['target_portals'] = props_portals
            props['target_iqns'] = props_iqns
            props['target_luns'] = props_luns
        else:
            index = random.randrange(0, len(props_luns))
            props['target_portal'] = props_portals[index]
            props['target_iqn'] = props_iqns[index]
            props['target_lun'] = props_luns[index]

        LOG.debug('Created new LUN mapping: %(props)s',
                  {'props': props})
        return {'driver_volume_type': 'iscsi', 'data': props}

    def _get_host_group(self, member):
        """Find existing host group by group member.

        :param member: host group member
        :returns: host group name
        """
        groups = self.nms.stmf.list_hostgroups()
        for group in groups:
            members = self.nms.stmf.list_hostgroup_members(group)
            if member in members:
                LOG.debug('Found host group %(group)s '
                          'for member %(member)s',
                          {'group': group,
                           'member': member})
                return group
        return None

    def _create_host_group(self, group, member):
        """Create a new host group.

        :param group: host group name
        :param member: host group member
        """
        LOG.debug('Create new host group %(group)s',
                  {'group': group})
        self.nms.stmf.create_hostgroup(group)
        LOG.debug('Add group member %(member)s '
                  'to host group %(group)s',
                  {'member': member,
                   'group': group})
        self.nms.stmf.add_hostgroup_member(group, member)

    def terminate_connection(self, volume, connector, **kwargs):
        """Terminate a connection to a volume.
        :param volume: a volume object
        :param connector: a connector object
        :returns: dictionary of connection information
        """

        info = {'driver_volume_type': 'iscsi', 'data': {}}
        zvol_path = self._get_zvol_path(volume['name'])
        host_groups = []
        host_iqn = None

        if isinstance(connector, dict) and 'initiator' in connector:
            host_iqn = connector.get('initiator')
            host_groups.append('All')
            host_group = self._get_host_group(host_iqn)
            if host_group is not None:
                host_groups.append(host_group)
            LOG.debug('Terminate connection for '
                      'volume %(volume)s and '
                      'initiator %(initiator)s',
                      {'volume': volume['name'],
                       'initiator': host_iqn})
        else:
            LOG.debug('Terminate all connections '
                      'for volume %(volume)s',
                      {'volume': volume['name']})

        if not self._is_lu_shared(zvol_path):
            LOG.debug('There are no LUN mappings '
                      'found for volume %(volume)s',
                      {'volume': volume['name']})
            return info

        mappings = self.nms.scsidisk.list_lun_mapping_entries(zvol_path)
        for mapping in mappings:
            mapping_lu = int(mapping['lun'])
            mapping_hg = mapping['host_group']
            mapping_tg = mapping['target_group']
            mapping_id = mapping['entry_number']
            if host_iqn is None or mapping_hg in host_groups:
                LOG.debug('Delete LUN mapping %(id)s '
                          'for volume %(volume)s, '
                          'target group %(tg)s and '
                          'host group %(hg)s',
                          {'id': mapping_id,
                           'volume': volume['name'],
                           'tg': mapping_tg,
                           'hg': mapping_hg})
                if (mapping_tg in self.mappings and
                        self.mappings[mapping_tg] > 0):
                    self.mappings[mapping_tg] -= 1
                else:
                    self.mappings[mapping_tg] = 0
                self.nms.scsidisk.remove_lun_mapping_entry(zvol_path,
                                                           mapping_id)
            else:
                LOG.debug('Skip LUN mapping %(id)s '
                          'for volume %(volume)s, '
                          'target group %(tg)s and '
                          'host group %(hg)s',
                          {'id': mapping_id,
                           'volume': volume['name'],
                           'tg': mapping_tg,
                           'hg': mapping_hg})
        return info

    def create_consistencygroup(self, context, group):
        """Creates a consistencygroup.

        :param context: the context of the caller.
        :param group: the dictionary of the consistency group to be created.
        :returns: group_model_update
        """
        group_model_update = {}
        return group_model_update

    def delete_consistencygroup(self, context, group, volumes):
        """Deletes a consistency group.

        :param context: the context of the caller.
        :param group: the dictionary of the consistency group to be deleted.
        :param volumes: a list of volume dictionaries in the group.
        :returns: group_model_update, volumes_model_update
        """
        group_model_update = {}
        volumes_model_update = []
        for volume in volumes:
            self.delete_volume(volume)
        return group_model_update, volumes_model_update

    def update_consistencygroup(self, context, group,
                                add_volumes=None,
                                remove_volumes=None):
        """Updates a consistency group.

        :param context: the context of the caller.
        :param group: the dictionary of the consistency group to be updated.
        :param add_volumes: a list of volume dictionaries to be added.
        :param remove_volumes: a list of volume dictionaries to be removed.
        :returns: group_model_update, add_volumes_update, remove_volumes_update
        """
        group_model_update = {}
        add_volumes_update = []
        remove_volumes_update = []
        return group_model_update, add_volumes_update, remove_volumes_update

    def create_cgsnapshot(self, context, cgsnapshot, snapshots):
        """Creates a consistency group snapshot.

        :param context: the context of the caller.
        :param cgsnapshot: the dictionary of the cgsnapshot to be created.
        :param snapshots: a list of snapshot dictionaries in the cgsnapshot.
        :returns: group_model_update, snapshots_model_update
        """
        group_model_update = {}
        snapshots_model_update = []
        cgsnapshot_name = 'cgsnapshot-%s' % cgsnapshot['id']
        self.nms.folder.create_snapshot(self.dataset, cgsnapshot_name, '-r')
        for snapshot in snapshots:
            volume_path = self._get_zvol_path(snapshot['volume_name'])
            cgsnapshot_path = '%s@%s' % (volume_path, cgsnapshot_name)
            self.nms.snapshot.rename(cgsnapshot_path, snapshot['name'], '')
        cgsnapshot_path = '%s@%s' % (self.dataset, cgsnapshot_name)
        self.nms.snapshot.destroy(cgsnapshot_path, '-rd')
        return group_model_update, snapshots_model_update

    def delete_cgsnapshot(self, context, cgsnapshot, snapshots):
        """Deletes a consistency group snapshot.

        :param context: the context of the caller.
        :param cgsnapshot: the dictionary of the cgsnapshot to be created.
        :param snapshots: a list of snapshot dictionaries in the cgsnapshot.
        :returns: group_model_update, snapshots_model_update
        """
        group_model_update = {}
        snapshots_model_update = []
        for snapshot in snapshots:
            self.delete_snapshot(snapshot)
        return group_model_update, snapshots_model_update

    def create_consistencygroup_from_src(self, context, group, volumes,
                                         cgsnapshot=None, snapshots=None,
                                         source_cg=None, source_vols=None):
        """Creates a consistency group from source.

        :param context: the context of the caller.
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
            cgsnapshot_name = 'cgsnapshot-%s' % group['id']
            self.nms.folder.create_snapshot(self.dataset, cgsnapshot_name, '-r')
            for volume, source_vol in zip(volumes, source_vols):
                source_vol_path = self._get_zvol_path(source_vol['name'])
                cgsnapshot_path = '%s@%s' % (source_vol_path, cgsnapshot_name)
                snapshot_name = self._get_clone_snapshot_name(volume['id'])
                self.nms.snapshot.rename(cgsnapshot_path, snapshot_name, '')
                snapshot = {'name': snapshot_name,
                            'volume_id': source_vol['id'],
                            'volume_name': source_vol['name'],
                            'volume_size': source_vol['size']}
                self.create_volume_from_snapshot(volume, snapshot)
            cgsnapshot_path = '%s@%s' % (self.dataset, cgsnapshot_name)
            self.nms.snapshot.destroy(cgsnapshot_path, '-rd')
        return group_model_update, volumes_model_update

    def get_volume_stats(self, refresh=False):
        """Get volume stats.

        If 'refresh' is True, run update the stats first.
        """
        if refresh:
            self._update_volume_stats()

        return self._stats

    def _update_volume_stats(self):
        """Retrieve stats info for NexentaStor appliance."""
        LOG.debug('Update backend %(backend)s statistics',
                  {'backend': self.backend_name})

        stats = self.nms.folder.get_child_props(self.dataset, 'available|used')
        free_amount = utils.str2gib_size(stats['available'])
        used_amount = utils.str2gib_size(stats['used'])
        total_amount = free_amount + used_amount

        location_info = '%(driver)s:%(host)s:%(dataset)s' % {
            'driver': self.__class__.__name__,
            'host': self.nms_host,
            'dataset': self.dataset
        }

        self._stats = {
            'vendor_name': 'Nexenta',
            'dedup': self.volume_deduplication,
            'compression': self.volume_compression,
            'description': self.volume_description,
            'driver_version': self.VERSION,
            'storage_protocol': 'iSCSI',
            'total_capacity_gb': total_amount,
            'free_capacity_gb': free_amount,
            'provisioned_capacity_gb': used_amount,
            'reserved_percentage': self.configuration.reserved_percentage,
            'multiattach': False,
            'QoS_support': False,
            'thin_provisioning_support': self.volume_sparse,
            'consistencygroup_support': True,
            'consistent_group_snapshot_enabled': True,
            'volume_backend_name': self.backend_name,
            'location_info': location_info,
            'iscsi_target_portal_port': self.portal_port,
            'nms_url': self.nms.url
        }

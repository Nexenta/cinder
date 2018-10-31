# Copyright 2018 Nexenta Systems, Inc.
# All Rights Reserved.
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
import math
import random
import six
import uuid

from eventlet import greenthread
from oslo_log import log as logging
from oslo_utils import units
from six.moves import urllib

from cinder import exception
from cinder.i18n import _
from cinder import interface
from cinder.volume import driver
from cinder.volume.drivers.nexenta.ns5 import jsonrpc
from cinder.volume.drivers.nexenta import options
from cinder.volume.drivers.nexenta import utils

VERSION = '1.3.8'
LOG = logging.getLogger(__name__)


@interface.volumedriver
class NexentaISCSIDriver(driver.ISCSIDriver):
    """Executes volume driver commands on Nexenta Appliance.

    Version history:
        1.0.0 - Initial driver version.
        1.1.0 - Added HTTPS support.
                Added use of sessions for REST calls.
                Added abandoned volumes and snapshots cleanup.
        1.2.0 - Failover support.
        1.2.1 - Configurable luns per parget, target prefix.
        1.3.0 - Removed target/TG caching, added support for target portals
                and host groups.
        1.3.1 - Refactored _do_export to query exact lunMapping.
        1.3.2 - Revert to snapshot support.
        1.3.3 - Refactored LUN creation, use host group for LUN mappings.
        1.3.4 - Adapted NexentaException for the latest Cinder.
        1.3.5 - Added deferred deletion for snapshots.
        1.3.6 - Fixed race between volume/clone deletion.
        1.3.7 - Added consistency group support.
        1.3.8 - Added volume multi-attach.
    """

    VERSION = VERSION

    # ThirdPartySystems wiki page
    CI_WIKI_NAME = "Nexenta_CI"

    def __init__(self, *args, **kwargs):
        super(NexentaISCSIDriver, self).__init__(*args, **kwargs)
        self.nef = None
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
        self.luns_per_target = self.configuration.nexenta_luns_per_target
        self.lu_writebackcache_disabled = (
            self.configuration.nexenta_lu_writebackcache_disabled)
        self.use_https = self.configuration.nexenta_use_https
        self.nef_host = self.configuration.nexenta_rest_address
        self.iscsi_host = self.configuration.nexenta_host
        self.nef_port = self.configuration.nexenta_rest_port
        self.nef_user = self.configuration.nexenta_user
        self.nef_password = self.configuration.nexenta_password
        self.storage_pool = self.configuration.nexenta_volume
        self.volume_group = self.configuration.nexenta_volume_group
        self.portal_port = self.configuration.nexenta_iscsi_target_portal_port
        self.portals = self.configuration.nexenta_iscsi_target_portals
        self.dataset_compression = (
            self.configuration.nexenta_dataset_compression)
        self.dataset_deduplication = self.configuration.nexenta_dataset_dedup
        self.dataset_description = (
            self.configuration.nexenta_dataset_description)
        self.iscsi_target_portal_port = (
            self.configuration.nexenta_iscsi_target_portal_port)
        self.dataset = '%s/%s' % (self.storage_pool, self.volume_group)

    @property
    def backend_name(self):
        backend_name = None
        if self.configuration:
            backend_name = self.configuration.safe_get('volume_backend_name')
        if not backend_name:
            backend_name = self.__class__.__name__
        return backend_name

    def do_setup(self, context):
        host = self.nef_host or self.iscsi_host
        self.nef = jsonrpc.NexentaJSONProxy(
            host, self.nef_port, self.nef_user, self.nef_password,
            self.use_https, self.storage_pool, self.verify_ssl)
        url = 'storage/volumeGroups'
        data = {
            'path': '/'.join([self.storage_pool, self.volume_group]),
            'volumeBlockSize': (
                self.configuration.nexenta_ns5_blocksize * units.Ki)
        }
        try:
            self.nef.post(url, data)
        except exception.NexentaException as ex:
            err = utils.ex2err(ex)
            if err['code'] == 'EEXIST':
                LOG.debug('Volume group %(group)s already exists',
                          {'group': self.volume_group})
            else:
                raise ex

    def check_for_setup_error(self):
        """Verify that the zfs pool, vg and iscsi service exists."""
        url = 'storage/pools/%s' % self.storage_pool
        self.nef.get(url)
        url = 'storage/volumeGroups/%s' % urllib.parse.quote_plus(
            self.dataset)
        try:
            self.nef.get(url)
        except exception.NexentaException:
            msg = (_('Volume group %(group)s not found')
                   % {'group': self.volume_group})
            raise exception.NexentaException(msg)
        services = self.nef.get('services')
        for service in services['data']:
            if service['name'] == 'iscsit':
                if service['state'] != 'online':
                    msg = _('iSCSI target service is not running')
                    raise exception.NexentaException(msg)
                break

    def create_volume(self, volume):
        """Create a zfs volume on appliance.

        :param volume: volume reference
        :returns: model update dict for volume reference
        """
        LOG.debug('Create volume %(volume)s',
                  {'volume': volume['name']})
        url = 'storage/volumes'
        path = '/'.join([self.storage_pool, self.volume_group, volume['name']])
        data = {
            'path': path,
            'volumeSize': volume['size'] * units.Gi,
            'volumeBlockSize': (
                self.configuration.nexenta_ns5_blocksize * units.Ki),
            'sparseVolume': self.configuration.nexenta_sparse
        }
        self.nef.post(url, data)

    def delete_volume(self, volume):
        """Deletes a logical volume.

        :param volume: volume reference
        """
        LOG.debug('Delete volume %(volume)s',
                  {'volume': volume['name']})
        path = self._get_volume_path(volume['name'])
        params = {'path': path}
        url = 'storage/volumes?%s' % (
            urllib.parse.urlencode(params))
        data = self.nef.get(url).get('data')
        if not data:
            return
        params = {'snapshots': True}
        url = 'storage/volumes/%s?%s' % (
            urllib.parse.quote_plus(path),
            urllib.parse.urlencode(params))
        try:
            self.nef.delete(url)
        except exception.NexentaException as ex:
            err = utils.ex2err(ex)
            if err['code'] == 'EEXIST':
                params = {'parent': path}
                url = 'storage/snapshots?%s' % (
                    urllib.parse.urlencode(params))
                snap_map = {}
                for snap in self.nef.get(url)['data']:
                    url = 'storage/snapshots/%s' % (
                        urllib.parse.quote_plus(snap['path']))
                    data = self.nef.get(url)
                    if data and data.get('clones'):
                        snap_map[data['creationTxg']] = snap['path']
                if snap_map:
                    snap = snap_map[max(snap_map)]
                    url = 'storage/snapshots/%s' % urllib.parse.quote_plus(
                        snap)
                    clone = self.nef.get(url)['clones'][0]
                    url = 'storage/volumes/%s/promote' % (
                        urllib.parse.quote_plus(clone))
                    self.nef.post(url)
                params = {'snapshots': True}
                url = 'storage/volumes/%s?%s' % (
                    urllib.parse.quote_plus(path),
                    urllib.parse.urlencode(params))
                self.nef.delete(url)
            else:
                raise ex

    def extend_volume(self, volume, new_size):
        """Extend an existing volume.

        :param volume: volume reference
        :param new_size: volume new size in GB
        """
        LOG.debug('Extend volume %(volume)s, new size: %(size)sGB',
                  {'volume': volume['name'],
                   'size': new_size})
        volume_path = self._get_volume_path(volume['name'])
        url = 'storage/volumes/%s' % urllib.parse.quote_plus(volume_path)
        self.nef.put(url, {'volumeSize': new_size * units.Gi})

    def create_snapshot(self, snapshot):
        """Creates a snapshot.

        :param snapshot: snapshot reference
        """
        LOG.info('Create snapshot %(snapshot)s for volume %(volume)s',
                 {'snapshot': snapshot['name'],
                  'volume': snapshot['volume_name']})
        volume_path = self._get_volume_path(snapshot['volume_name'])
        url = 'storage/snapshots'
        data = {'path': '%s@%s' % (volume_path, snapshot['name'])}
        self.nef.post(url, data)

    def delete_snapshot(self, snapshot):
        """Deletes a snapshot.

        :param snapshot: snapshot reference
        """
        LOG.debug('Delete snapshot: %(snapshot)s',
                  {'snapshot': snapshot['name']})
        volume_path = self._get_volume_path(snapshot['volume_name'])
        snapshot_path = '%s@%s' % (volume_path, snapshot['name'])
        self._delete_snapshot(snapshot_path)

    def snapshot_revert_use_temp_snapshot(self):
        # Considering that NexentaStor based drivers use COW images
        # for storing snapshots, having chains of such images,
        # creating a backup snapshot when reverting one is not
        # actually helpful.
        return False

    def revert_to_snapshot(self, context, volume, snapshot):
        """Revert volume to snapshot."""
        LOG.debug('Revert volume %(volume)s to snapshot %(snapshot)s',
                  {'volume': volume['name'],
                   'snapshot': snapshot['name']})
        volume_path = self._get_volume_path(volume['name'])
        url = 'storage/volumes/%s/rollback' % urllib.parse.quote_plus(
            volume_path)
        self.nef.post(url, {'snapshot': snapshot['name']})

    def create_volume_from_snapshot(self, volume, snapshot):
        """Create new volume from other's snapshot on appliance.

        :param volume: reference of volume to be created
        :param snapshot: reference of source snapshot
        """
        LOG.debug('Create volume %(volume)s from snapshot: %(snapshot)s',
                  {'volume': volume['name'],
                   'snapshot': snapshot['name']})
        target_path = self._get_volume_path(volume['name'])
        source_path = self._get_volume_path(snapshot['volume_name'])
        snapshot_path = '%s@%s' % (source_path, snapshot['name'])
        url = 'storage/snapshots/%s/clone' % urllib.parse.quote_plus(
            snapshot_path)
        self.nef.post(url, {'targetPath': target_path})
        if volume['size'] > snapshot['volume_size']:
            self.extend_volume(volume, volume['size'])

    def create_cloned_volume(self, volume, src_vref):
        """Creates a clone of the specified volume.

        :param volume: new volume reference
        :param src_vref: source volume reference
        """
        snapshot_name = self._get_clone_snapshot_name(volume['id'])
        snapshot = {'name': snapshot_name,
                    'volume_name': src_vref['name'],
                    'volume_id': src_vref['id'],
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

    def create_export(self, _ctx, volume, connector):
        """Export a volume."""
        pass

    def ensure_export(self, _ctx, volume):
        """Synchronously recreate an export for a volume."""
        pass

    def remove_export(self, _ctx, volume):
        """Remove an export for a volume."""
        pass

    def terminate_connection(self, volume, connector, **kwargs):
        """Terminate a connection to a volume.

        :param volume: a volume object
        :param connector: a connector object
        :returns: dictionary of connection information
        """
        info = {'driver_volume_type': 'iscsi', 'data': {}}
        host_iqn = None
        host_groups = []
        volume_path = self._get_volume_path(volume['name'])
        if isinstance(connector, dict) and 'initiator' in connector:
            connectors = []
            for attachment in volume.volume_attachment:
                connectors.append(attachment.get('connector'))
            if connectors.count(connector) > 1:
                LOG.debug('Detected multiple connections on host '
                          '%(host_name)s [%(host_ip)s] for volume '
                          '%(volume)s, skip terminate volume connection',
                          {'host_name': connector.get('host', 'unknown'),
                           'host_ip': connector.get('ip', 'unknown'),
                           'volume': volume['name']})
                return True
            host_iqn = connector.get('initiator')
            host_groups.append(options.DEFAULT_HOST_GROUP)
            host_group = self._get_host_group(host_iqn)
            if host_group is not None:
                host_groups.append(host_group)
            LOG.debug('Terminate connection for volume %(volume)s '
                      'and initiator %(initiator)s',
                      {'volume': volume['name'],
                       'initiator': host_iqn})
        else:
            LOG.debug('Terminate all connections for volume %(volume)s',
                      {'volume': volume['name']})

        params = {'volume': volume_path}
        url = 'san/lunMappings?%s' % urllib.parse.urlencode(params)
        mappings = self.nef.get(url).get('data')
        if not mappings:
            LOG.debug('There are no LUN mappings found for volume %(volume)s',
                      {'volume': volume['name']})
            return info
        for mapping in mappings:
            mapping_id = mapping.get('id')
            mapping_tg = mapping.get('targetGroup')
            mapping_hg = mapping.get('hostGroup')
            if host_iqn is None or mapping_hg in host_groups:
                LOG.debug('Delete LUN mapping %(id)s for volume %(volume)s, '
                          'target group %(tg)s and host group %(hg)s',
                          {'id': mapping_id, 'volume': volume['name'],
                           'tg': mapping_tg, 'hg': mapping_hg})
                self._delete_lun_mapping(mapping_id)
            else:
                LOG.debug('Skip LUN mapping %(id)s for volume %(volume)s, '
                          'target group %(tg)s and host group %(hg)s',
                          {'id': mapping_id, 'volume': volume['name'],
                           'tg': mapping_tg, 'hg': mapping_hg})
        return info

    def get_volume_stats(self, refresh=False):
        """Get volume stats.

        If 'refresh' is True, run update the stats first.
        """
        if refresh:
            self._update_volume_stats()

        return self._stats

    def _update_volume_stats(self):
        """Retrieve stats info for NexentaStor appliance."""
        LOG.debug('Updating volume stats')
        params = {'fields': 'bytesAvailable,bytesUsed'}
        url = 'storage/volumeGroups/%s?%s' % (
            urllib.parse.quote_plus(self.dataset),
            urllib.parse.urlencode(params))
        stats = self.nef.get(url)
        free = utils.str2gib_size(stats['bytesAvailable'])
        allocated = utils.str2gib_size(stats['bytesUsed'])

        location_info = '%(driver)s:%(host)s:%(pool)s/%(group)s' % {
            'driver': self.__class__.__name__,
            'host': self.iscsi_host,
            'pool': self.storage_pool,
            'group': self.volume_group,
        }
        self._stats = {
            'vendor_name': 'Nexenta',
            'dedup': self.dataset_deduplication,
            'compression': self.dataset_compression,
            'description': self.dataset_description,
            'driver_version': self.VERSION,
            'storage_protocol': 'iSCSI',
            'sparsed_volumes': self.configuration.nexenta_sparse,
            'total_capacity_gb': free + allocated,
            'free_capacity_gb': free,
            'reserved_percentage': self.configuration.reserved_percentage,
            'QoS_support': False,
            'multiattach': True,
            'consistencygroup_support': True,
            'consistent_group_snapshot_enabled': True,
            'volume_backend_name': self.backend_name,
            'location_info': location_info,
            'iscsi_target_portal_port': self.iscsi_target_portal_port,
            'nef_url': self.nef.url
        }

    def _get_volume_path(self, volume_name):
        """Return zfs volume name that corresponds given volume name."""
        return '%s/%s' % (self.dataset, volume_name)

    @staticmethod
    def _get_clone_snapshot_name(volume_id):
        """Return name for snapshot that will be used to clone the volume."""
        return 'cinder-clone-snapshot-%s' % volume_id

    def _get_target_group_name(self, target_name):
        """Return Nexenta iSCSI target group name for volume."""
        return target_name.replace(
            self.configuration.nexenta_target_prefix,
            self.configuration.nexenta_target_group_prefix
        )

    def _get_target_name(self, target_group_name):
        """Return Nexenta iSCSI target name for volume."""
        return target_group_name.replace(
            self.configuration.nexenta_target_group_prefix,
            self.configuration.nexenta_target_prefix
        )

    def _get_host_addresses(self):
        """Return Nexenta IP addresses list."""
        host_addresses = []
        url = 'network/addresses'
        data = self.nef.get(url).get('data')
        for item in data:
            ip_cidr = six.text_type(item['address'])
            ip_addr, ip_mask = ip_cidr.split('/')
            ip_obj = ipaddress.ip_address(ip_addr)
            if not ip_obj.is_loopback:
                host_addresses.append(ip_obj.exploded)
        LOG.debug('Configured IP addresses: %(addresses)s',
                  {'addresses': host_addresses})
        return host_addresses

    def _get_host_portals(self):
        """Return configured iSCSI portals list."""
        host_portals = []
        host_addresses = self._get_host_addresses()
        portal_host = self.iscsi_host
        if portal_host:
            if portal_host in host_addresses:
                if self.portal_port:
                    portal_port = int(self.portal_port)
                else:
                    portal_port = options.DEFAULT_ISCSI_PORT
                host_portal = '%s:%s' % (portal_host, portal_port)
                host_portals.append(host_portal)
            else:
                LOG.debug('Skip not a local portal IP address %(portal)s',
                          {'portal': portal_host})
        else:
            LOG.debug('Configuration parameter nexenta_host is not defined')
        for portal in self.portals.split(','):
            if not portal:
                continue
            host_port = portal.split(':')
            portal_host = host_port[0]
            if portal_host in host_addresses:
                if len(host_port) == 2:
                    portal_port = int(host_port[1])
                else:
                    portal_port = options.DEFAULT_ISCSI_PORT
                host_portal = '%s:%s' % (portal_host, portal_port)
                if host_portal not in host_portals:
                    host_portals.append(host_portal)
            else:
                LOG.debug('Skip not a local portal IP address %(portal)s',
                          {'portal': portal_host})
        LOG.debug('Configured iSCSI portals: %(portals)s',
                  {'portals': host_portals})
        return host_portals

    def _target_group_props(self, group_name, host_portals):
        """Check and update an existing targets/portals for given target group.

        :param group_name: target group name
        :param host_portals: configured host portals list
        :returns: dictionary of portals per target
        """
        if not group_name.startswith(self.target_group_prefix):
            LOG.debug('Skip not a cinder target group %(group)s',
                      {'group': group_name})
            return {}
        group_props = {}
        params = {'name': group_name}
        url = 'san/targetgroups?%s' % urllib.parse.urlencode(params)
        data = self.nef.get(url).get('data')
        if not data:
            LOG.debug('Skip target group %(group)s: group not found',
                      {'group': group_name})
            return {}
        target_names = data[0]['members']
        if not target_names:
            target_name = self._get_target_name(group_name)
            self._create_target(target_name, host_portals)
            self._update_target_group(group_name, [target_name])
            group_props[target_name] = host_portals
            return group_props
        for target_name in target_names:
            group_props[target_name] = []
            params = {'name': target_name}
            url = 'san/iscsi/targets?%s' % urllib.parse.urlencode(params)
            data = self.nef.get(url).get('data')
            if not data:
                LOG.debug('Skip target group %(group)s: '
                          'group member %(target)s not found',
                          {'group': group_name, 'target': target_name})
                return {}
            target_portals = data[0]['portals']
            if not target_portals:
                LOG.debug('Skip target group %(group)s: '
                          'group member %(target)s has no portals',
                          {'group': group_name, 'target': target_name})
                return {}
            for item in target_portals:
                target_portal = '%s:%s' % (item['address'], item['port'])
                if target_portal not in host_portals:
                    LOG.debug('Skip target group %(group)s: '
                              'group member %(target)s bind to a '
                              'non local portal address %(portal)s',
                              {'group': group_name,
                               'target': target_name,
                               'portal': target_portal})
                    return {}
                group_props[target_name].append(target_portal)
        return group_props

    def initialize_connection(self, volume, connector):
        """Do all steps to get zfs volume exported at separate target.

        :param volume: volume reference
        :param connector: connector reference
        :returns: dictionary of connection information
        """
        volume_path = self._get_volume_path(volume['name'])
        host_iqn = connector.get('initiator')
        LOG.debug('Initialize connection for volume: %(volume)s '
                  'and initiator: %(initiator)s',
                  {'volume': volume_path, 'initiator': host_iqn})

        host_groups = [options.DEFAULT_HOST_GROUP]
        host_group = self._get_host_group(host_iqn)
        if host_group:
            host_groups.append(host_group)

        host_portals = self._get_host_portals()
        props_portals = []
        props_iqns = []
        props_luns = []
        params = {'volume': volume_path}
        url = 'san/lunMappings?%s' % urllib.parse.urlencode(params)
        mappings = self.nef.get(url).get('data')
        for mapping in mappings:
            mapping_id = mapping['id']
            mapping_lu = mapping['lun']
            mapping_hg = mapping['hostGroup']
            mapping_tg = mapping['targetGroup']
            if mapping_tg == options.DEFAULT_TARGET_GROUP:
                LOG.debug('Delete LUN mapping %(id)s for target group %(tg)s',
                          {'id': mapping_id, 'tg': mapping_tg})
                self._delete_lun_mapping(mapping_id)
                continue
            if mapping_hg not in host_groups:
                LOG.debug('Skip LUN mapping %(id)s for host group %(hg)s',
                          {'id': mapping_id, 'hg': mapping_hg})
                continue
            group_props = self._target_group_props(mapping_tg, host_portals)
            if not group_props:
                LOG.debug('Skip LUN mapping %(id)s for target group %(tg)s',
                          {'id': mapping_id, 'tg': mapping_tg})
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
            LOG.debug('Use existing LUN mapping(s) %(props)s',
                      {'props': props})
            return {'driver_volume_type': 'iscsi', 'data': props}

        if host_group is None:
            host_group = '%s-%s' % (self.host_group_prefix, uuid.uuid4().hex)
            self._create_host_group(host_group, host_iqn)

        mappings_spread = {}
        targets_spread = {}
        url = 'san/targetgroups'
        data = self.nef.get(url).get('data')
        for item in data:
            target_group = item['name']
            group_props = self._target_group_props(target_group, host_portals)
            members = len(group_props)
            if members == 0:
                LOG.debug('Skip unsuitable target group %(tg)s',
                          {'tg': target_group})
                continue
            params = {'targetGroup': target_group}
            url = 'san/lunMappings?%s' % urllib.parse.urlencode(params)
            data = self.nef.get(url).get('data')
            mappings = len(data)
            if not mappings < self.luns_per_target:
                LOG.debug('Skip target group %(tg)s: '
                          'group members limit reached: %(limit)s',
                          {'tg': target_group, 'limit': mappings})
                continue
            targets_spread[target_group] = group_props
            mappings_spread[target_group] = mappings
            LOG.debug('Found target group %(tg)s with %(members)s '
                      'members and %(mappings)s LUNs',
                      {'tg': target_group, 'members': members,
                       'mappings': mappings})

        if not mappings_spread:
            target = '%s-%s' % (self.target_prefix, uuid.uuid4().hex)
            target_group = self._get_target_group_name(target)
            self._create_target(target, host_portals)
            self._create_target_group(target_group, [target])
            props_portals += host_portals
            props_iqns += [target] * len(host_portals)
        else:
            target_group = min(mappings_spread, key=mappings_spread.get)
            targets = targets_spread[target_group]
            members = targets.keys()
            mappings = mappings_spread[target_group]
            LOG.debug('Using existing target group %(tg)s '
                      'with members %(members)s and %(mappings)s LUNs',
                      {'tg': target_group, 'members': members,
                       'mappings': mappings})
            for target in targets:
                portals = targets[target]
                props_portals += portals
                props_iqns += [target] * len(portals)

        url = 'san/lunMappings'
        data = {
            'volume': volume_path,
            'targetGroup': target_group,
            'hostGroup': host_group
        }
        LOG.debug('Create LUN mapping %(data)s',
                  {'data': data})
        self.nef.post(url, data)

        params = {
            'fields': 'lun',
            'volume': volume_path,
            'targetGroup': target_group,
            'hostGroup': host_group
        }
        LOG.debug('Get LUN number of LUN mapping for %(params)s',
                  {'params': params})
        url = 'san/lunMappings?%s' % urllib.parse.urlencode(params)
        data = self._poll_result(url)
        lun = data[0]['lun']
        props_luns = [lun] * len(props_iqns)

        if multipath:
            props['target_portals'] = props_portals
            props['target_iqns'] = props_iqns
            props['target_luns'] = props_luns
        else:
            index = random.randrange(0, len(props_luns))
            props['target_portal'] = props_portals[index]
            props['target_iqn'] = props_iqns[index]
            props['target_lun'] = props_luns[index]

        if not self.lu_writebackcache_disabled:
            LOG.debug('Get LUN guid for volume %(volume)s',
                      {'volume': volume_path})
            params = {'fields': 'guid', 'volume': volume_path}
            url = 'san/logicalUnits?%s' % urllib.parse.urlencode(params)
            data = self.nef.get(url).get('data')
            guid = data[0]['guid']
            LOG.debug('Enable Write Back Cache for LUN %(guid)s',
                      {'guid': guid})
            url = 'san/logicalUnits/%s' % urllib.parse.quote_plus(guid)
            data = {'writebackCacheDisabled': False}
            self.nef.put(url, data)

        LOG.debug('Created new LUN mapping(s): %(props)s',
                  {'props': props})
        return {'driver_volume_type': 'iscsi', 'data': props}

    def _create_target_group(self, name, members):
        """Create a new target group with members.

        :param name: group name
        :param members: group members list
        """
        url = 'san/targetgroups'
        data = {
            'name': name,
            'members': members
        }
        LOG.debug('Create new target group %(name)s '
                  'with members %(members)s',
                  {'name': name, 'members': members})
        self.nef.post(url, data)

    def _update_target_group(self, name, members):
        """Update a existing target group with new members.

        :param name: group name
        :param members: group members list
        """
        url = 'san/targetgroups/%s' % urllib.parse.quote_plus(name)
        data = {
            'members': members
        }
        LOG.debug('Update existing target group %(name)s '
                  'with new members %(members)s',
                  {'name': name, 'members': members})
        self.nef.put(url, data)

    def _delete_lun_mapping(self, mapping):
        """Delete an existing LUN mapping.

        :param mapping: LUN mapping ID
        """
        url = 'san/lunMappings/%s' % mapping
        LOG.debug('Delete LUN mapping %(mapping)s',
                  {'mapping': mapping})
        self.nef.delete(url)

    def _create_target(self, name, portals):
        """Create a new target with portals.

        :param name: target name
        :param portals: target portals list
        """
        url = 'san/iscsi/targets'
        data = {
            'name': name,
            'portals': self._s2d(portals)
        }
        LOG.debug('Create new target %(name)s with portals %(portals)s',
                  {'name': name, 'portals': portals})
        self.nef.post(url, data)

    def _get_host_group(self, member):
        """Find existing host group by group member.

        :param member: host group member
        :returns: host group name
        """
        url = 'san/hostgroups'
        host_groups = self.nef.get(url).get('data')
        for host_group in host_groups:
            members = host_group['members']
            if member in members:
                name = host_group['name']
                LOG.debug('Found host group %(name)s for member %(member)s',
                          {'name': name, 'member': member})
                return name
        return None

    def _create_host_group(self, name, member):
        """Create a new host group.

        :param name: host group name
        :param member: host group member
        """
        url = 'san/hostgroups'
        data = {
            'name': name,
            'members': [member]
        }
        LOG.debug('Create new host group %(name)s with member %(member)s',
                  {'name': name, 'member': member})
        self.nef.post(url, data)

    def _poll_result(self, url):
        """Poll non-empty response data for NEF get method.

        :param url: NEF URL
        :returns: response data list
        """
        for retry in range(0, options.POLL_RETRIES):
            data = self.nef.get(url).get('data')
            if data:
                return data
            delay = int(math.exp(retry))
            LOG.debug('Retry after %(delay)s seconds delay',
                      {'delay': delay})
            greenthread.sleep(delay)
        return []

    @staticmethod
    def _s2d(css):
        """Parse list of colon-separated address and port to dictionary.

        :param css: list of colon-separated address and port
        :returns: dictionary
        """
        result = []
        for key_val in css:
            key, val = key_val.split(':')
            result.append({'address': key, 'port': int(val)})
        return result

    @staticmethod
    def _d2s(kvp):
        """Parse dictionary to list of colon-separated address and port.

        :param kvp: dictionary
        :returns: list of colon-separated address and port
        """
        result = []
        for key_val in kvp:
            result.append('%s:%s' % (key_val['address'], key_val['port']))
        return result

    def _delete_snapshot(self, path, recursive=False, defer=True):
        """Deletes a snapshot.

        :param path: absolute snapshot path
        :param recursive: boolean, True to recursively destroy snapshots
                          of the same name on a child datasets
        :param defer: boolean, True to mark the snapshot for deferred
                      destroy when there are not any active references
                      to it (for example, clones)
        """
        params = {'path': path}
        url = 'storage/snapshots?%s' % urllib.parse.urlencode(params)
        data = self.nef.get(url).get('data')
        if not data:
            return
        params = {'recursive': recursive, 'defer': defer}
        url = 'storage/snapshots/%s?%s' % (
            urllib.parse.quote_plus(path),
            urllib.parse.urlencode(params))
        self.nef.delete(url)

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
        cgsnapshot_path = '%s@%s' % (self.dataset, cgsnapshot_name)
        url = 'storage/snapshots'
        data = {'path': cgsnapshot_path, 'recursive': True}
        self.nef.post(url, data)
        for snapshot in snapshots:
            volume_path = self._get_volume_path(snapshot['volume_name'])
            snapshot_path = '%s@%s' % (volume_path, cgsnapshot_name)
            url = 'storage/snapshots/%s/rename' % (
                urllib.parse.quote_plus(snapshot_path))
            data = {'newName': snapshot['name']}
            self.nef.post(url, data)
        self._delete_snapshot(cgsnapshot_path, True)
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
            cgsnapshot_path = '%s@%s' % (self.dataset, cgsnapshot_name)
            url = 'storage/snapshots'
            data = {'path': cgsnapshot_path, 'recursive': True}
            self.nef.post(url, data)
            for volume, source_vol in zip(volumes, source_vols):
                source_path = self._get_volume_path(source_vol['name'])
                snapshot_path = '%s@%s' % (source_path, cgsnapshot_name)
                snapshot_name = self._get_clone_snapshot_name(volume['id'])
                url = 'storage/snapshots/%s/rename' % (
                    urllib.parse.quote_plus(snapshot_path))
                data = {'newName': snapshot_name}
                self.nef.post(url, data)
                snapshot = {'name': snapshot_name,
                            'volume_id': source_vol['id'],
                            'volume_name': source_vol['name'],
                            'volume_size': source_vol['size']}
                self.create_volume_from_snapshot(volume, snapshot)
            self._delete_snapshot(cgsnapshot_path, True)
        return group_model_update, volumes_model_update

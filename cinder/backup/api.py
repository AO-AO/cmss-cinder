# Copyright (C) 2012 Hewlett-Packard Development Company, L.P.
# Copyright (c) 2014 TrilioData, Inc
# Copyright (c) 2015 EMC Corporation
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

"""
Handles all requests relating to the volume backups service.
"""

from eventlet import greenthread
from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import excutils
import uuid

from cinder.backup import rpcapi as backup_rpcapi
from cinder.compute import nova
from cinder import context
from cinder.db import base
from cinder import exception
from cinder.i18n import _, _LI, _LE, _LW
import cinder.policy
from cinder import quota
from cinder import utils
import cinder.volume
from cinder.volume import utils as volume_utils

CONF = cfg.CONF
LOG = logging.getLogger(__name__)
QUOTAS = quota.QUOTAS
MAGICSTR = "!@##@!"
PERIODICSTR = "%#periodic%#"


def check_policy(context, action):
    target = {
        'project_id': context.project_id,
        'user_id': context.user_id,
    }
    _action = 'backup:%s' % action
    cinder.policy.enforce(context, _action, target)


class API(base.Base):
    """API for interacting with the volume backup manager."""

    def __init__(self, db_driver=None):
        self.backup_rpcapi = backup_rpcapi.BackupAPI()
        self.volume_api = cinder.volume.API()
        super(API, self).__init__(db_driver)

    def get(self, context, backup_id):
        check_policy(context, 'get')
        rv = self.db.backup_get(context, backup_id)
        return dict(rv.iteritems())

    def delete(self, context, backup_id):
        """Make the RPC call to delete a volume backup."""
        check_policy(context, 'delete')
        backup = self.get(context, backup_id)
        if backup['status'] not in ['available', 'error']:
            msg = _('Backup status must be available or error')
            raise exception.InvalidBackup(reason=msg)

        # parent_id = backup.get("parent_id", None)
        description = backup.get("display_description", None)
        # volume_id = backup.get("volume_id", None)
        if description:
            if description[-6:] == MAGICSTR:
                LOG.error(_LE("[backup delete] should not delete "
                          "active backup, although we allow"))

            # Add 'periodic' into context for periodic backup
            if PERIODICSTR in description:
                context.set_periodic(True)

        # Don't allow backup to be deleted if there are incremental
        # backups dependent on it.
        deltas = self.get_all(context, {'parent_id': backup['id']})
        if deltas and len(deltas):
            msg = _('Incremental backups exist for this backup.')
            raise exception.InvalidBackup(reason=msg)

        self.db.backup_update(context, backup_id, {'status': 'deleting'})
        self.backup_rpcapi.delete_backup(context,
                                         backup['host'],
                                         backup['id'])

    def get_all(self, context, search_opts=None):
        if search_opts is None:
            search_opts = {}
        check_policy(context, 'get_all')
        if context.is_admin:
            backups = self.db.backup_get_all(context, filters=search_opts)
        else:
            backups = self.db.backup_get_all_by_project(context,
                                                        context.project_id,
                                                        filters=search_opts)

        return backups

    def _is_backup_service_enabled(self, volume, volume_host):
        """Check if there is a backup service available."""
        topic = CONF.backup_topic
        ctxt = context.get_admin_context()
        services = self.db.service_get_all_by_topic(ctxt,
                                                    topic,
                                                    disabled=False)
        for srv in services:
            if (srv['availability_zone'] == volume['availability_zone'] and
                    srv['host'] == volume_host and
                    utils.service_is_up(srv)):
                return True
        return False

    def _list_backup_services(self):
        """List all enabled backup services.

        :returns: list -- hosts for services that are enabled for backup.
        """
        topic = CONF.backup_topic
        ctxt = context.get_admin_context()
        services = self.db.service_get_all_by_topic(ctxt, topic)
        return [srv['host'] for srv in services if not srv['disabled']]

    def create(self, context, name, description, volume_id,
               container, incremental=False, availability_zone=None):
        """Make the RPC call to create a volume backup."""
        check_policy(context, 'create')
        volume = self.volume_api.get(context, volume_id)

        if volume['status'] != "available":
            msg = _('Volume to be backed up must be available')
            raise exception.InvalidVolume(reason=msg)

        volume_host = volume_utils.extract_host(volume['host'], 'host')
        if not self._is_backup_service_enabled(volume, volume_host):
            raise exception.ServiceNotFound(service_id='cinder-backup')

        # do quota reserver before setting volume status and backup status
        try:
            reserve_opts = {'backups': 1,
                            'backup_gigabytes': volume['size']}
            reservations = QUOTAS.reserve(context, **reserve_opts)
        except exception.OverQuota as e:
            overs = e.kwargs['overs']
            usages = e.kwargs['usages']
            quotas = e.kwargs['quotas']

            def _consumed(resource_name):
                return (usages[resource_name]['reserved'] +
                        usages[resource_name]['in_use'])

            for over in overs:
                if 'gigabytes' in over:
                    msg = _LW("Quota exceeded for %(s_pid)s, tried to create "
                              "%(s_size)sG backup (%(d_consumed)dG of "
                              "%(d_quota)dG already consumed)")
                    LOG.warning(msg, {'s_pid': context.project_id,
                                      's_size': volume['size'],
                                      'd_consumed': _consumed(over),
                                      'd_quota': quotas[over]})
                    raise exception.VolumeBackupSizeExceedsAvailableQuota(
                        requested=volume['size'],
                        consumed=_consumed('backup_gigabytes'),
                        quota=quotas['backup_gigabytes'])
                elif 'backups' in over:
                    msg = _LW("Quota exceeded for %(s_pid)s, tried to create "
                              "backups (%(d_consumed)d backups "
                              "already consumed)")

                    LOG.warning(msg, {'s_pid': context.project_id,
                                      'd_consumed': _consumed(over)})
                    raise exception.BackupLimitExceeded(
                        allowed=quotas[over])

        # Find the latest backup of the volume and use it as the parent
        # backup to do an incremental backup.
        latest_backup = None
        # Added for periodic backup
        if getattr(context, 'periodic', False):
            latest_backup = None
            if description:
                description = PERIODICSTR + description
            else:
                description = PERIODICSTR
        else:
            if incremental:
                backups = self.db.backup_get_all_by_volume(context.elevated(),
                                                           volume_id)
                if backups:
                    normal_backups = []
                    for bk in backups:
                        if not bk.display_description or \
                                PERIODICSTR not in bk.display_description:
                            LOG.debug("Found normal backup %(bak)s "
                                      "for volume %(vol)s." %
                                      {"bak": bk.id, "vol": volume_id})
                            normal_backups.append(bk)
                    if normal_backups:
                        LOG.debug("The normal backups for volume "
                                  "%(vol)s: %(baks)s." %
                                  {"vol": volume_id,
                                   "baks": [bk.id for bk in normal_backups]})
                        latest_backup = max(normal_backups,
                                            key=lambda x: x['created_at'])
        parent_id = None
        if latest_backup:
            if latest_backup['status'] == "available":
                parent_id = latest_backup['id']
                LOG.info(_LI("Found parent backup %(bak)s for volume "
                             "%(volume)s. Do an incremental backup."),
                         {'bak': latest_backup['id'],
                          'volume': volume['id']})
            elif latest_backup['status'] == "creating":
                msg = _('The parent backup is creating.')
                LOG.info(_LI("The parent backup %(bak)s is creating."),
                         {'bak': latest_backup['id']})
                raise exception.InvalidBackup(reason=msg)
            else:
                LOG.info(_LI("No backups available to do an incremental "
                         "backup, do a full backup for volume %(volume)s."),
                         {'volume': volume['id']})
        else:
            LOG.info(_LI("No backups available to do an incremental "
                     "backup, do a full backup for volume %(volume)s."),
                     {'volume': volume['id']})

        self.db.volume_update(context, volume_id, {'status': 'backing-up'})

        options = {'user_id': context.user_id,
                   'project_id': context.project_id,
                   'display_name': name,
                   'display_description': description,
                   'volume_id': volume_id,
                   'status': 'creating',
                   'container': container,
                   'parent_id': parent_id,
                   # Set backup size to "0" which means
                   # it's not available. Backup driver
                   # will return the exact size when
                   # backing up is done. We lined up with OP
                   # that when backup is in "creating" status,
                   # OP will show "--" in the "size" field
                   # instead of "0".
                   # 'size': volume['size'],
                   'size': 0,
                   'host': volume_host, }
        try:
            backup = self.db.backup_create(context, options)
            QUOTAS.commit(context, reservations)
        except Exception:
            with excutils.save_and_reraise_exception():
                try:
                    self.db.backup_destroy(context, backup['id'])
                finally:
                    QUOTAS.rollback(context, reservations)

        # TODO(DuncanT): In future, when we have a generic local attach,
        #                this can go via the scheduler, which enables
        #                better load balancing and isolation of services
        self.backup_rpcapi.create_backup(context,
                                         backup['host'],
                                         backup['id'],
                                         volume_id)

        return backup

    def restore(self, context, backup_id, volume_id=None):
        """Make the RPC call to restore a volume backup."""
        check_policy(context, 'restore')
        backup = self.get(context, backup_id)
        if backup['status'] != 'available':
            msg = _('Backup status must be available')
            raise exception.InvalidBackup(reason=msg)

        size = backup['size']
        if size is None:
            msg = _('Backup to be restored has invalid size')
            raise exception.InvalidBackup(reason=msg)

        # Create a volume if none specified. If a volume is specified check
        # it is large enough for the backup
        if volume_id is None:
            name = 'restore_backup_%s' % backup_id
            description = 'auto-created_from_restore_from_backup'

            LOG.info(_LI("Creating volume of %(size)s GB for restore of "
                         "backup %(backup_id)s"),
                     {'size': size, 'backup_id': backup_id},
                     context=context)
            volume = self.volume_api.create(context, size, name, description)
            volume_id = volume['id']

            while True:
                volume = self.volume_api.get(context, volume_id)
                if volume['status'] != 'creating':
                    break
                greenthread.sleep(1)
        else:
            volume = self.volume_api.get(context, volume_id)

        if volume['status'] not in ["available", "in-use"]:
            msg = (_('Volume to be backed up must be available '
                     'or in-use, but the current status is "%s".')
                   % volume['status'])
            raise exception.InvalidVolume(reason=msg)
        elif volume['status'] in ["in-use"]:
            for attachment in volume['volume_attachment']:
                instance_uuid = attachment['instance_uuid']
                instance = nova.API().get_server(context, instance_uuid)
                if instance.status not in ['SHUTOFF']:
                    msg = (_('Volume to be backed up can be in-use, but the '
                             'attached vm should in poweroff status, now vm '
                             'status is "%s".') % instance.status)
                    raise exception.InvalidVolume(reason=msg)

        # record volume status in the display_description
        self.db.volume_update(context, volume_id, {'display_description':
                                                   volume['status']})

        LOG.debug('Checking backup size %(bs)s against volume size %(vs)s',
                  {'bs': size, 'vs': volume['size']})
        # backup size is in GB
        if size > volume['size'] * 1024:
            msg = (_('volume size %(volume_size)d GB is too small to restore '
                     'backup of size %(size)d MB.') %
                   {'volume_size': volume['size'], 'size': size})
            raise exception.InvalidVolume(reason=msg)

        LOG.info(_LI("Overwriting volume %(volume_id)s with restore of "
                     "backup %(backup_id)s"),
                 {'volume_id': volume_id, 'backup_id': backup_id},
                 context=context)

        # Setting the status here rather than setting at start and unrolling
        # for each error condition, it should be a very small window
        self.db.backup_update(context, backup_id, {'status': 'restoring'})
        self.db.volume_update(context, volume_id, {'status':
                                                   'restoring-backup'})

        volume_host = volume_utils.extract_host(volume['host'], 'host')
        self.backup_rpcapi.restore_backup(context,
                                          volume_host,
                                          backup['id'],
                                          volume_id)

        d = {'backup_id': backup_id,
             'volume_id': volume_id, }

        return d

    def reset_status(self, context, backup_id, status):
        """Make the RPC call to reset a volume backup's status.

        Call backup manager to execute backup status reset operation.
        :param context: running context
        :param backup_id: which backup's status to be reset
        :parma status: backup's status to be reset
        :raises: InvalidBackup
        """
        # get backup info
        backup = self.get(context, backup_id)
        # send to manager to do reset operation
        self.backup_rpcapi.reset_status(ctxt=context, host=backup['host'],
                                        backup_id=backup_id, status=status)

    def export_record(self, context, backup_id):
        """Make the RPC call to export a volume backup.

        Call backup manager to execute backup export.

        :param context: running context
        :param backup_id: backup id to export
        :returns: dictionary -- a description of how to import the backup
        :returns: contains 'backup_url' and 'backup_service'
        :raises: InvalidBackup
        """
        check_policy(context, 'backup-export')
        backup = self.get(context, backup_id)
        if backup['status'] != 'available':
            msg = (_('Backup status must be available and not %s.') %
                   backup['status'])
            raise exception.InvalidBackup(reason=msg)

        LOG.debug("Calling RPCAPI with context: "
                  "%(ctx)s, host: %(host)s, backup: %(id)s.",
                  {'ctx': context,
                   'host': backup['host'],
                   'id': backup['id']})
        export_data = self.backup_rpcapi.export_record(context,
                                                       backup['host'],
                                                       backup['id'])

        return export_data

    def import_record(self, context, backup_service, backup_url):
        """Make the RPC call to import a volume backup.

        :param context: running context
        :param backup_service: backup service name
        :param backup_url: backup description to be used by the backup driver
        :raises: InvalidBackup
        :raises: ServiceNotFound
        """
        check_policy(context, 'backup-import')

        # NOTE(ronenkat): since we don't have a backup-scheduler
        # we need to find a host that support the backup service
        # that was used to create the backup.
        # We  send it to the first backup service host, and the backup manager
        # on that host will forward it to other hosts on the hosts list if it
        # cannot support correct service itself.
        hosts = self._list_backup_services()
        if len(hosts) == 0:
            raise exception.ServiceNotFound(service_id=backup_service)

        options = {'user_id': context.user_id,
                   'project_id': context.project_id,
                   'volume_id': '0000-0000-0000-0000',
                   'status': 'creating', }
        backup = self.db.backup_create(context, options)
        first_host = hosts.pop()
        self.backup_rpcapi.import_record(context,
                                         first_host,
                                         backup['id'],
                                         backup_service,
                                         backup_url,
                                         hosts)

        return backup

    def create_instance_backup(self, context, instance_uuid, name, description,
                               volume_ids, container, incremental=False,
                               availability_zone=None, force=True):
        """Make the RPC call to create backup for volume-based instance."""
        # Use the same policy as backup creatation
        check_policy(context, 'create')

        server = nova.API().get_server(context, instance_uuid)
        if server.status not in ["ACTIVE", "SHUTOFF", "PAUSED", "SUSPENDED",
                                 "SHELVED_OFFLOADED"]:
            msg = (_("Instance %(instance_uuid)s in %(status)s status "
                     "which is not allowed to be backed up.") %
                   {'instance_uuid': instance_uuid,
                    'status': server.status})
            raise exception.InvalidInstanceStatus(reason=msg)

        volumes = [self.volume_api.get(context, volume_id)
                   for volume_id in volume_ids]

        for volume in volumes:
            # Verify all volumes are in 'in-use' state
            if volume['status'] != "in-use":
                msg = (_('Volume to be backed up must be in-use '
                         'but the current status is "%s".')
                       % volume['status'])
                raise exception.InvalidVolume(reason=msg)

            # Verify backup service is enabled on host
            volume_host = volume_utils.extract_host(volume['host'], 'host')
            if not self._is_backup_service_enabled(volume, volume_host):
                raise exception.ServiceNotFound(service_id='cinder-backup')

        backups = []
        inst_backup_kwargs = []

        # Add a 32-bit UUID prefix to display_description, in order to
        # distinguish which backups are created at the same time
        desc_prefix = str(uuid.uuid4()).replace('-', '')

        for volume in volumes:
            # Reserve a quota before setting volume status and backup status
            try:
                reserve_opts = {'backups': 1,
                                'backup_gigabytes': volume['size']}
                LOG.info(_LI("create_instance_backup "
                             "reserve_opts: %(reserve_opts)s"),
                         {'reserve_opts': reserve_opts})
                reservations = QUOTAS.reserve(context, **reserve_opts)
            except exception.OverQuota as e:
                overs = e.kwargs['overs']
                usages = e.kwargs['usages']
                quotas = e.kwargs['quotas']

                # reset status for the other volumes and
                # remove the related backup
                for backup in backups:
                    self.db.volume_update(context, backup['volume_id'],
                                          {'status': 'in-use'})
                    self.db.backup_update(context, backup['id'],
                                          {'status': 'error'})
                    self.delete(context, backup['id'])

                def _consumed(resource_name):
                    return (usages[resource_name]['reserved'] +
                            usages[resource_name]['in_use'])

                for over in overs:
                    if 'gigabytes' in over:
                        msg = _LW("Quota exceeded for %(s_pid)s, tried to "
                                  "create " "%(s_size)sG backup "
                                  "(%(d_consumed)dG of "
                                  "%(d_quota)dG already consumed)")
                        LOG.warning(msg, {'s_pid': context.project_id,
                                          's_size': volume['size'],
                                          'd_consumed': _consumed(over),
                                          'd_quota': quotas[over]})
                        raise exception.VolumeBackupSizeExceedsAvailableQuota(
                            requested=volume['size'],
                            consumed=_consumed('backup_gigabytes'),
                            quota=quotas['backup_gigabytes'])
                    elif 'backups' in over:
                        msg = _LW("Quota exceeded for %(s_pid)s, tried to "
                                  "create backups (%(d_consumed)d backups "
                                  "already consumed)")
                        LOG.warning(msg, {'s_pid': context.project_id,
                                          'd_consumed': _consumed(over)})
                        raise exception.BackupLimitExceeded(
                            allowed=quotas[over])

            # Since Ceph doesn't use parent_id to determine an incremental
            # backup, comment this part.
            #
            # Find the latest backup of the volume and use it as the parent
            # backup to do an incremental backup.
            # latest_backup = None
            # if incremental:
            #     backups = \
            #              objects.BackupList.get_all_by_volume(context.elevated(),
            #                                                   volume['id'])
            #     if backups.objects:
            #         latest_backup = max(backups.objects,
            #                             key=lambda x: x['created_at'])
            #     else:
            #         msg = _('No backups available \
            #                  to do an incremental backup.')
            #         raise exception.InvalidBackup(reason=msg)
            latest_backup = None
            # Added for periodic backup
            if getattr(context, 'periodic', False):
                latest_backup = None
                description = PERIODICSTR + description if description \
                    else PERIODICSTR
            else:
                if incremental:
                    all_backups = self.db.\
                        backup_get_all_by_volume(context.elevated(),
                                                 volume['id'])
                    if all_backups:
                        normal_backups = []
                        for bk in all_backups:
                            if not bk.display_description or \
                                    PERIODICSTR not in bk.display_description:
                                normal_backups.append(bk)
                        if normal_backups:
                            latest_backup = max(normal_backups,
                                                key=lambda x: x['created_at'])

            parent_id = None
            if latest_backup:
                if latest_backup['status'] == "available":
                    parent_id = latest_backup['id']
                    LOG.info(_LI("Found parent backup %(bak)s for volume "
                                 "%(volume)s. Do an incremental backup."),
                             {'bak': latest_backup['id'],
                              'volume': volume['id']})
                elif latest_backup['status'] == "creating":
                    msg = _('The parent backup is creating.')
                    LOG.info(_LI("The parent backup %(bak)s is creating."),
                             {'bak': latest_backup['id']})
                    raise exception.InvalidBackup(reason=msg)
                else:
                    LOG.info(_LI("No backups available to do an incremental "
                                 "backup, do a full backup for "
                                 "volume %(volume)s."),
                             {'volume': volume['id']})
            else:
                LOG.info(_LI("No backups available to do an incremental "
                         "backup, do a full backup for volume %(volume)s."),
                         {'volume': volume['id']})

            options = {'user_id': context.user_id,
                       'project_id': context.project_id,
                       'display_name': name,
                       'display_description': (lambda x: desc_prefix + x if x
                                               else desc_prefix)(description),
                       'volume_id': volume['id'],
                       'status': 'creating',
                       'container': container,
                       'parent_id': parent_id,
                       # Set backup size to "0" which means
                       # it's not available. Backup driver
                       # will return the exact size when
                       # backing up is done. We lined up with OP
                       # that when backup is in "creating" status,
                       # OP will show "--" in the "size" field
                       # instead of "0".
                       # 'size': volume['size'],
                       'size': 0,
                       'host': volume_host, }

            # (maqi) Use volume display_description field to save volume
            # previous_status since volumes in Kilo don't have
            # previous_status field in database
            previous_status = volume['status']
            self.db.volume_update(context, volume['id'],
                                  {'status': 'backing-up',
                                   'display_description': previous_status})

            try:
                backup = self.db.backup_create(context, options)
                QUOTAS.commit(context, reservations)
            except Exception:
                with excutils.save_and_reraise_exception():
                    try:
                        self.db.backup_destroy(context, backup['id'])
                    finally:
                        QUOTAS.rollback(context, reservations)
            backups.append(backup)
            kwargs = {
                'host': backup['host'],
                'backup_id': backup['id'],
                'volume_id': volume['id'],
            }
            inst_backup_kwargs.append(kwargs)

        self.backup_rpcapi.create_instance_backup(context,
                                                  instance_uuid,
                                                  inst_backup_kwargs)
        LOG.debug("I am ready to return from create_instance_backup"
                  "with result: %(backups)s",
                  {'backups': backups})
        return backups

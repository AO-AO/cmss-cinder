# Copyright (C) 2012 Hewlett-Packard Development Company, L.P.
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
Backup manager manages volume backups.

Volume Backups are full copies of persistent volumes stored in a backup
store e.g. an object store or any other backup store if and when support is
added. They are usable without the original object being available. A
volume backup can be restored to the original volume it was created from or
any other available volume with a minimum size of the original volume.
Volume backups can be created, restored, deleted and listed.

**Related Flags**

:backup_topic:  What :mod:`rpc` topic to listen to (default:
                        `cinder-backup`).
:backup_manager:  The module name of a class derived from
                          :class:`manager.Manager` (default:
                          :class:`cinder.backup.manager.Manager`).

"""

import eventlet
from eventlet import greenthread
import time

from oslo_config import cfg
from oslo_log import log as logging
import oslo_messaging as messaging
from oslo_utils import excutils
from oslo_utils import importutils
import six

from cinder.backup import driver
from cinder.backup import rpcapi as backup_rpcapi
from cinder.compute import nova
from cinder import context
from cinder import exception
from cinder.i18n import _, _LE, _LI, _LW
from cinder import manager
from cinder import quota
from cinder import rpc
from cinder import utils

from cinder.volume import utils as volume_utils

LOG = logging.getLogger(__name__)

backup_manager_opts = [
    cfg.StrOpt('backup_driver',
               default='cinder.backup.drivers.swift',
               help='Driver to use for backups.',
               deprecated_name='backup_service'),
]

# This map doesn't need to be extended in the future since it's only
# for old backup services
mapper = {'cinder.backup.services.swift': 'cinder.backup.drivers.swift',
          'cinder.backup.services.ceph': 'cinder.backup.drivers.ceph'}

CONF = cfg.CONF
CONF.register_opts(backup_manager_opts)
QUOTAS = quota.QUOTAS
ATTACH_TIMEOUT = 360
DETACH_TIMEOUT = 1440
RETRY = 3

MOST_BACKUP_RETRIES = 720
MAGICSTR = "!@##@!"

FUJITSU_CLONE_START = "[]_S_"
FUJITSU_CLONE_END = "[]_F_"
FUJITSI_VOLUME_TYPE_NAME = "fujitsu-ipsan"
EBS_VOLUME_TYPE_NAME = "ebs-data"


class BackupManager(manager.SchedulerDependentManager):
    """Manages backup of block storage devices."""

    RPC_API_VERSION = '1.0'

    target = messaging.Target(version=RPC_API_VERSION)

    def __init__(self, service_name=None, *args, **kwargs):
        self.service = importutils.import_module(self.driver_name)
        self.az = CONF.storage_availability_zone
        self.volume_managers = {}
        self._setup_volume_drivers()
        self.backup_rpcapi = backup_rpcapi.BackupAPI()
        super(BackupManager, self).__init__(service_name='backup',
                                            *args, **kwargs)

    @property
    def driver_name(self):
        """This function maps old backup services to backup drivers."""

        return self._map_service_to_driver(CONF.backup_driver)

    def _map_service_to_driver(self, service):
        """Maps services to drivers."""

        if service in mapper:
            return mapper[service]
        return service

    @property
    def driver(self):
        return self._get_driver()

    def _get_volume_backend(self, host=None, allow_null_host=False):
        if host is None:
            if not allow_null_host:
                msg = _("NULL host not allowed for volume backend lookup.")
                raise exception.BackupFailedToGetVolumeBackend(msg)
        else:
            LOG.debug("Checking hostname '%s' for backend info.", host)
            part = host.partition('@')
            if (part[1] == '@') and (part[2] != ''):
                backend = part[2]
                LOG.debug("Got backend '%s'.", backend)
                return backend

        LOG.info(_LI("Backend not found in hostname (%s) so using default."),
                 host)

        if 'default' not in self.volume_managers:
            # For multi-backend we just pick the top of the list.
            return self.volume_managers.keys()[0]

        return 'default'

    def _get_manager(self, backend):
        LOG.debug("Manager requested for volume_backend '%s'.",
                  backend)
        if backend is None:
            LOG.debug("Fetching default backend.")
            backend = self._get_volume_backend(allow_null_host=True)
        if backend not in self.volume_managers:
            msg = (_("Volume manager for backend '%s' does not exist.") %
                   (backend))
            raise exception.BackupFailedToGetVolumeBackend(msg)
        return self.volume_managers[backend]

    def _get_driver(self, backend=None):
        LOG.debug("Driver requested for volume_backend '%s'.",
                  backend)
        if backend is None:
            LOG.debug("Fetching default backend.")
            backend = self._get_volume_backend(allow_null_host=True)
        mgr = self._get_manager(backend)
        mgr.driver.db = self.db
        return mgr.driver

    def _setup_volume_drivers(self):
        if CONF.enabled_backends:
            for backend in CONF.enabled_backends:
                host = "%s@%s" % (CONF.host, backend)
                mgr = importutils.import_object(CONF.volume_manager,
                                                host=host,
                                                service_name=backend)
                config = mgr.configuration
                backend_name = config.safe_get('volume_backend_name')
                LOG.debug("Registering backend %(backend)s (host=%(host)s "
                          "backend_name=%(backend_name)s).",
                          {'backend': backend, 'host': host,
                           'backend_name': backend_name})
                self.volume_managers[backend] = mgr
        else:
            default = importutils.import_object(CONF.volume_manager)
            LOG.debug("Registering default backend %s.", default)
            self.volume_managers['default'] = default

    def _init_volume_driver(self, ctxt, driver):
        LOG.info(_LI("Starting volume driver %(driver_name)s (%(version)s)."),
                 {'driver_name': driver.__class__.__name__,
                  'version': driver.get_version()})
        try:
            driver.do_setup(ctxt)
            driver.check_for_setup_error()
        except Exception:
            LOG.exception(_LE("Error encountered during initialization of "
                              "driver: %(name)s."),
                          {'name': driver.__class__.__name__})
            # we don't want to continue since we failed
            # to initialize the driver correctly.
            return

        driver.set_initialized()

    def init_host(self):
        """Do any initialization that needs to be run if this is a
           standalone service.
        """
        ctxt = context.get_admin_context()

        for mgr in self.volume_managers.itervalues():
            self._init_volume_driver(ctxt, mgr.driver)

        LOG.info(_LI("Cleaning up incomplete backup operations."))
        volumes = self.db.volume_get_all_by_host(ctxt, self.host)
        for volume in volumes:
            volume_host = volume_utils.extract_host(volume['host'], 'backend')
            backend = self._get_volume_backend(host=volume_host)
            attachments = volume['volume_attachment']
            if attachments:
                if volume['status'] == 'backing-up':
                    LOG.info(_LI('Resetting volume %s to available '
                                 '(was backing-up).'), volume['id'])
                    mgr = self._get_manager(backend)
                    for attachment in attachments:
                        if (attachment['attached_host'] == self.host and
                                attachment['instance_uuid'] is None):
                            mgr.detach_volume(ctxt, volume['id'],
                                              attachment['id'])
                if volume['status'] == 'restoring-backup':
                    LOG.info(_LI('setting volume %s to error_restoring '
                                 '(was restoring-backup).'), volume['id'])
                    mgr = self._get_manager(backend)
                    for attachment in attachments:
                        if (attachment['attached_host'] == self.host and
                                attachment['instance_uuid'] is None):
                            mgr.detach_volume(ctxt, volume['id'],
                                              attachment['id'])
                    self.db.volume_update(ctxt, volume['id'],
                                          {'status': 'error_restoring'})

        # TODO(smulcahy) implement full resume of backup and restore
        # operations on restart (rather than simply resetting)
        backups = self.db.backup_get_all_by_host(ctxt, self.host)
        for backup in backups:
            if backup['status'] == 'creating':
                LOG.info(_LI('Resetting backup %s to error (was creating).'),
                         backup['id'])
                err = 'incomplete backup reset on manager restart'
                self.db.backup_update(ctxt, backup['id'], {'status': 'error',
                                                           'fail_reason': err})
            if backup['status'] == 'restoring':
                LOG.info(_LI('Resetting backup %s to '
                             'available (was restoring).'),
                         backup['id'])
                self.db.backup_update(ctxt, backup['id'],
                                      {'status': 'available'})
            if backup['status'] == 'deleting':
                LOG.info(_LI('Resuming delete on backup: %s.'), backup['id'])
                try:
                    self.delete_backup(ctxt, backup['id'])
                except Exception:
                    # Don't block startup of the backup service.
                    LOG.exception(_LE("Problem cleaning incomplete backup "
                                      "operations."))

    def create_backup(self, context, backup_id):
        """Create volume backups using configured backup service."""
        bakup = self.db.backup_get(context, backup_id)
        volume_id = bakup['volume_id']
        volume = self.db.volume_get(context, volume_id)
        LOG.info(_LI('Create backup started, backup: %(backup_id)s '
                     'volume: %(volume_id)s.'),
                 {'backup_id': backup_id, 'volume_id': volume_id})

        self._notify_about_backup_usage(context, bakup, "create.start")
        volume_host = volume_utils.extract_host(volume['host'], 'backend')
        backend = self._get_volume_backend(host=volume_host)

        self.db.backup_update(context, backup_id, {'host': self.host,
                                                   'service':
                                                   self.driver_name})

        expected_status = 'backing-up'
        actual_status = volume['status']
        if actual_status != expected_status:
            err = _('Create backup aborted, expected volume status '
                    '%(expected_status)s but got %(actual_status)s.') % {
                'expected_status': expected_status,
                'actual_status': actual_status,
            }
            self.db.backup_update(context, backup_id, {'status': 'error',
                                                       'fail_reason': err})
            raise exception.InvalidVolume(reason=err)

        expected_status = 'creating'
        actual_status = bakup['status']
        if actual_status != expected_status:
            err = _('Create backup aborted, expected backup status '
                    '%(expected_status)s but got %(actual_status)s.') % {
                'expected_status': expected_status,
                'actual_status': actual_status,
            }
            self.db.volume_update(context, volume_id, {'status': 'available'})
            self.db.backup_update(context, backup_id, {'status': 'error',
                                                       'fail_reason': err})
            raise exception.InvalidBackup(reason=err)

        try:
            # NOTE(flaper87): Verify the driver is enabled
            # before going forward. The exception will be caught,
            # the volume status will be set back to available and
            # the backup status to 'error'
            utils.require_driver_initialized(self._get_driver(backend))

            backup_service = self.service.get_backup_driver(context)
            # (maqi) Need to get the backup size and container according to
            # driver's behavior. For EBS, backup_size is in MB,
            # container name is 'sheepdog'.
            backup_result = self._get_driver(backend).\
                backup_volume(context, bakup, backup_service) or {}
            LOG.debug("The size of backup %(id)s is %(size)s MB.",
                      {"id": backup_id,
                       "size": backup_result.get('size')})
        except Exception as err:
            with excutils.save_and_reraise_exception():
                self.db.volume_update(context, volume_id,
                                      {'status': 'available'})
                self.db.backup_update(context, backup_id,
                                      {'status': 'error',
                                       'fail_reason': six.text_type(err)})

        self.db.volume_update(context, volume_id, {'status': 'available'})
        backup = self.db.backup_update(context, backup_id,
                                       {'status': 'available',
                                        'size': backup_result.get('size'),
                                        'availability_zone': self.az,
                                        # 'container' is used in deleting
                                        'container':
                                        backup_result.get('container',
                                                          bakup['container'])})
        LOG.info(_LI('Create backup finished. backup: %s.'), backup_id)
        self._notify_about_backup_usage(context, backup, "create.end")

    def restore_backup(self, context, backup_id, volume_id):
        """Restore volume backups from configured backup service."""
        LOG.info(_LI('Restore backup started, backup: %(backup_id)s '
                     'volume: %(volume_id)s.'),
                 {'backup_id': backup_id, 'volume_id': volume_id})

        backup = self.db.backup_get(context, backup_id)
        volume = self.db.volume_get(context, volume_id)

        volume_host = volume_utils.extract_host(volume['host'], 'backend')
        backend = self._get_volume_backend(host=volume_host)
        self._notify_about_backup_usage(context, backup, "restore.start")

        self.db.backup_update(context, backup_id, {'host': self.host})

        expected_status = 'restoring-backup'
        actual_status = volume['status']
        if actual_status != expected_status:
            err = (_('Restore backup aborted, expected volume status '
                     '%(expected_status)s but got %(actual_status)s.') %
                   {'expected_status': expected_status,
                    'actual_status': actual_status})
            self.db.backup_update(context, backup_id, {'status': 'available'})
            # volume state not correct, openstack leave it with status,
            # but we set it to error_restoring, let op easier to
            # determine whether success or failed
            self.db.volume_update(context, volume_id, {'status': 'error'})
            raise exception.InvalidVolume(reason=err)

        expected_status = 'restoring'
        actual_status = backup['status']
        if actual_status != expected_status:
            err = (_('Restore backup aborted: expected backup status '
                     '%(expected_status)s but got %(actual_status)s.') %
                   {'expected_status': expected_status,
                    'actual_status': actual_status})
            self.db.backup_update(context, backup_id, {'status': 'error',
                                                       'fail_reason': err})
            self.db.volume_update(context, volume_id,
                                  {'status': 'error_restoring'})
            raise exception.InvalidBackup(reason=err)

        # volume size is in GB and backup size is in MB
        if volume['size'] * 1024 > backup['size']:
            LOG.info(_LI('Volume: %(vol_id)s, size: %(vol_size)d is '
                         'larger than backup: %(backup_id)s, '
                         'size: %(backup_size)d, continuing with restore.'),
                     {'vol_id': volume['id'],
                      'vol_size': volume['size'] * 1024,
                      'backup_id': backup['id'],
                      'backup_size': backup['size']})

        backup_service = self._map_service_to_driver(backup['service'])
        configured_service = self.driver_name
        if backup_service != configured_service:
            err = _('Restore backup aborted, the backup service currently'
                    ' configured [%(configured_service)s] is not the'
                    ' backup service that was used to create this'
                    ' backup [%(backup_service)s].') % {
                'configured_service': configured_service,
                'backup_service': backup_service,
            }
            self.db.backup_update(context, backup_id, {'status': 'available'})
            self.db.volume_update(context, volume_id,
                                  {'status': 'error_restoring'})
            raise exception.InvalidBackup(reason=err)

        '''
        #comment my code seg 1
        #[lihao] 1. we should record the volume mount point
        #[lihao] We get the first instance where volume is attached
        volume_attachments = volume.get("volume_attachment",None)
        if volume_attachments:
            volume_attachment = volume_attachments[0]
            mountpoint = volume_attachment.get("mountpoint", None)
            instance_uuid = volume_attachment.get("instance_uuid", None)
            #attachment_id = volume_attachment.get("id", None)
            flag = 1

        #[lihao] 2. start detach the volume
        if flag == 1:
            try:
                LOG.debug("[restore] 1. Start detach volume, volume_id = %s"
                          % volume_id)
                nova.API().detach_volume(context, instance_uuid, volume_id,
                                         timeout=3600)
                volume = self.db.volume_get(context, volume_id)
                volume_status = volume.get("status", None)
                #[lihao] wait for detach success, Do I need to set a timeout?
                i = 0
                while volume_status != "available" and i < DETACH_TIMEOUT:
                    greenthread.sleep(1)
                    volume = self.db.volume_get(context, volume_id)
                    volume_status = volume.get("status", None)
                    LOG.warn("[Restore] 2. Wait for detach volume %s %d(s)"
                             % (volume_id, i))
                    i += 1

                if volume_status != "available":
                    LOG.error("[restore] err1. Detach volume error"
                    "(timeout),volume_id = %s" % volume_id)
                    self.db.backup_update(context, backup_id,
                                          {'status': 'available'})
                    self.db.volume_update(context, volume_id,
                                          {'status': 'error_restoring'})
                    raise exception.BackupRestoreCannotDetach()

                #[lihao] if detach success, set the volume status to
                #"restoring-backup"
                self.db.volume_update(context, volume_id,
                                      {'status': 'restoring-backup'})
                #must reload volume
                volume = self.db.volume_get(context, volume_id)
            except Exception as e:
                LOG.error("[restore] err2. restore_backup error, "
                          "unable to detach volume %s" % e.message)
                raise
        '''

        try:
            # NOTE(flaper87): Verify the driver is enabled
            # before going forward. The exception will be caught,
            # the volume status will be set back to available and
            # the backup status to 'error'
            utils.require_driver_initialized(self._get_driver(backend))

            backup_service = self.service.get_backup_driver(context)
            self._get_driver(backend).restore_backup(context, backup,
                                                     volume,
                                                     backup_service)
        except Exception:
            with excutils.save_and_reraise_exception():
                '''
                #comment my code seg 2
                #[lihao] 3. if restore error, we should also attach volume
                if flag == 1:
                    LOG.debug("[restore] err3. restore error, start to"
                    "attach volume to VM, volume_id = %s" % volume_id)
                    self.db.volume_update(context, volume_id,
                                          {'status': 'available'})
                    nova.API().attach_volume(context, instance_uuid,
                                             volume_id, mountpoint,
                                             timeout=3600)
                    volume = self.db.volume_get(context, volume_id)
                    volume_status = volume.get("status", None)
                    #[lihao] wait for attach success
                    i = 0
                    while volume_status != "in-use" and i < ATTACH_TIMEOUT:
                        greenthread.sleep(1)
                        volume = self.db.volume_get(context, volume_id)
                        volume_status = volume.get("status", None)
                        LOG.warn("Wait for attach volume %s %d(s)"
                                 % (volume_id, i))
                        i += 1

                    if volume_status != "in-use":
                        LOG.error("restore meet error, and volume can't"
                                  "attach (attach timeout)")

                    LOG.debug("[restore] err4. restore error, but attach to"
                    " VM OK, volume_id = %s" % volume_id)
                '''

                self.db.volume_update(context, volume_id,
                                      {'status': 'error_restoring'})
                self.db.backup_update(context, backup_id,
                                      {'status': 'available'})

        '''
        #comment my code seg 3
        #[lihao] 4. attach_volume to VM
        if flag == 1:
            try:
                LOG.debug("[restore] 4. backup restore OK, start to attach"
                          " volume to VM volume_id = %s" % volume_id)
                self.db.volume_update(context, volume_id,
                                      {'status': 'available'})
                nova.API().attach_volume(context, instance_uuid,
                                         volume_id, mountpoint, timeout=3600)
                #make volume_status change otherwise the status maybe available
                greenthread.sleep(5)
                volume = self.db.volume_get(context, volume_id)
                volume_status = volume.get("status", None)
                i = 0
                j = 1
                while volume_status != "in-use" and i < ATTACH_TIMEOUT:
                    if volume_status == "available" and j <= RETRY:
                        LOG.error("[restore] err5.
                                  attach volume %s failed even retry %d"
                             %(volume_id, j))
                        nova.API().attach_volume(context,
                                                 instance_uuid,
                                         volume_id, mountpoint, timeout=3600)
                        greenthread.sleep(5)
                        i = 0
                        j += 1
                    elif volume_status == "available" and j > RETRY:
                        LOG.error("[restore] err6. attach vol %s failed,even "
                                  " retried %d times" % (volume_id, RETRY))
                        self.db.volume_update(context, volume_id,
                                      {'status': 'error_restoring'})
                        self.db.backup_update(context, backup_id,
                                      {'status': 'available'})
                        raise exception.BackupRestoreCannotAttach()

                    greenthread.sleep(1)
                    volume = self.db.volume_get(context, volume_id)
                    volume_status = volume.get("status", None)
                    LOG.warn("[restore] 5. Wait for attach volume %s %d(s)"
                             % (volume_id, i))
                    i += 1

                if volume_status != "in-use":
                    #[lihao] if we can't attach volume, how to do?
                    LOG.error("[restore] err7. restore success, "
                              "but volume can't attach (attach timeout)")
                    raise

                LOG.debug("[restore] 6. backup restore OK, attach volume"
                          " to VM OK, volume_id = %s" % volume_id)
                LOG.debug("[restore] 7. BCEC restore process is"
                          "succeed, have a good night")

            except Exception as e:
                LOG.error("[restore] err8. restore success, but attach VM meet"
                "error, (you can attach by hand) volume_id = %s, VM_id = %s, "
                "reason = %s" % (volume_id, instance_uuid, e.message))
        '''
        # lihao change -- start
        self.db.volume_update(context, volume_id,
                              {'status': volume['display_description']})
        self.db.volume_update(context, volume_id,
                              {'display_description': ""})
        backup = self.db.backup_update(context, backup_id,
                                       {'status': 'available'})

        if context.is_admin:
            backups = self.db.backup_get_all(context,
                                             filters={"volume_id": volume_id})
        else:
            backups = self.db.backup_get_all_by_project(
                context, context.project_id, filters={
                    "volume_id": volume_id})

        for bak in backups:
            description = bak.get("display_description", None)
            LOG.debug("[restore] backups for volume description = %s"
                      % description)
            if description:
                if description[-6:] == MAGICSTR:
                    LOG.debug(
                        "[restore] find active backup, its description is %s"
                        % description)
                    self.db.backup_update(context, bak['id'], {
                        'display_description': description[:-6]})

        description = backup.get("display_description", None)
        if description:
            new_description = description + MAGICSTR
            LOG.debug("[restore] new_description is %s" % new_description)
            backup = self.db.backup_update(
                context, backup_id,
                {'display_description': new_description})
        else:
            LOG.debug("[restore] new_description is %s" % MAGICSTR)
            backup = self.db.backup_update(context, backup_id,
                                           {'display_description': MAGICSTR})
        # lihao change -- finish
        LOG.info(_LI('Restore backup finished, backup %(backup_id)s restored'
                     ' to volume %(volume_id)s.'),
                 {'backup_id': backup_id, 'volume_id': volume_id})
        self._notify_about_backup_usage(context, backup, "restore.end")

    def delete_backup(self, context, backup_id):
        """Delete volume backup from configured backup service."""
        try:
            # NOTE(flaper87): Verify the driver is enabled
            # before going forward. The exception will be caught
            # and the backup status updated. Fail early since there
            # are no other status to change but backup's
            utils.require_driver_initialized(self.driver)
        except exception.DriverNotInitialized as err:
            with excutils.save_and_reraise_exception():
                self.db.backup_update(context, backup_id,
                                      {'status': 'error',
                                       'fail_reason':
                                       six.text_type(err)})

        LOG.info(_LI('Delete backup started, backup: %s.'), backup_id)
        backup = self.db.backup_get(context, backup_id)
        self._notify_about_backup_usage(context, backup, "delete.start")
        self.db.backup_update(context, backup_id, {'host': self.host})

        expected_status = 'deleting'
        actual_status = backup['status']
        if actual_status != expected_status:
            err = _('Delete_backup aborted, expected backup status '
                    '%(expected_status)s but got %(actual_status)s.') \
                % {'expected_status': expected_status,
                   'actual_status': actual_status}
            self.db.backup_update(context, backup_id,
                                  {'status': 'error', 'fail_reason': err})
            raise exception.InvalidBackup(reason=err)

        backup_service = self._map_service_to_driver(backup['service'])
        if backup_service is not None:
            configured_service = self.driver_name
            if backup_service != configured_service:
                err = _('Delete backup aborted, the backup service currently'
                        ' configured [%(configured_service)s] is not the'
                        ' backup service that was used to create this'
                        ' backup [%(backup_service)s].')\
                    % {'configured_service': configured_service,
                       'backup_service': backup_service}
                self.db.backup_update(context, backup_id,
                                      {'status': 'error'})
                raise exception.InvalidBackup(reason=err)

            try:
                backup_service = self.service.get_backup_driver(context)
                backup_service.delete(backup)
            except Exception as err:
                with excutils.save_and_reraise_exception():
                    self.db.backup_update(context, backup_id,
                                          {'status': 'error',
                                           'fail_reason':
                                           six.text_type(err)})

        # Get reservations
        try:
            context.read_deleted = "yes"
            volume = self.db.volume_get(context, backup['volume_id'])
            reserve_opts = {
                'backups': -1,
                # In ecloud, it's possible backup.size=0
                # So get quota from volume.size
                'backup_gigabytes': -volume['size'],
            }
            LOG.debug("reserve_opts in delete_backup: %s" % reserve_opts)
            reservations = QUOTAS.reserve(context,
                                          project_id=backup['project_id'],
                                          **reserve_opts)
        except Exception:
            reservations = None
            LOG.exception(_LE("Failed to update usages deleting backup"))

        context = context.elevated()
        self.db.backup_destroy(context, backup_id)

        # Commit the reservations
        if reservations:
            QUOTAS.commit(context, reservations,
                          project_id=backup['project_id'])

        LOG.info(_LI('Delete backup finished, backup %s deleted.'), backup_id)
        self._notify_about_backup_usage(context, backup, "delete.end")

    def _notify_about_backup_usage(self,
                                   context,
                                   backup,
                                   event_suffix,
                                   extra_usage_info=None):
        volume_utils.notify_about_backup_usage(
            context, backup, event_suffix,
            extra_usage_info=extra_usage_info,
            host=self.host)

    def export_record(self, context, backup_id):
        """Export all volume backup metadata details to allow clean import.

        Export backup metadata so it could be re-imported into the database
        without any prerequisite in the backup database.

        :param context: running context
        :param backup_id: backup id to export
        :returns: backup_record - a description of how to import the backup
        :returns: contains 'backup_url' - how to import the backup, and
        :returns: 'backup_service' describing the needed driver.
        :raises: InvalidBackup
        """
        LOG.info(_LI('Export record started, backup: %s.'), backup_id)

        backup = self.db.backup_get(context, backup_id)

        expected_status = 'available'
        actual_status = backup['status']
        if actual_status != expected_status:
            err = (_('Export backup aborted, expected backup status '
                     '%(expected_status)s but got %(actual_status)s.') %
                   {'expected_status': expected_status,
                    'actual_status': actual_status})
            raise exception.InvalidBackup(reason=err)

        backup_record = {}
        backup_record['backup_service'] = backup['service']
        backup_service = self._map_service_to_driver(backup['service'])
        configured_service = self.driver_name
        if backup_service != configured_service:
            err = (_('Export record aborted, the backup service currently'
                     ' configured [%(configured_service)s] is not the'
                     ' backup service that was used to create this'
                     ' backup [%(backup_service)s].') %
                   {'configured_service': configured_service,
                    'backup_service': backup_service})
            raise exception.InvalidBackup(reason=err)

        # Call driver to create backup description string
        try:
            utils.require_driver_initialized(self.driver)
            backup_service = self.service.get_backup_driver(context)
            backup_url = backup_service.export_record(backup)
            backup_record['backup_url'] = backup_url
        except Exception as err:
            msg = six.text_type(err)
            raise exception.InvalidBackup(reason=msg)

        LOG.info(_LI('Export record finished, backup %s exported.'), backup_id)
        return backup_record

    def import_record(self,
                      context,
                      backup_id,
                      backup_service,
                      backup_url,
                      backup_hosts):
        """Import all volume backup metadata details to the backup db.

        :param context: running context
        :param backup_id: The new backup id for the import
        :param backup_service: The needed backup driver for import
        :param backup_url: An identifier string to locate the backup
        :param backup_hosts: Potential hosts to execute the import
        :raises: InvalidBackup
        :raises: ServiceNotFound
        """
        LOG.info(_LI('Import record started, backup_url: %s.'), backup_url)

        # Can we import this backup?
        if (backup_service != self.driver_name):
            # No, are there additional potential backup hosts in the list?
            if len(backup_hosts) > 0:
                # try the next host on the list, maybe he can import
                first_host = backup_hosts.pop()
                self.backup_rpcapi.import_record(context,
                                                 first_host,
                                                 backup_id,
                                                 backup_service,
                                                 backup_url,
                                                 backup_hosts)
            else:
                # empty list - we are the last host on the list, fail
                err = _('Import record failed, cannot find backup '
                        'service to perform the import. Request service '
                        '%(service)s') % {'service': backup_service}
                self.db.backup_update(context, backup_id, {'status': 'error',
                                                           'fail_reason': err})
                raise exception.ServiceNotFound(service_id=backup_service)
        else:
            # Yes...
            try:
                utils.require_driver_initialized(self.driver)
                backup_service = self.service.get_backup_driver(context)
                backup_options = backup_service.import_record(backup_url)
            except Exception as err:
                msg = six.text_type(err)
                self.db.backup_update(context,
                                      backup_id,
                                      {'status': 'error',
                                       'fail_reason': msg})
                raise exception.InvalidBackup(reason=msg)

            required_import_options = ['display_name',
                                       'display_description',
                                       'container',
                                       'size',
                                       'service_metadata',
                                       'service',
                                       'object_count']

            backup_update = {}
            backup_update['status'] = 'available'
            backup_update['service'] = self.driver_name
            backup_update['availability_zone'] = self.az
            backup_update['host'] = self.host
            for entry in required_import_options:
                if entry not in backup_options:
                    msg = (_('Backup metadata received from driver for '
                             'import is missing %s.'), entry)
                    self.db.backup_update(context,
                                          backup_id,
                                          {'status': 'error',
                                           'fail_reason': msg})
                    raise exception.InvalidBackup(reason=msg)
                backup_update[entry] = backup_options[entry]
            # Update the database
            self.db.backup_update(context, backup_id, backup_update)

            # Verify backup
            try:
                if isinstance(backup_service, driver.BackupDriverWithVerify):
                    backup_service.verify(backup_id)
                else:
                    LOG.warning(_LW('Backup service %(service)s does not '
                                    'support verify. Backup id %(id)s is '
                                    'not verified. Skipping verify.'),
                                {'service': self.driver_name,
                                 'id': backup_id})
            except exception.InvalidBackup as err:
                with excutils.save_and_reraise_exception():
                    self.db.backup_update(context, backup_id,
                                          {'status': 'error',
                                           'fail_reason':
                                           six.text_type(err)})

            LOG.info(_LI('Import record id %s metadata from driver '
                         'finished.'), backup_id)

    def reset_status(self, context, backup_id, status):
        """Reset volume backup status.

        :param context: running context
        :param backup_id: The backup id for reset status operation
        :param status: The status to be set
        :raises: InvalidBackup
        :raises: BackupVerifyUnsupportedDriver
        :raises: AttributeError
        """
        LOG.info(_LI('Reset backup status started, backup_id: '
                     '%(backup_id)s, status: %(status)s.'),
                 {'backup_id': backup_id,
                  'status': status})
        try:
            # NOTE(flaper87): Verify the driver is enabled
            # before going forward. The exception will be caught
            # and the backup status updated. Fail early since there
            # are no other status to change but backup's
            utils.require_driver_initialized(self.driver)
        except exception.DriverNotInitialized:
            with excutils.save_and_reraise_exception():
                LOG.exception(_LE("Backup driver has not been initialized"))

        backup = self.db.backup_get(context, backup_id)
        backup_service = self._map_service_to_driver(backup['service'])
        LOG.info(_LI('Backup service: %s.'), backup_service)
        if backup_service is not None:
            configured_service = self.driver_name
            if backup_service != configured_service:
                err = _('Reset backup status aborted, the backup service'
                        ' currently configured [%(configured_service)s] '
                        'is not the backup service that was used to create'
                        ' this backup [%(backup_service)s].') % \
                    {'configured_service': configured_service,
                     'backup_service': backup_service}
                raise exception.InvalidBackup(reason=err)
            # Verify backup
            try:
                # check whether the backup is ok or not
                if status == 'available' and backup['status'] != 'restoring':
                    # check whether we could verify the backup is ok or not
                    if isinstance(backup_service,
                                  driver.BackupDriverWithVerify):
                        backup_service.verify(backup_id)
                        self.db.backup_update(context, backup_id,
                                              {'status': status})
                    # driver does not support verify function
                    else:
                        msg = (_('Backup service %(configured_service)s '
                                 'does not support verify. Backup id'
                                 ' %(id)s is not verified. '
                                 'Skipping verify.') %
                               {'configured_service': self.driver_name,
                                'id': backup_id})
                        raise exception.BackupVerifyUnsupportedDriver(
                            reason=msg)
                # reset status to error or from restoring to available
                else:
                    if (status == 'error' or
                        (status == 'available' and
                            backup['status'] == 'restoring')):
                        self.db.backup_update(context, backup_id,
                                              {'status': status})
            except exception.InvalidBackup:
                with excutils.save_and_reraise_exception():
                    LOG.error(_LE("Backup id %s is not invalid. "
                                  "Skipping reset."), backup_id)
            except exception.BackupVerifyUnsupportedDriver:
                with excutils.save_and_reraise_exception():
                    LOG.error(_LE('Backup service %(configured_service)s '
                                  'does not support verify. Backup id '
                                  '%(id)s is not verified. '
                                  'Skipping verify.'),
                              {'configured_service': self.driver_name,
                               'id': backup_id})
            except AttributeError:
                msg = (_('Backup service %(service)s does not support '
                         'verify. Backup id %(id)s is not verified. '
                         'Skipping reset.') %
                       {'service': self.driver_name,
                        'id': backup_id})
                LOG.error(msg)
                raise exception.BackupVerifyUnsupportedDriver(
                    reason=msg)

            # send notification to ceilometer
            notifier_info = {'id': backup_id, 'update': {'status': status}}
            notifier = rpc.get_notifier('backupStatusUpdate')
            notifier.info(context, "backups.reset_status.end",
                          notifier_info)

    def _create_backup_for_instance_backup(self, context, backup_id):
        """Create a volume backup.

        This method is used in volume-based instance backup.
        """

        backup = self.db.backup_get(context, backup_id)
        volume_id = backup['volume_id']
        volume = self.db.volume_get(context, volume_id)
        # volume's display_description attribute is used to save
        # backup's previous_status as a workaround.
        previous_status = volume['display_description']

        # Fujistu backup driver needs to know the volume status.
        # It takes a special action if the volume is in-use.
        new_ctx = context.to_dict()
        new_ctx['vol_previous_status'] = previous_status
        context = context.from_dict(new_ctx)

        LOG.info(_LI('Create backup started, backup: %(backup_id)s '
                     'volume: %(volume_id)s.'),
                 {'backup_id': backup_id, 'volume_id': volume_id})

        self._notify_about_backup_usage(context, backup, "create.start")
        volume_host = volume_utils.extract_host(volume['host'], 'backend')
        backend = self._get_volume_backend(host=volume_host)

        self.db.backup_update(context, backup_id, {'host': self.host,
                                                   'service':
                                                   self.driver_name})

        expected_status = 'backing-up'
        actual_status = volume['status']
        if actual_status != expected_status:
            err = _('Create backup aborted, expected volume status '
                    '%(expected_status)s but got %(actual_status)s.') % {
                'expected_status': expected_status,
                'actual_status': actual_status,
            }
            self.db.backup_update(context, backup_id, {'status': 'error',
                                                       'fail_reason': err})
            raise exception.InvalidVolume(reason=err)

        expected_status = 'creating'
        actual_status = backup['status']
        if actual_status != expected_status:
            err = _('Create backup aborted, expected backup status '
                    '%(expected_status)s but got %(actual_status)s.') % {
                'expected_status': expected_status,
                'actual_status': actual_status,
            }
            self.db.volume_update(context, volume_id,
                                  {'status': previous_status})
            self.db.backup_update(context, backup_id, {'status': 'error',
                                                       'fail_reason': err})
            raise exception.InvalidBackup(reason=err)

        try:
            # NOTE(flaper87): Verify the driver is enabled
            # before going forward. The exception will be caught,
            # the volume status will be set back to available and
            # the backup status to 'error'
            volume_driver = self._get_driver(backend)
            utils.require_driver_initialized(volume_driver)

            backup_service = self.service.get_backup_driver(context)
            # (maqi) Need to get backup size according to driver's behavior.
            # For EBS, backup_size is in MB.
            backup_result = self._get_driver(backend).backup_volume(
                context, backup, backup_service) or {}
            LOG.debug("The size of backup %(id)s is %(size)s MB.",
                      {"id": backup_id,
                       "size": backup_result.get('size')})
        except Exception as err:
            with excutils.save_and_reraise_exception():
                LOG.debug("Backup of volume %s failed due to %s" %
                          (volume_id, six.text_type(err)))
                self.db.volume_update(context, volume_id,
                                      {'status': previous_status})
                self.db.backup_update(context, backup_id,
                                      {'status': 'error',
                                       'fail_reason': six.text_type(err)})

        self.db.volume_update(context, volume_id, {'status': previous_status})
        container = backup_result.get('container', backup['container'])
        backup = self.db.backup_update(context, backup_id,
                                       {'status': 'available',
                                        'size': backup_result.get('size'),
                                        'availability_zone': self.az,
                                        'container': container})
        # parent_id won't be changed during backup creation.
        # It will be changed to -1 only in restore operation.
        # parent_dict = {"parent_id": '-1'}
        # for bk in self.db.backup_get_all_by_volume(context,
        #                                            volume_id,
        #                                            filters=parent_dict):
        #     if bk.id != backup_id:
        #         LOG.debug('Reset parent_id for backup %s.' % bk.id)
        #         self.db.backup_update(context, bk.id,
        #                              {'parent_id': None})

        LOG.info(_LI('Create backup finished. backup: %s.'), backup_id)
        self._notify_about_backup_usage(context, backup, "create.end")

    def create_instance_backup(self, context, instance_uuid,
                               inst_backup_kwargs):
        """Create volumes backup for volume-based instance
        using configured backup service.

        Before backing up the volume, it will freez instance
        file system. When the backup is done, it will thaw
        the file system.
        """

        previous_vm_state = nova.API().get_vm_state(context, instance_uuid)
        LOG.debug("The previous_vm_state of instance %s is %s" %
                  (instance_uuid, previous_vm_state))
        if previous_vm_state == 'active':
            try:
                LOG.info(_LI('Before backup freez instance '
                             '%(instance_uuid)s.') %
                         {'instance_uuid': instance_uuid})
                # flush cache to disk
                nova.API().exec_cmd(context, instance_uuid, "sync")
                # freeze instance file system
                nova.API().freeze_filesystem(context, instance_uuid)
            except exception.ServerNotFound:
                LOG.warn(_LW('Instance freeze fails since '
                             'instance %(instance_uuid)s is not found.') %
                         {'instance_uuid': instance_uuid})
            except exception.APITimeout:
                LOG.warn(_LW('Instance %(instance_uuid)s freeze fails due to '
                             'nova api timeout.') %
                         {'instance_uuid': instance_uuid})
            except exception.ExecCmdError:
                LOG.warn(_LW('Instance %(instance_uuid)s cache flush fails.') %
                         {'instance_uuid': instance_uuid})
            except exception.QemuGANotEnable:
                LOG.warn(_LW('Instance freeze fails since Qemu '
                             'guest agent is not enabled on instance '
                             '%(instance_uuid)s.') %
                         {'instance_uuid': instance_uuid})
            except exception.QemuGANotAvailable:
                LOG.warn(_LW('Instance freeze fails since Qemu '
                             'guest agent is not available on '
                             'instance %(instance_uuid)s.') %
                         {'instance_uuid': instance_uuid})
            except exception.QemuGARepeatFreeze:
                LOG.warn(_LW("Instance %(instance_uuid)s is already frozen.") %
                         {'instance_uuid': instance_uuid})
            except Exception as err:
                LOG.warn(_LW('Instance %(instance_uuid)s freeze fails '
                             'due to: %(err)s') %
                         {'instance_uuid': instance_uuid,
                          'err': six.text_type(err)})

        # LOG.debug("Set instance %s state to 'backing_up'." % instance_uuid)
        # nova.API().set_vm_state(context, instance_uuid, "backing_up")

        # Use greenthread to create backup for each volume
        pool = eventlet.GreenPool()
        for kwargs in inst_backup_kwargs:
            LOG.info(_LI('Start backup for id %(backup_id)s') %
                     {'backup_id': kwargs['backup_id']})
            pool.spawn_n(self._create_backup_for_instance_backup,
                         context, kwargs['backup_id'])

        def _get_backups_by_volumetype(context, backup_list, volume_type_name):
            backups = []
            for backup in backup_list:
                if self.db.volume_get(context, backup['volume_id'])[
                        'volume_type']['name'] == volume_type_name:
                    LOG.debug("Backup %s's volume_type matched."
                              % backup['id'])
                    backups.append(backup)
            return backups

        # wait for backup to be done
        for attempt in range(MOST_BACKUP_RETRIES):
            backup_list = [self.db.backup_get(context, kwargs['backup_id'])
                           for kwargs in inst_backup_kwargs]
            # this part is specific for Zhengqi Gongyouyun
            fujitsu_backup_list = _get_backups_by_volumetype(
                context, backup_list, FUJITSI_VOLUME_TYPE_NAME)
            ebs_backup_list = _get_backups_by_volumetype(context, backup_list,
                                                         EBS_VOLUME_TYPE_NAME)

            bak_status_list = [backup['status'] for backup in backup_list]
            # backup_status_list = [self.db.backup_get(context,
            #                       kwargs['backup_id'])['status']
            #                       for kwargs in inst_backup_kwargs]
            if set(bak_status_list) <= set(['available', 'error']):
                LOG.info(_LI("All backups are done, break loop(1)."))
                break
            elif set(bak_status_list) <= set(['available', 'error',
                                              'creating']):
                # when EBS backup isn't finished
                if 'creating' in [backup['status']
                                  for backup in ebs_backup_list]:
                    # if 'creating' in [backup['status'] for backup in
                    # ebs_backup_list]
                    LOG.info(_LI("Backup is on-going. "
                             "bak_status_list: % (bak_status_list)s.") %
                             {"bak_status_list": set(bak_status_list)})
                    greenthread.sleep(5)
                    # continue
                else:
                    # Currently there are only EBS and FUJISTU backups
                    # If it's not EBS, it's FUJITSU

                    # It's OK to break loop when
                    # 1. All FUJITSU backups are available or
                    # 2. backup is creating and clone session is established.
                    #
                    # backup display_description is used to save the "clone
                    # session established" flag for FUJITSU.
                    # When clone session is established, it will be like
                    # "xxxxx[]_S_4de21e60-1486-434e-a82f-5349f5f095cb";
                    # when clone is finished, it will be like
                    # "xxxxx[]_F_4de21e60-1486-434e-a82f-5349f5f095cb".
                    # When backup is done, display_description
                    # is reset to "xxxxx".
                    break_loop = True
                    for b in fujitsu_backup_list:
                        if b.status == 'creating' and \
                            (FUJITSU_CLONE_START not in
                             b.display_description and
                             FUJITSU_CLONE_END not in b.display_description):
                            break_loop = False
                            LOG.debug(
                                "FUJITSU clone session isn't established"
                                " for backup: %(backup_id)s. "
                                "description: %(description)s" %
                                {"backup_id": b.id,
                                 "description": b.display_description})
                    if break_loop:
                        LOG.info(_LI("All backups are done, break loop(2)."))
                        break
                    else:
                        LOG.info(_LI("Backup is on-going. "
                                     "bak_status_list: %(bak_status_list)s.") %
                                 {"bak_status_list": set(bak_status_list)})
                        greenthread.sleep(5)

        if attempt == MOST_BACKUP_RETRIES - 1:
            LOG.info(_LI("Backing up of %(instance_uuid)s isn't  "
                         "finished in 3600s.") %
                     {'instance_uuid': instance_uuid})
            for kwargs in inst_backup_kwargs:
                self.db.backup_update(context, kwargs['backup_id'],
                                      {'status': 'error',
                                       'fail_reason':
                                       "backing up isn't finished in 3600s"})

        # LOG.debug("Set instance %s state to %s" %
        #           (instance_uuid, previous_vm_state))
        # nova.API().set_vm_state(context, instance_uuid, previous_vm_state)

        # thaw instance file system
        if previous_vm_state == 'active':
            LOG.info(_LI('Start to thaw instance %(instance_uuid)s.') %
                     {'instance_uuid': instance_uuid})
            try:
                nova.API().thaw_filesystem(context, instance_uuid)
            except exception.APITimeout:
                LOG.info(_LI("Thaw instance API timeout. "
                             "Sleep 20s and try again."))
                time.sleep(20)
                nova.API().thaw_filesystem(context, instance_uuid)
            except (exception.ServerNotFound, exception.QemuGARepeatThaw):
                pass

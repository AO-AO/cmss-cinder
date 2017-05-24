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

"""The backups api."""

from oslo_log import log as logging
import webob
from webob import exc

from cinder.api import common
from cinder.api import extensions
from cinder.api.openstack import wsgi
from cinder.api.views import backups as backup_views
from cinder.api import xmlutil
from cinder import backup as backupAPI
from cinder import exception
from cinder.i18n import _, _LI
from cinder import utils

LOG = logging.getLogger(__name__)


def make_backup(elem):
    elem.set('id')
    elem.set('status')
    elem.set('size')
    elem.set('container')
    elem.set('parent_id')
    elem.set('volume_id')
    elem.set('object_count')
    elem.set('availability_zone')
    elem.set('created_at')
    elem.set('name')
    elem.set('description')
    elem.set('fail_reason')


def make_backup_restore(elem):
    elem.set('backup_id')
    elem.set('volume_id')


def make_backup_export_import_record(elem):
    elem.set('backup_service')
    elem.set('backup_url')


class BackupTemplate(xmlutil.TemplateBuilder):
    def construct(self):
        root = xmlutil.TemplateElement('backup', selector='backup')
        make_backup(root)
        alias = Backups.alias
        namespace = Backups.namespace
        return xmlutil.MasterTemplate(root, 1, nsmap={alias: namespace})


class BackupsTemplate(xmlutil.TemplateBuilder):
    def construct(self):
        root = xmlutil.TemplateElement('backups')
        elem = xmlutil.SubTemplateElement(root, 'backup', selector='backups')
        make_backup(elem)
        alias = Backups.alias
        namespace = Backups.namespace
        return xmlutil.MasterTemplate(root, 1, nsmap={alias: namespace})


class BackupRestoreTemplate(xmlutil.TemplateBuilder):
    def construct(self):
        root = xmlutil.TemplateElement('restore', selector='restore')
        make_backup_restore(root)
        alias = Backups.alias
        namespace = Backups.namespace
        return xmlutil.MasterTemplate(root, 1, nsmap={alias: namespace})


class BackupExportImportTemplate(xmlutil.TemplateBuilder):
    def construct(self):
        root = xmlutil.TemplateElement('backup-record',
                                       selector='backup-record')
        make_backup_export_import_record(root)
        alias = Backups.alias
        namespace = Backups.namespace
        return xmlutil.MasterTemplate(root, 1, nsmap={alias: namespace})


class CreateDeserializer(wsgi.MetadataXMLDeserializer):
    def default(self, string):
        dom = utils.safe_minidom_parse_string(string)
        backup = self._extract_backup(dom)
        return {'body': {'backup': backup}}

    def _extract_backup(self, node):
        backup = {}
        backup_node = self.find_first_child_named(node, 'backup')

        attributes = ['container', 'display_name',
                      'display_description', 'volume_id',
                      'parent_id']

        for attr in attributes:
            if backup_node.getAttribute(attr):
                backup[attr] = backup_node.getAttribute(attr)
        return backup


class RestoreDeserializer(wsgi.MetadataXMLDeserializer):
    def default(self, string):
        dom = utils.safe_minidom_parse_string(string)
        restore = self._extract_restore(dom)
        return {'body': {'restore': restore}}

    def _extract_restore(self, node):
        restore = {}
        restore_node = self.find_first_child_named(node, 'restore')
        if restore_node.getAttribute('volume_id'):
            restore['volume_id'] = restore_node.getAttribute('volume_id')
        return restore


class BackupImportDeserializer(wsgi.MetadataXMLDeserializer):
    def default(self, string):
        dom = utils.safe_minidom_parse_string(string)
        backup = self._extract_backup(dom)
        retval = {'body': {'backup-record': backup}}
        return retval

    def _extract_backup(self, node):
        backup = {}
        backup_node = self.find_first_child_named(node, 'backup-record')

        attributes = ['backup_service', 'backup_url']

        for attr in attributes:
            if backup_node.getAttribute(attr):
                backup[attr] = backup_node.getAttribute(attr)
        return backup


class BackupsController(wsgi.Controller):
    """The Backups API controller for the OpenStack API."""

    _view_builder_class = backup_views.ViewBuilder

    def __init__(self):
        self.backup_api = backupAPI.API()
        super(BackupsController, self).__init__()

    @wsgi.serializers(xml=BackupTemplate)
    def show(self, req, id):
        """Return data about the given backup."""
        LOG.debug('show called for member %s', id)
        context = req.environ['cinder.context']

        try:
            backup = self.backup_api.get(context, backup_id=id)
            req.cache_db_backup(backup)
        except exception.BackupNotFound as error:
            raise exc.HTTPNotFound(explanation=error.msg)

        return self._view_builder.detail(req, backup)

    def delete(self, req, id):
        """Delete a backup."""
        LOG.debug('delete called for member %s', id)
        context = req.environ['cinder.context']

        LOG.info(_LI('Delete backup with id: %s'), id, context=context)

        try:
            self.backup_api.delete(context, id)
        except exception.BackupNotFound as error:
            raise exc.HTTPNotFound(explanation=error.msg)
        except exception.InvalidBackup as error:
            raise exc.HTTPBadRequest(explanation=error.msg)

        return webob.Response(status_int=202)

    @wsgi.serializers(xml=BackupsTemplate)
    def index(self, req):
        """Returns a summary list of backups."""
        return self._get_backups(req, is_detail=False)

    @wsgi.serializers(xml=BackupsTemplate)
    def detail(self, req):
        """Returns a detailed list of backups."""
        return self._get_backups(req, is_detail=True)

    @staticmethod
    def _get_backup_filter_options():
        """Return volume search options allowed by non-admin."""
        return ('name', 'status', 'volume_id')

    def _get_backups(self, req, is_detail):
        """Returns a list of backups, transformed through view builder."""
        context = req.environ['cinder.context']
        filters = req.params.copy()

        utils.remove_invalid_filter_options(context,
                                            filters,
                                            self._get_backup_filter_options())

        if 'name' in filters:
            filters['display_name'] = filters['name']
            del filters['name']

        backups = self.backup_api.get_all(context, search_opts=filters)
        backup_count = len(backups)
        limited_list = common.limited(backups, req)
        req.cache_db_backups(limited_list)

        if is_detail:
            backups = self._view_builder.detail_list(req, limited_list,
                                                     backup_count)
        else:
            backups = self._view_builder.summary_list(req, limited_list,
                                                      backup_count)
        return backups

    # TODO(frankm): Add some checks here including
    # - whether requested volume_id exists so we can return some errors
    #   immediately
    # - maybe also do validation of swift container name
    @wsgi.response(202)
    @wsgi.serializers(xml=BackupTemplate)
    @wsgi.deserializers(xml=CreateDeserializer)
    def create(self, req, body):
        """Create a new backup."""
        LOG.debug('Creating new backup %s', body)
        if not self.is_valid_body(body, 'backup'):
            raise exc.HTTPBadRequest()

        context = req.environ['cinder.context']

        try:
            backup = body['backup']
            volume_id = backup['volume_id']
        except KeyError:
            msg = _("Incorrect request body format")
            raise exc.HTTPBadRequest(explanation=msg)
        container = backup.get('container', None)
        name = backup.get('name', None)
        description = backup.get('description', None)
        incremental = backup.get('incremental', False)

        periodic = backup.get('periodic', False)
        if periodic:
            # new_ctx = context.to_dict()
            # new_ctx['periodic'] = True
            # context = context.from_dict(new_ctx)
            context.set_periodic(True)
            LOG.info(_LI("Creating periodic backup of volume %(volume_id)s "
                         "in container %(container)s"),
                     {'volume_id': volume_id, 'container': container},
                     context=context)
        else:
            LOG.info(_LI("Creating backup of volume %(volume_id)s in container"
                         " %(container)s"),
                     {'volume_id': volume_id, 'container': container},
                     context=context)

        try:
            new_backup = self.backup_api.create(context, name, description,
                                                volume_id, container,
                                                incremental)
        except exception.InvalidVolume as error:
            raise exc.HTTPBadRequest(explanation=error.msg)
        except exception.VolumeNotFound as error:
            raise exc.HTTPNotFound(explanation=error.msg)
        except exception.ServiceNotFound as error:
            raise exc.HTTPInternalServerError(explanation=error.msg)
        except exception.InvalidBackup as error:
            raise exc.HTTPBadRequest(explanation=error.msg)

        retval = self._view_builder.summary(req, dict(new_backup.iteritems()))
        return retval

    @wsgi.response(202)
    @wsgi.serializers(xml=BackupRestoreTemplate)
    @wsgi.deserializers(xml=RestoreDeserializer)
    def restore(self, req, id, body):
        """Restore an existing backup to a volume."""
        LOG.debug('Restoring backup %(backup_id)s (%(body)s)',
                  {'backup_id': id, 'body': body})
        if not self.is_valid_body(body, 'restore'):
            msg = _("Incorrect request body format")
            raise exc.HTTPBadRequest(explanation=msg)

        context = req.environ['cinder.context']
        restore = body['restore']
        volume_id = restore.get('volume_id', None)

        periodic = restore.get('periodic', False)
        if periodic:
            backup = self.backup_api.get(context, backup_id=id)
            if backup['volume_id'] != volume_id:
                LOG.debug("Can't restore backup %(backup_id)s to "
                          "volume %(volume_id)s",
                          {'backup_id': id, 'volume_id': volume_id},
                          context=context)
                msg = _("Periodic backup must be restored to "
                        "the original volume")
                raise exc.HTTPBadRequest(explanation=msg)
            context.set_periodic(True)
            LOG.info(_LI("Restoring periodic backup %(backup_id)s to "
                         "volume %(volume_id)s"),
                     {'backup_id': id, 'volume_id': volume_id},
                     context=context)
        else:
            LOG.info(_LI("Restoring backup %(backup_id)s to "
                         "volume %(volume_id)s"),
                     {'backup_id': id, 'volume_id': volume_id},
                     context=context)

        try:
            new_restore = self.backup_api.restore(context,
                                                  backup_id=id,
                                                  volume_id=volume_id)
        except exception.InvalidInput as error:
            raise exc.HTTPBadRequest(explanation=error.msg)
        except exception.InvalidVolume as error:
            raise exc.HTTPBadRequest(explanation=error.msg)
        except exception.InvalidBackup as error:
            raise exc.HTTPBadRequest(explanation=error.msg)
        except exception.BackupNotFound as error:
            raise exc.HTTPNotFound(explanation=error.msg)
        except exception.VolumeNotFound as error:
            raise exc.HTTPNotFound(explanation=error.msg)
        except exception.VolumeSizeExceedsAvailableQuota as error:
            raise exc.HTTPRequestEntityTooLarge(
                explanation=error.msg, headers={'Retry-After': 0})
        except exception.VolumeLimitExceeded as error:
            raise exc.HTTPRequestEntityTooLarge(
                explanation=error.msg, headers={'Retry-After': 0})

        retval = self._view_builder.restore_summary(
            req, dict(new_restore.iteritems()))
        return retval

    @wsgi.response(200)
    @wsgi.serializers(xml=BackupExportImportTemplate)
    def export_record(self, req, id):
        """Export a backup."""
        LOG.debug('export record called for member %s.', id)
        context = req.environ['cinder.context']

        try:
            backup_info = self.backup_api.export_record(context, id)
        except exception.BackupNotFound as error:
            raise exc.HTTPNotFound(explanation=error.msg)
        except exception.InvalidBackup as error:
            raise exc.HTTPBadRequest(explanation=error.msg)

        retval = self._view_builder.export_summary(
            req, dict(backup_info.iteritems()))
        LOG.debug('export record output: %s.', retval)
        return retval

    @wsgi.response(201)
    @wsgi.serializers(xml=BackupTemplate)
    @wsgi.deserializers(xml=BackupImportDeserializer)
    def import_record(self, req, body):
        """Import a backup."""
        LOG.debug('Importing record from %s.', body)
        if not self.is_valid_body(body, 'backup-record'):
            msg = _("Incorrect request body format.")
            raise exc.HTTPBadRequest(explanation=msg)
        context = req.environ['cinder.context']
        import_data = body['backup-record']
        # Verify that body elements are provided
        try:
            backup_service = import_data['backup_service']
            backup_url = import_data['backup_url']
        except KeyError:
            msg = _("Incorrect request body format.")
            raise exc.HTTPBadRequest(explanation=msg)
        LOG.debug('Importing backup using %(service)s and url %(url)s.',
                  {'service': backup_service, 'url': backup_url})

        try:
            new_backup = self.backup_api.import_record(context,
                                                       backup_service,
                                                       backup_url)
        except exception.BackupNotFound as error:
            raise exc.HTTPNotFound(explanation=error.msg)
        except exception.InvalidBackup as error:
            raise exc.HTTPBadRequest(explanation=error.msg)
        except exception.ServiceNotFound as error:
            raise exc.HTTPInternalServerError(explanation=error.msg)

        retval = self._view_builder.summary(req, dict(new_backup.iteritems()))
        LOG.debug('import record output: %s.', retval)
        return retval

    @wsgi.response(202)
    @wsgi.serializers(xml=BackupTemplate)
    @wsgi.deserializers(xml=CreateDeserializer)
    def bcec(self, req, body):
        """Create backup for volume-based instance."""

        # Validate body
        LOG.debug('Creating new backup %s', body)
        if 'backup' not in body:
            raise exc.HTTPBadRequest()

        context = req.environ['cinder.context']
        backup_list = body['backup']

        # Verify parameters in backup_list are equal,
        # including 'container', 'description', 'name',
        # 'instance_uuid', 'incremental'
        # Need to confirm with Jianping!!

        # container = set()
        # description = set()
        # name = set()
        # instance_uuid = set()
        # incremental = set()
        # for backup in backup_list:
        #     try:
        #         volume_id = backup['volume_id']
        #         container.add(backup['container'])
        #         description.add(backup['description'])
        #         name.add(backup['name'])
        #         instance_uuid.add(backup['instance_uuid'])
        #         incremental.add(backup['incremental'])
        #     except KeyError:
        #         msg = _("Incorrect request body format")
        #         raise exc.HTTPBadRequest(explanation=msg)
        #     self.validate_name_and_description(backup)
        # if len(instance_uuid) != 1:
        #     msg = _("Invalid request body format: \
        #             volumes are not attached to the same instance.")
        #     raise exc.HTTPBadRequest(explanation=msg)
        # for para in [container, description, name, incremental]:
        #     if len(para) != 1:
        #         msg = _("Invalid request body format: \
        #                 parameters in backups are not equal")
        #         raise exc.HTTPBadRequest(explanation=msg)
        try:
            instance_uuid = backup_list[0]['instance_uuid']
            container = backup_list[0].get('container', None)
            description = backup_list[0].get('description', None)
            name = backup_list[0].get('name', None)
            incremental = backup_list[0].get('incremental', False)
            periodic = backup_list[0].get('periodic', False)
            force = True
            volumes = [backup['volume_id'] for backup in backup_list]
            for backup in backup_list[1:]:
                if instance_uuid != backup['instance_uuid']:
                    msg = _("Invalid request body format: "
                            "volumes are not attached to the same instance.")
                    raise exc.HTTPBadRequest(explanation=msg)
                if container != backup.get('container', None) or \
                   description != backup.get('description', None) or \
                   name != backup.get('name', None) or \
                   incremental != backup.get('incremental', False) or \
                   periodic != backup.get('periodic', False):
                    msg = _("Invalid request body format: "
                            "parameters in backups are not equal")
                    raise exc.HTTPBadRequest(explanation=msg)
                # self.validate_name_and_description(backup)
        except KeyError:
            msg = _("Incorrect request body format")
            raise exc.HTTPBadRequest(explanation=msg)

        if periodic:
            context.set_periodic(True)
            LOG.info(_LI("Creating periodic backup of instance "
                         "%(instance_uuid)s with volumes %(volumes)s "
                         "in container %(container)s"),
                     {'instance_uuid': instance_uuid,
                      'volumes': volumes,
                      'container': container},
                     context=context)
        else:
            LOG.info(_LI("Creating backup of instance %(instance_uuid)s "
                         "with volumes %(volumes)s "
                         "in container %(container)s"),
                     {'instance_uuid': instance_uuid,
                      'volumes': volumes,
                      'container': container},
                     context=context)

        try:
            new_backup = self.backup_api.create_instance_backup(context,
                                                                instance_uuid,
                                                                name,
                                                                description,
                                                                volumes,
                                                                container,
                                                                incremental,
                                                                None,
                                                                force)
        # Need to consider these exceptions
        except exception.InvalidVolume as error:
            raise exc.HTTPBadRequest(explanation=error.msg)
        except exception.VolumeNotFound as error:
            raise exc.HTTPNotFound(explanation=error.msg)
        except exception.ServiceNotFound as error:
            raise exc.HTTPInternalServerError(explanation=error.msg)

        retval = self._view_builder.instance_backup_summary(req, new_backup)
        return retval


class Backups(extensions.ExtensionDescriptor):
    """Backups support."""

    name = 'Backups'
    alias = 'backups'
    namespace = 'http://docs.openstack.org/volume/ext/backups/api/v1'
    updated = '2012-12-12T00:00:00+00:00'

    def get_resources(self):
        resources = []
        res = extensions.ResourceExtension(
            Backups.alias, BackupsController(),
            collection_actions={'detail': 'GET', 'import_record': 'POST',
                                'bcec': 'POST'},
            member_actions={'restore': 'POST', 'export_record': 'GET',
                            'action': 'POST'})
        resources.append(res)
        return resources

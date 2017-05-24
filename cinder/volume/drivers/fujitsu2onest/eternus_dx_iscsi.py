# Copyright (c) 2015 FUJITSU LIMITED
# Copyright (c) 2012 EMC Corporation.
# Copyright (c) 2012 OpenStack Foundation
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
#

"""
Design for oNest volume backup for Kilo by fnst.
"""
import six
from cinder import context
from cinder import exception
from cinder import utils
from cinder.i18n import _, _LI
from cinder.openstack.common import fileutils
from cinder.volume.drivers.fujitsu import eternus_dx_iscsi
from cinder.volume.drivers.fujitsu2onest import eternus_dx_common
from oslo_utils import excutils
from oslo_log import log as logging

LOG = logging.getLogger(__name__)

FJ_SRC_VOL_ID = "FJ_Source_Volume_ID"

class FJDXISCSIDriver(eternus_dx_iscsi.FJDXISCSIDriver):

    def __init__(self, *args, **kwargs):
        super(FJDXISCSIDriver, self).__init__(*args, **kwargs)
        self.common = eternus_dx_common.FJDXCommon(
            'iSCSI', configuration=self.configuration)

    def backup_volume(self, context, backup, backup_service):
        LOG.info(_LI('Backup volume, volume id: %s.'), backup.volume_id)
        return self._backup_volume_temp_volume(
            context, backup, backup_service)

    def _backup_volume_temp_volume(self, context, backup, backup_service):
        """Create a new backup from an existing volume.
        For in-use volume, create a temp volume and back it up.
        """

        volume = self.db.volume_get(context, backup.volume_id)
        device_to_backup = volume

        # For in-use volume, create clone volume for temp and back it up.
        # At last, delete temp
        temp_vol_ref = None
        try:
            # Get source volume status.
            if hasattr(context,'vol_previous_status'):
                previous_status = context.vol_previous_status
                if previous_status == 'in-use':
                    LOG.info(_LI('FUJITSU backup, source volume is in-use,'
                                 'use clone volume to create backup.'))
                    temp_vol_ref = self._create_temp_cloned_volume(
                        context, volume)
                    device_to_backup = temp_vol_ref
                else:
                    LOG.info(_LI('FUJITSU backup, '
                                 'source volume is available, '
                                 'just use it to create backup.'))
            else:
                LOG.info(_LI('Normal backup, volume id:%s'), backup.volume_id)

            backup_info = self._backup_device(
                context, backup, backup_service, device_to_backup)
        finally:
            # No matter backup successful or failed, delete temp volume if exist.
            if temp_vol_ref:
                LOG.info(_LI('FUJITSU backup, '
                             'delete temp volume for in-use backup.'))
                self.delete_volume(temp_vol_ref)
                self.db.volume_destroy(context, temp_vol_ref['id'])
        LOG.info(_LI('Backup finish, volume id:%(vol_id)s, '
                     'backup id:%(backup_id)s, backup_info:%(info)s.'),
                     {'vol_id': backup.volume_id,
                      'backup_id': backup['id'],
                      'info': backup_info})
        return backup_info

    def _backup_device(self, context, backup, backup_service, device):
        """Create a new backup from a volume."""

        LOG.debug('Creating a new backup for %s.', device['name'])
        use_multipath = self.configuration.use_multipath_for_image_xfer
        enforce_multipath = self.configuration.enforce_multipath_for_image_xfer
        properties = utils.brick_get_connector_properties(use_multipath,
                                                          enforce_multipath)
        attach_info, device = self._attach_volume(context, device, properties)

        try:
            volume_path = attach_info['device']['path']

            # Secure network file systems will not chown files.
            if self.secure_file_operations_enabled():
                with fileutils.file_open(volume_path) as volume_file:
                    backup_info = backup_service.backup(backup, volume_file)
            else:
                with utils.temporary_chown(volume_path):
                    with fileutils.file_open(volume_path) as volume_file:
                        backup_info = backup_service.backup(backup, volume_file)

        finally:
            self._detach_volume(context, attach_info, device, properties)
        return backup_info

    def _create_temp_cloned_volume(self, context, volume):
        temp_volume = {
            'size': volume['size'],
            'host': volume['host'],
            'user_id': context.user_id,
            'project_id': context.project_id,
        }
        temp_vol_ref = self.db.volume_create(context, temp_volume)

        # Create clone volume.
        try:
            element_path = self.common.create_volume(temp_vol_ref)[0]
            temp_vol_ref['provider_location'] = six.text_type(element_path)
            self.common.clone_volume_for_onest(volume, temp_vol_ref)
        except Exception:
            with excutils.save_and_reraise_exception():
                LOG.error(_('FUJITSU backup, create clone volume failed.'))
                self.delete_volume(temp_vol_ref)
                self.db.volume_destroy(context, temp_vol_ref['id'])
        return temp_vol_ref

    def create_volume(self, volume):
        '''
        Create volume
        '''
        LOG.info(_('create_volume,Enter method'))
        element_path     = None
        metadata         = None
        v_metadata       = None
        d_metadata       = {}
        data             = None

        v_metadata = volume.get('volume_metadata')
        for data in v_metadata:
           d_metadata[data['key']] = data['value']

        if FJ_SRC_VOL_ID not in d_metadata:
            # create volume
            (element_path, metadata) = self.common.create_volume(volume)
        else:
            # create cloned volume for backup solution
            # get source & target volume information
            ctxt = context.get_admin_context()
            volume_id = volume['id']
            volume_size = volume['size']
            src_volid = d_metadata[FJ_SRC_VOL_ID]
            if src_volid is None:
                msg = (_('create_volume,Source volume id is None'))
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            # end of if

            src_vref = self.db.volume_get(ctxt, src_volid)

            # check target volume information with reference to  source volume
            src_vref_size = src_vref['size']
            src_vref_status = src_vref['status']
            if ('error' in src_vref_status) or ('deleting' in src_vref_status):
                msg = (_('Invalid volume status : %(status)s') % {'status': src_vref_status})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            elif volume_size < src_vref_size:
                msg = (_('Volume size %(volume_size)sGB cannot be smaller than original volume size %(src_vref_size)sGB. They must be >= original volume size.')
                        % {'volume_size': volume_size,
                           'src_vref_size': src_vref_size})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            # end of if

            # main processing
            LOG.info(_('create_volume,Call create cloned volume'))
            (element_path, metadata) = self.common.create_cloned_volume(volume, src_vref, use_service_name=True)
            LOG.info(_('create_volume,Return create cloned volume'))

            # post processing
            try:
                if src_vref.bootable:
                    self.db.volume_update(ctxt, volume['id'], {'bootable': True})
                    self.db.volume_glance_metadata_copy_from_volume_to_volume(
                        ctxt, src_volid, volume['id'])
                # end of if

                self.db.volume_update(ctxt, volume['id'], {'source_volid': src_volid})
            except Exception as ex:
               msg = (_('create volume, Failed updating volume metadata,'
                        'reason: %(reason)s,'
                        'volume_id: %(volume_id)s,'
                        'src_volid: %(src_volid)s,')
                       % {'reason': ex,
                          'volume_id': volume['id'],
                          'src_volid': src_volid})
               raise exception.MetadataCopyFailure(reason=msg)
        # end of if

        d_metadata.update(metadata)

        LOG.info(_('create_volume,Exit method'))
        return {'provider_location' : six.text_type(element_path),
                'metadata'          : d_metadata}
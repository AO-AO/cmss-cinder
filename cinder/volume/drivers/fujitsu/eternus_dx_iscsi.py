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

'''
iSCSI Cinder Volume driver for Fujitsu ETERNUS DX S2 and S3 series.
'''

##------------------------------------------------------------------------------------------------##
##                                                                                                ##
##  ETERNUS OpenStack Volume Driver                                                               ##
##                                                                                                ##
##  Note      :                                                                                   ##
##  $Date:: 2015-07-08 10:58:35 +0900#$                                                           ##
##  $Revision: 10865 $                                                                            ##
##  File Name : eternus_dx_iscsi.py                                                               ##
##  Copyright 2015 FUJITSU LIMITED                                                                ##
##                                                                                                ##
##  history :                                                                                     ##
##      2014.03 : 1.0.0 : volume(create,delete,attach,detach,create from snapshot)                ##
##                        snapshot(create,delete)                                                 ##
##      2014.04 : 1.0.1 : Fix comment                                                             ##
##      2014.06 : 1.1.0 : Support 4 features                                                      ##
##                         Extend volume                                                          ##
##                         Clone Volume                                                           ##
##                         Copy Image to Volume                                                   ##
##                         Copy Volume to Image                                                   ##
##      2014.11 : 1.1.1 : Support ETERNUS DX V10L30 Firmware                                      ##
##      2015.01 : 1.1.2 : Improve Copy Image to Volume                                            ##
##      2015.02 : 1.1.3 : Improve initilize_connection, create image volume                       ##
##      2015.04 : 1.1.4 : QoS, Format Volume in Delete Volume, Extend Volume on RAID Group,       ##
##                        Create Snapshot when specifying TPP as EternusPool,                     ##
##                        Data Protection communicating with CCM                                  ##
##      2015.07 : 1.1.5 : for kilo                                                                ##
##------------------------------------------------------------------------------------------------##

from cinder import context
from cinder import exception
from cinder import utils
from cinder.i18n import _, _LE, _LW
from cinder.volume import driver
from cinder.volume.drivers.fujitsu import eternus_dx_common
from cinder.volume.drivers.fujitsu import image_utils
from oslo_concurrency import lockutils
from oslo_config import cfg
from oslo_log import log as logging
import time
import threading
import six

LOG = logging.getLogger(__name__)

#**************************************************************************************************#
FJ_SRC_VOL_ID = "FJ_Source_Volume_ID"
#**************************************************************************************************#
FJ_ETERNUS_DX_OPT_list = [cfg.BoolOpt('use_fujitsu_image_volume',
                                default=True,
                                help='Use image volume when copying image to volume')]
#**************************************************************************************************#

#--------------------------------------------------------------------------------------------------#
# Class : FJDXISCSIDriver                                                                          #
#         summary      : iSCSI cinder volume driver for Fujitsu ETERNUS DX                         #
#--------------------------------------------------------------------------------------------------#
class FJDXISCSIDriver(driver.ISCSIDriver):
    '''
    ETERNUS Cinder Volume Driver version1.0
    for Fujitsu ETERNUS DX S2 and S3 series
    '''
    # initialize
    VERSION = "OSVD1.1.5"

    #----------------------------------------------------------------------------------------------#
    # Method : __init__                                                                            #
    #         summary      :                                                                       #
    #         return-value :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def __init__(self, *args, **kwargs):
        '''
        Constructor
        '''
        LOG.info(_('Starting FJDXISCSIDriver ($Revision: 10865 $)'))
        super(FJDXISCSIDriver, self).__init__(*args, **kwargs)
        self.configuration.append_config_values(FJ_ETERNUS_DX_OPT_list)
        self.common = eternus_dx_common.FJDXCommon(
            'iSCSI',
            configuration=self.configuration)
        self.first_loop = True

        return

    #----------------------------------------------------------------------------------------------#
    # Method : check_for_setup_error                                                               #
    #         summary      :                                                                       #
    #         return-value :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def check_for_setup_error(self):
        pass
        return

    #----------------------------------------------------------------------------------------------#
    # Method : create_volume                                                                       #
    #         summary      : create volume on ETERNUS                                              #
    #         return-value : volume metadata                                                       #
    #----------------------------------------------------------------------------------------------#
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

        metadata.update(d_metadata)

        LOG.info(_('create_volume,Exit method'))
        return {'provider_location' : six.text_type(element_path),
                'metadata'          : metadata}

    #----------------------------------------------------------------------------------------------#
    # Method : create_volume_from_snapshot                                                         #
    #         summary      : create volume from snapshot                                           #
    #         return-value : volume metadata                                                       #
    #----------------------------------------------------------------------------------------------#
    def create_volume_from_snapshot(self, volume, snapshot):
        '''
        Creates a volume from a snapshot.
        '''
        LOG.info(_('create_volume_from_snapshot,Enter method'))
        element_path     = None
        metadata         = None
        v_metadata       = None
        data             = None

        (element_path, metadata) = self.common.create_volume_from_snapshot(volume, snapshot)

        v_metadata = volume.get('volume_metadata')
        for data in v_metadata:
            metadata[data['key']] = data['value']

        LOG.info(_('create_volume_from_snapshot,Exit method'))
        return {'provider_location' : six.text_type(element_path),
                'metadata'          : metadata}

    #----------------------------------------------------------------------------------------------#
    # Method : create_cloned_volume                                                                #
    #         summary      : create cloned volume on ETERNUS                                       #
    #         return-value : volume metadata                                                       #
    #----------------------------------------------------------------------------------------------#
    def create_cloned_volume(self, volume, src_vref):
        """Create cloned volume."""
        LOG.info(_('create_cloned_volume,Enter method'))
        element_path     = None
        metadata         = None
        v_metadata       = None
        data             = None

        (element_path, metadata) = self.common.create_cloned_volume(volume, src_vref)

        v_metadata = volume.get('volume_metadata')
        for data in v_metadata:
            metadata[data['key']] = data['value']

        LOG.info(_('create_cloned_volume,Exit method'))
        return {'provider_location' : six.text_type(element_path),
                'metadata'          : metadata}

    #----------------------------------------------------------------------------------------------#
    # Method : delete_volume                                                                       #
    #         summary      : delete volume on ETERNUS                                              #
    #         return-value : none                                                                  #
    #----------------------------------------------------------------------------------------------#
    def delete_volume(self, volume):
        '''
        Delete volume on ETERNUS.
        '''
        LOG.info(_('delete_volume,Enter method'))

        self.common.delete_volume(volume)

        LOG.info(_('delete_volume,Exit method'))
        return

    #----------------------------------------------------------------------------------------------#
    # Method : create_snapshot                                                                     #
    #         summary      : create snapshot using SnapOPC                                         #
    #         return-value : none                                                                  #
    #----------------------------------------------------------------------------------------------#
    def create_snapshot(self, snapshot):
        '''
        Creates a snapshot.
        '''
        LOG.info(_('create_snapshot,Enter method'))
        element_path     = None

        element_path = self.common.create_snapshot(snapshot)

        LOG.info(_('create_snapshot,Exit method'))
        return {'provider_location' : six.text_type(element_path)}

    #----------------------------------------------------------------------------------------------#
    # Method : delete_snapshot                                                                     #
    #         summary      : delete snapshot                                                       #
    #         return-value : none                                                                  #
    #----------------------------------------------------------------------------------------------#
    def delete_snapshot(self, snapshot):
        '''
        Deletes a snapshot.
        '''
        LOG.info(_('delete_snapshot,Enter method'))

        self.common.delete_snapshot(snapshot)

        LOG.info(_('delete_snapshot,Exit method'))
        return

    #----------------------------------------------------------------------------------------------#
    # Method : ensure_export                                                                       #
    #         summary      : Driver entry point to get the export info for an existing volume.     #
    #         return-value : none                                                                  #
    #----------------------------------------------------------------------------------------------#
    def ensure_export(self, context, volume):
        """Driver entry point to get the export info for an existing volume."""
        pass
        return

    #----------------------------------------------------------------------------------------------#
    # Method : create_export                                                                       #
    #         summary      : Driver entry point to get the export info for a new volume.           #
    #         return-value : none                                                                  #
    #----------------------------------------------------------------------------------------------#
    def create_export(self, context, volume):
        """Driver entry point to get the export info for a new volume."""
        pass
        return

    #----------------------------------------------------------------------------------------------#
    # Method : remove_export                                                                       #
    #         summary      : Driver entry point to remove an export for a volume.                  #
    #         return-value : none                                                                  #
    #----------------------------------------------------------------------------------------------#
    def remove_export(self, context, volume):
        """Driver entry point to remove an export for a volume."""
        pass
        return

    #----------------------------------------------------------------------------------------------#
    # Method : initialize_connection                                                               #
    #         summary      : set HostAffinityGroup on ETERNUS                                      #
    #         return-value : connection info                                                       #
    #----------------------------------------------------------------------------------------------#
    def initialize_connection(self, volume, connector):
        '''
        Allow connection to connector and return connection info.
        '''
        LOG.info(_('initialize_connection,Enter method'))

        info=self.common.initialize_connection(volume, connector)

        LOG.info(_('initialize_connection,Exit method'))
        return info

    #----------------------------------------------------------------------------------------------#
    # Method : terminate_connection                                                                #
    #         summary      : remove HostAffinityGroup on ETERNUS                                   #
    #         return-value :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def terminate_connection(self, volume, connector, **kwargs):
        '''
        Disallow connection from connector.
        '''
        LOG.info(_('terminate_connection,Enter method'))

        self.common.terminate_connection(volume, connector)

        LOG.info(_('terminate_connection,Exit method'))
        return

    #----------------------------------------------------------------------------------------------#
    # Method : get_volume_stats                                                                    #
    #         summary      : get pool capacity                                                     #
    #         return-value : self.stats                                                            #
    #----------------------------------------------------------------------------------------------#
    def get_volume_stats(self, refresh=False):
        '''
        Get volume stats.
        If 'refresh' is True, run update the stats first.
        '''
        LOG.debug(_('*****get_volume_stats,Enter method'))

        if refresh == True:
            data                        = self.common.refresh_volume_stats()
            backend_name                = self.configuration.safe_get('volume_backend_name')
            data['volume_backend_name'] = backend_name or 'FJDXISCSIDriver'
            data['storage_protocol']    = 'iSCSI'
            self._stats                 = data
        # end of if

        LOG.debug(_('*****get_volume_stats,Exit method'))

        if self.first_loop is True:
            self.first_loop = False
            monitor_thread=threading.Thread(target=self.common.monitor_image_volume)
            monitor_thread.start()

        return self._stats

    #----------------------------------------------------------------------------------------------#
    # Method : extend_volume                                                                       #
    #         summary      : extend volume on ETERNUS                                              #
    #         return-value :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def extend_volume(self, volume, new_size):
        '''
        Extend volume.
        '''
        LOG.info(_('extend_volume,Enter method'))

        self.common.extend_volume(volume, new_size)

        LOG.info(_('extend_volume,Exit method'))
        return

    #----------------------------------------------------------------------------------------------#
    # Method : copy_image_to_volume                                                                #
    #         summary      : copy image to volume                                                  #
    #         return-value :                                                                       #
    #----------------------------------------------------------------------------------------------#
    @lockutils.synchronized('ETERNUS_DX-img', 'cinder-', True)
    def copy_image_to_volume(self, context, volume, image_service, image_id):
        '''
        Fetch the image from image_service and write it to the volume.
        '''
        LOG.info(_('copy_image_to_volume,Enter method (use_image_volume: %s)') 
                   % self.configuration.use_fujitsu_image_volume)

        setup        = False
        cloned       = False
        image_volume = None

        setup = self._setup_image_volume(volume)
        use_multipath = self.configuration.use_multipath_for_image_xfer
        enforce_multipath = self.configuration.enforce_multipath_for_image_xfer
        properties = utils.brick_get_connector_properties(use_multipath,
                                                          enforce_multipath)

        if self.configuration.use_fujitsu_image_volume is True:

            try: # creating image volume using dd command
                if setup is False:
                    cloned = self._copy_image_volume_to_volume(volume, image_id)

                if cloned is True:
                    LOG.info(_('copy_image_to_volume,create volume using OPC'))
                else:
                    image_volume = self._create_image_volume(volume, image_id)

                    try:
                        self._write_image_to_volume(context, properties, image_volume, image_service, image_id)
                        self._add_image_volume(image_volume, image_id)
                    except Exception as ex:
                        LOG.info(ex)
                        self.delete_volume(image_volume)
                        raise ex

                    if setup is False:
                        cloned = self._copy_image_volume_to_volume(volume, image_id, src_vref=image_volume)
                        LOG.info(_('copy_image_to_volume, create volume by writing image, '
                                   'result : %s') % (cloned))

            except Exception as ex: # when failing to create image volume using dd command
                    LOG.warn(ex)
                    LOG.warn(_('copy_image_to_volume,create volume without creating image volume'))

                    if setup is False:
                        self._write_image_to_volume(context, properties, volume, image_service, image_id)
                    else:
                        raise ex
        elif setup is True: # use_fujitsu_image_volume=False & setup=True
            msg = (_('copy_image_to_volume, this backend specify that use_fujitsu_image_volume is False'))
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        else: # if not self.configuration.use_fujitsu_image_volume is True
            properties = utils.brick_get_connector_properties()
            self._write_image_to_volume(context, properties, volume, image_service, image_id)

        LOG.info(_('copy_image_to_volume,Exit method'))
        return

    #----------------------------------------------------------------------------------------------#
    # Method : _write_image_to_volume                                                              #
    #         summary      : write image to volume                                                 #
    #         return-value :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def _write_image_to_volume(self, context, properties, volume, image_service, image_id):
        '''
        Fetch the image from image_service and write it to the volume using dd command.
        '''
        LOG.debug(_('*****copy_image_to_volume,Enter method'))
        attach_info, volume = self._attach_volume(context, volume, properties)

        try:
            image_utils.fetch_to_raw(context,
                                     image_service,
                                     image_id,
                                     attach_info['device']['path'],
                                     self.configuration.volume_dd_blocksize,
                                     size=volume['size'])
        finally:
            self._detach_volume(context, attach_info, volume, properties)

        LOG.debug(_('*****copy_image_to_volume,Exit method'))
        return

    #----------------------------------------------------------------------------------------------#
    # Method : _setup_image_volume                                                                 #
    #         summary      : setup image volume if setup is needed                                 #
    #         return-value : setup, image_volume                                                   #
    #----------------------------------------------------------------------------------------------#
    def _setup_image_volume(self, volume):
        '''
        Check whether setup mode or not, and prepare for setup mode
        '''
        LOG.debug(_('*****_setup_image_volume,Enter method'))

        setup = False

        meta = volume.get('volume_metadata')
        for data in meta:
            if (data['key'] == 'fujitsu_image_setup') and (data['value'] == 'True'):
                setup = True
                volume_type_id = volume['volume_type_id']
                volume['volume_type_id'] = None
                self.common.delete_volume(volume)
                volume['volume_type_id'] = volume_type_id
                LOG.info(_('_setup_image_volume, setup:%s') % (setup))
                break

        LOG.debug(_('*****_setup_image_volume (setup:%s),Exit method') % (setup))
        return setup

    #----------------------------------------------------------------------------------------------#
    # Method : _create_image_volume                                                                #
    #         summary      : create image volume                                                   #
    #         return-value : image volume information                                              #
    #----------------------------------------------------------------------------------------------#
    def _create_image_volume(self, volume_ref, image_id):
        '''
        Create blank volume for image volume
        '''
        LOG.debug(_('*****_create_image_volume,Enter method'))

        image_volume = self.common.create_image_volume(volume_ref, image_id)

        LOG.debug(_('*****create_image_volume,Exit method'))
        return image_volume

    #----------------------------------------------------------------------------------------------#
    # Method : _add_image_volume                                                                   #
    #         summary      : add image volume                                                      #
    #         return-value :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def _add_image_volume(self, volume_ref, image_id):
        '''
        Create blank volume for image volume
        '''
        LOG.debug(_('*****_add_image_volume,Enter method'))

        self.common.add_image_volume(volume_ref, image_id)

        LOG.debug(_('*****add_image_volume,Exit method'))
        return

    #----------------------------------------------------------------------------------------------#
    # Method : _copy_image_volume_to_volume                                                        #
    #         summary      : copy image volume to volume                                           #
    #         return-value : cloned                                                                #
    #----------------------------------------------------------------------------------------------#
    def _copy_image_volume_to_volume(self, volume, image_id, src_vref=None):
        '''
        Copy image volume to volume
        '''
        LOG.debug(_('*****_copy_image_volume_to_volume,Enter method'))

        cloned = self.common.copy_image_volume_to_volume(volume, image_id, src_vref)

        LOG.debug(_('*****_copy_image_volume_to_volume (cloned:%s),Exit method') % (cloned))
        return cloned

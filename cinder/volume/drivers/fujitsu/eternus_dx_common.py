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
Cinder Volume driver for Fujitsu ETERNUS DX S2 and S3 series.
'''

##------------------------------------------------------------------------------------------------##
##                                                                                                ##
##  ETERNUS OpenStack Volume Driver                                                               ##
##                                                                                                ##
##  Note      :                                                                                   ##
##  $Date:: 2015-07-21 16:43:52 +0900#$                                                           ##
##  $Revision: 10883 $                                                                            ##
##  File Name : eternus_dx_common.py                                                              ##
##  Copyright 2015 FUJITSU LIMITED                                                                ##
##                                                                                                ##
##  history :                                                                                     ##
##      2014.03 : 1.0.0 : volume(create,delete,attach,detach,create from snapshot)                ##
##                        snapshot(create,delete)                                                 ##
##      2014.04 : 1.0.1 : Fix comment                                                             ##
##      2014.06 : 1.1.0 : Support 4 features                                                      ##
##                         Extend Volume                                                          ##
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

import os
import time
import threading
import hashlib
import base64
import uuid
import codecs
import six
from cinder import context
from cinder import db
from cinder import exception
from cinder import utils
from cinder.i18n import _, _LE, _LW
from cinder.volume import volume_types
from cinder.volume import qos_specs
from cinder.volume.configuration import Configuration
from oslo_concurrency import lockutils
from oslo_concurrency import processutils
from oslo_config import cfg
from oslo_log import log as logging
from xml.dom.minidom import *
from xml.etree.ElementTree import *
import functools

LOG = logging.getLogger(__name__)

try:
    import pywbem
except:
    LOG.error(_('import pywbem failed!!'
               'pywbem is necessary for this volume driver.'))

#----------------------------------------------------------------------------------------------#
# Method : FJDXLockutils                                                                       #
#         summary      : lockuils for ETERNUS DX                                               #
#         return-value : result by executing argment function                                  #
#----------------------------------------------------------------------------------------------#
def FJDXLockutils(name, lock_file_prefix, external=False, lock_path=None):
    '''
    lockutils for ETERNUS DX
    '''
    def decorator(func):
        def wrapper(self, *args, **kwargs):
            lockname = 'ETERNUS_DX-' + name + '-' + self._get_drvcfg('EternusIP').replace('.','_')
            @lockutils.synchronized(lockname, lock_file_prefix, external, lock_path)
            @functools.wraps(func)
            def caller():
                return func(self, *args, **kwargs)
            return caller()
        return wrapper
    return decorator


#**************************************************************************************************#
CONF                    = cfg.CONF
VOL_PREFIX              = "FJosv_"
RAIDGROUP               = 2
TPPOOL                  = 5
SNAPOPC                 = 4
OPC                     = 5
RETURN_TO_RESOURCEPOOL  = 19
DETACH                  = 8
INITIALIZED             = 2
UNSYNCHRONIZED          = 3
BROKEN                  = 5
PREPARED                = 11
REPL                    = "FUJITSU_ReplicationService"
STOR_CONF               = "FUJITSU_StorageConfigurationService"
CTRL_CONF               = "FUJITSU_ControllerConfigurationService"
STOR_HWID               = "FUJITSU_StorageHardwareIDManagementService"
MONITOR_IMGVOL_INTERVAL = 600
DELETE_IMGVOL           = "Deleting"
FJ_REMOTE_SRC_META      = "FJ_Remote_Copy_Source"
FJ_REMOTE_DEST_META     = "FJ_Remote_Copy_Destination"
FJ_REC_CLONE            = "Clone"
FJ_REC_MIRROR           = "Mirror"
FJ_VOL_FORMAT_KEY       = "type:delete_with_volume_format"

#**************************************************************************************************#
FJ_ETERNUS_DX_OPT_list = [cfg.StrOpt('cinder_eternus_config_file',
                                default='/etc/cinder/cinder_fujitsu_eternus_dx.xml',
                                help='config file for cinder fujitsu_eternus_dx volume driver'),
                          cfg.StrOpt('fujitsu_min_image_volume_per_storage',
                                default='0',
                                help='minimum number of image volume per storage(ETERNUS)')]

CINDER_CONF_OPT_list   = [cfg.StrOpt('fujitsu_image_management_dir',
                          default=CONF.image_conversion_dir,
                          help='directory for image management file(xml)')]
CONF.register_opts(CINDER_CONF_OPT_list)

FJ_QOS_KEY_list        = ['maxBWS']
#**************************************************************************************************#
POOL_TYPE_dic          = {RAIDGROUP:'RAID_GROUP',
                          TPPOOL   :'Thinporvisioning_POOL'
                          }

OPERATION_dic          = {SNAPOPC:RETURN_TO_RESOURCEPOOL,
                          OPC    :DETACH
                          }

RETCODE_dic            = {'0'    :'Success',
                          '1'    :'Method Not Supported',
                          '4'    :'Failed',
                          '5'    :'Invalid Parameter',
                          '4096' :'Success',
                          '4097' :'Size Not Supported',
                          '4101' :'Target/initiator combination already exposed',
                          '4102' :'Requested logical unit number in use',
                          '32769':'Maximum number of Logical Volume in'
                                  ' a RAID group has been reached',
                          '32770':'Maximum number of Logical Volume in'
                                  ' the storage device has been reached',
                          '32771':'Maximum number of registered Host WWN'
                                  ' has been reached',
                          '32772':'Maximum number of affinity group has been reached',
                          '32773':'Maximum number of host affinity has been reached',
                          '32785':'The RAID group is in busy state',
                          '32786':'The Logical Volume is in busy state',
                          '32787':'The device is in busy state',
                          '32788':'Element Name is in use',
                          '32792':'No Copy License',
                          '32796':'Quick Format Error',
                          '32801':'The CA port is in invalid setting',
                          '32802':'The Logical Volume is Mainframe volume',
                          '32803':'The RAID group is not operative',
                          '32804':'The Logical Volume is not operative',
                          '32808':'No Thin Provisioning License',
                          '32809':'The Logical Element is ODX volume',
                          '32811':'This operation cannot be performed to the NAS resources',
                          '32812':'This operation cannot be performed to the Storage'
                                  ' Cluster resources',
                          '32816':'Fatal error generic',
                          '35302':'Invalid LogicalElement',
                          '35304':'LogicalElement state error',
                          '35316':'Multi-hop error',
                          '35318':'Maximum number of multi-hop has been reached',
                          '35324':'RAID is broken',
                          '35331':'Maximum number of session has been reached(per device)',
                          '35333':'Maximum number of session has been reached(per SourceElement)',
                          '35334':'Maximum number of session has been reached(per TargetElement)',
                          '35335':'Maximum number of Snapshot generation has been'
                                  ' reached (per SourceElement)',
                          '35346':'Copy table size is not setup',
                          '35347':'Copy table size is not enough'
                          }

#**************************************************************************************************#

#--------------------------------------------------------------------------------------------------#
# Class : FJDXCommon                                                                               #
#         summary      : cinder volume driver for Fujitsu ETERNUS DX                               #
#--------------------------------------------------------------------------------------------------#
class FJDXCommon(object):
    '''
    Common code that does not depend on protocol.
    '''

    # initialize
    stats = {'driver_version': '1.1.5',
             'free_capacity_gb': 0,
             'reserved_percentage': 0,
             'storage_protocol': None,
             'total_capacity_gb': 0,
             'vendor_name': 'FUJITSU',
             'QoS_support': True,
             'volume_backend_name': None}

    #----------------------------------------------------------------------------------------------#
    # Method : __init__                                                                            #
    #         summary      :                                                                       #
    #         return-value :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def __init__(self, prtcl, configuration=None):
        '''
        Constructor
        '''
        LOG.info(_('Starting FJDXCommon ($Revision: 10883 $)'))
        self.protocol      = prtcl
        self.configuration = configuration
        self.configuration.append_config_values(FJ_ETERNUS_DX_OPT_list)

        if prtcl == 'iSCSI':
            # get iSCSI ipaddress from driver configuration file
            self.configuration.iscsi_ip_address = self._get_drvcfg('EternusISCSIIP')
        # end of if

        self.conn = self._get_eternus_connection()
        self.configuration.fujitsu_image_management_file=CONF.fujitsu_image_management_dir + '/' + 'cinder_fujitsu_image_management.xml'

        if ((self.configuration.fujitsu_min_image_volume_per_storage.isdigit() is False)
             or (self.configuration.use_fujitsu_image_volume is False)):
            self.configuration.fujitsu_min_image_volume_per_storage='0'
        # end of if

        self._check_user()
        self.invalid_migration_list = []
        return

    #----------------------------------------------------------------------------------------------#
    # Method : create_volume                                                                       #
    #         summary      : create volume on ETERNUS                                              #
    #         return-value : volume metadata                                                       #
    #----------------------------------------------------------------------------------------------#
    def create_volume(self, volume):
        '''
        create volume on ETERNUS
        '''
        # volumesize    : volume size Byte
        # volumename    : volumename on ETERNUS
        # eternus_pool  : poolname
        # pool          : pool instance
        # pooltype      : RAID(2) or TPP(5)
        # configservice : FUJITSU_StorageConfigurationService
        # msg           : message
        # rc            : result of invoke method
        # errordesc     : error message
        # job           : job information
        # element       : element (including ETERNUS SMI-S class information)
        # element_path  : element path
        # metadata      : additional metadata
        # volume_no     : OLU NO

        LOG.debug(_('*****create_volume,Enter method'))

        # initialize
        systemnamelist = None
        volumesize     = 0
        volumename     = None
        eternus_pool   = None
        pool           = None
        pooltype       = 0
        configservice  = None
        msg            = None
        rc             = 0
        errordesc      = None
        job            = None
        element        = None
        element_path   = {}
        metadata       = {}
        volume_no      = None

        # main processing
        # conversion of the unit. GB to B
        volumesize = int(volume['size']) * 1073741824

        # create to volumename on ETERNUS from cinder VolumeID
        volumename = self._create_volume_name(volume['id'])
        LOG.debug(_('*****create_volume,volumename:%(volumename)s,'
                    'volumesize:%(volumesize)u')
                   % {'volumename': volumename,
                      'volumesize': volumesize})

        self.conn = self._get_eternus_connection()

        # get poolname from driver configuration file
        eternus_pool = self._get_drvcfg('EternusPool')
        # Existence check the pool
        pool = self._find_pool(eternus_pool)
        if pool is None:
            msg = (_('create_volume,'
                     'eternus_pool:%(eternus_pool)s,'
                     'not found.')
                    % {'eternus_pool': eternus_pool})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        # set pooltype
        if 'RSP' in pool['InstanceID']:
            pooltype = RAIDGROUP
        else:
            pooltype = TPPOOL
        # end of if

        configservice = self._find_eternus_service(STOR_CONF)
        if configservice is None:
            msg = (_('create_volume,volume:%(volume)s,'
                     'volumename:%(volumename)s,'
                     'eternus_pool:%(eternus_pool),'
                     'Error!! Storage Configuration Service is None.')
                    % {'volume':volume,
                       'volumename': volumename,
                       'eternus_pool':eternus_pool})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if
        LOG.debug(_('*****create_volume,CreateOrModifyElementFromStoragePool,'
                    'ConfigService:%(service)s,'
                    'ElementName:%(volumename)s,'
                    'InPool:%(eternus_pool)s,'
                    'ElementType:%(pooltype)u,'
                    'Size:%(volumesize)u')
                   % {'service':configservice,
                      'volumename': volumename,
                      'eternus_pool':eternus_pool,
                      'pooltype':pooltype,
                      'volumesize': volumesize})

        # Invoke method for create volume
        rc, errordesc, job = self._exec_eternus_service(
            'CreateOrModifyElementFromStoragePool',
            configservice,
            ElementName=volumename,
            InPool=pool,
            ElementType=pywbem.Uint16(pooltype),
            Size=pywbem.Uint64(volumesize))

        if rc == 32788L: #Element Name is in use
            msg = (_('create_volume,'
                     'volumename:%(volumename)s,'
                     'Return code:%(rc)lu,'
                     'Error:%(errordesc)s')
                    % {'volumename': volumename,
                       'rc': rc,
                       'errordesc':errordesc})
            LOG.warn(msg)
        elif rc != 0L and rc != 4096L:
            msg = (_('create_volume,'
                     'volumename:%(volumename)s,'
                     'Return code:%(rc)lu,'
                     'Error:%(errordesc)s')
                    % {'volumename': volumename,
                       'rc': rc,
                       'errordesc':errordesc})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        else:
            element = job['TheElement']
        # end of if

        # set qos to created volume
        self._set_qos(volume)

        # get eternus model for metadata
        # systemname = systemnamelist[0]['IdentifyingNumber']
        # ex) ET092DC4511133A10
        try:
            systemnamelist = self._enum_eternus_instances(
                'FUJITSU_StorageProduct')
        except:
            msg=(_('create_volume,'
                   'volume:%(volume)s,'
                   'EnumerateInstances,'
                   'cannot connect to ETERNUS.')
                  % {'volume':volume})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug(_('*****create_volume,'
                    'volumename:%(volumename)s,'
                    'Return code:%(rc)lu,'
                    'Error:%(errordesc)s,'
                    'Backend:%(backend)s,'
                    'Pool Name:%(eternus_pool)s,'
                    'Pool Type:%(pooltype)s,'
                    'Leaving create_volume')
                   % {'volumename': volumename,
                      'rc': rc,
                      'errordesc':errordesc,
                      'backend':systemnamelist[0]['IdentifyingNumber'],
                      'eternus_pool':eternus_pool,
                      'pooltype':POOL_TYPE_dic[pooltype]})

        # create return value
        if element is not None:
            element_path['classname']   = element.classname
            element_path['keybindings'] = {'CreationClassName':element['CreationClassName'],
                                           'SystemName':element['SystemName'],
                                           'DeviceID':element['DeviceID'],
                                           'SystemCreationClassName':element['SystemCreationClassName']}

            volume_no = "0x" + element['DeviceID'][24:28]
        else:
            vol_instance = self._find_lun(volume)
            element_path['classname']   = vol_instance.classname
            element_path['keybindings'] = {'CreationClassName':vol_instance['CreationClassName'],
                                           'SystemName':vol_instance['SystemName'],
                                           'DeviceID':vol_instance['DeviceID'],
                                           'SystemCreationClassName':vol_instance['SystemCreationClassName']}
            volume_no = "0x" + vol_instance['DeviceID'][24:28]
        # end of if

        metadata= {'FJ_Backend':systemnamelist[0]['IdentifyingNumber'],
                   'FJ_Volume_Name':volumename,
                   'FJ_Volume_No':volume_no,
                   'FJ_Pool_Name':eternus_pool,
                   'FJ_Pool_Type':POOL_TYPE_dic[pooltype]}

        return (element_path, metadata)

    #----------------------------------------------------------------------------------------------#
    # Method : create_volume_from_snapshot                                                         #
    #         summary      : create volume from snapshot                                           #
    #         return-value : volume metadata                                                       #
    #----------------------------------------------------------------------------------------------#
    @FJDXLockutils('vol', 'cinder-', True)
    def create_volume_from_snapshot(self, volume, snapshot):
        '''
        Creates a volume from a snapshot
        '''
        # sysnames                   : FUJITSU_SrogareProduct
        # backend                    : ETERNUS model
        # snapshotname               : snapshotname on OpenStack
        # t_volumename               : target volumename on ETERNUS
        # s_volumename               : source volumename on ETERNUS
        # eternus_pool               : poolname
        # pool                       : pool instance
        # pooltype                   : RAID(2) or TPP(5)
        # configservice              : FUJITSU_StorageConfigurationService
        # source_volume_instance     : snapshot instance
        # target_volume_instance     : target volume instance
        # target_volume_instancename : target volume instance name
        # msg                        : message
        # rc                         : result of invoke method
        # errordesc                  : error message
        # job                        : job information
        # element_path               : element path
        # metadata                   : additional metadata

        LOG.debug(_('*****create_volume_from_snapshot,Enter method'))

        # initialize
        systemnamelist             = None
        systemname                 = None
        snapshotname               = None
        t_volumename               = None
        s_volumename               = None
        eternus_pool               = None
        pool                       = None
        pooltype                   = 0
        repservice                 = None
        source_volume_instance     = None
        target_volume_instance     = None
        target_volume_instancename = None
        msg                        = None
        rc                         = 0
        errordesc                  = None
        job                        = None
        element_path               = {}
        metadata                   = {}

        # main processing
        snapshotname           = snapshot['name']
        t_volumename           = self._create_volume_name(volume['id'])
        s_volumename           = self._create_volume_name(snapshot['id'])
        self.conn              = self._get_eternus_connection()
        source_volume_instance = self._find_lun(snapshot)

        # Existence check the source volume
        if source_volume_instance is None:
            msg=(_('create_volume_from_snapshot,'
                   'Source Volume is not exist in ETERNUS.'))
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        # create volume for destination of cloned volume
        (element_path, metadata)   = self.create_volume(volume)
        target_volume_instancename = self._create_volume_instance_name(element_path['classname'], element_path['keybindings'])
        target_volume_instance     = self._get_eternus_instance(target_volume_instancename)

        # get eternus model for metadata
        # systemname = systemnamelist[0]['IdentifyingNumber']
        # ex) ET092DC4511133A10
        try:
            systemnamelist = self._enum_eternus_instances(
                'FUJITSU_StorageProduct')
        except:
            msg=(_('create_volume_from_snapshot,'
                   'volume:%(volume)s,'
                   'EnumerateInstances,'
                   'cannot connect to ETERNUS.')
                  % {'volume':volume})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        systemname = systemnamelist[0]['IdentifyingNumber']

        LOG.debug(_('*****create_volume_from_snapshot,'
                    'volumename:%(volumename)s,'
                    'snapshotname:%(snapshotname)s,'
                    'source volume instance:%(source_volume_instance)s,')
                   % {'volumename': t_volumename,
                      'snapshotname': snapshotname,
                      'source_volume_instance': str(source_volume_instance.path)})

        # get repservice for CreateElementReplica
        repservice = self._find_eternus_service(REPL)

        if repservice is None:
            msg = (_('create_volume_from_snapshot,'
                     'Replication Service not found'))
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        # get poolname from driver configuration file
        eternus_pool = self._get_drvcfg('EternusPool')
        # Existence check the pool
        pool = self._find_pool(eternus_pool)
        if pool is None:
            msg = (_('create_volume_from_snapshot,'
                     'eternus_pool:%(eternus_pool)s,'
                     'not found.')
                    % {'eternus_pool': eternus_pool})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        # set pooltype
        if 'RSP' in pool['InstanceID']:
            pooltype = RAIDGROUP
        else:
            pooltype = TPPOOL
        # end of if

        # Invoke method for create cloned volume from snapshot
        rc, errordesc, job = self._exec_eternus_service(
            'CreateElementReplica',
            repservice,
            TargetPool=pool,
            SyncType=pywbem.Uint16(8),
            SourceElement=source_volume_instance.path,
            TargetElement=target_volume_instance.path)

        if rc != 0L and rc != 4096L:
            msg = (_('create_volume_from_snapshot,'
                     'volumename:%(volumename)s,'
                     'snapshotname:%(snapshotname)s,'
                     'Return code:%(rc)lu,'
                     'Error:%(errordesc)s')
                    % {'volumename': t_volumename,
                       'snapshotname': snapshotname,
                       'rc': rc,
                       'errordesc':errordesc})
            LOG.error(msg)
            if rc == 5 and str(systemname[4]) == '2':
                msg = (_('create_volume_from_snapshot,'
                         'NOT supported on DX S2[%(backend)s].')
                        % {'backend':systemname})
                LOG.error(msg)
            # end of if
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        LOG.debug(_('*****create_volume_from_snapshot,Exit method'))

        return (element_path, metadata)

    #----------------------------------------------------------------------------------------------#
    # Method : create_cloned_volume                                                                #
    #         summary      : create cloned volume on ETERNUS                                       #
    #         return-value : volume meta data                                                      #
    #----------------------------------------------------------------------------------------------#
    def create_cloned_volume(self, volume, src_vref, CloneOnly=False, use_service_name=False):
        '''
        Create clone of the specified volume.
        '''
        # source_volume_instance     : source volume instance
        # target_volume_instance     : target volume instance
        # target_volume_instancename : target volume instance name
        # element_path               : element path
        # metadata                   : additional metadata

        LOG.debug(_('*****create_cloned_volume,Enter method'))

        # initialize
        source_volume_instance     = None
        target_volume_instancename = None
        target_volume_instance     = None
        element_path               = {}
        metadata                   = {}

        # main processing
        source_volume_instance = self._find_lun(src_vref, use_service_name)

        if source_volume_instance is None:
            msg=(_('create_cloned_volume,'
                   'Source Volume is not exist in ETERNUS.'))
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        if CloneOnly is False:
            (element_path, metadata)   = self.create_volume(volume)
            target_volume_instancename = self._create_volume_instance_name(element_path['classname'], element_path['keybindings'])
            target_volume_instance     = self._get_eternus_instance(target_volume_instancename)
        else:
            target_volume_instance = self._find_lun(volume)
            try:
                element_path = eval(volume['provider_location'])
                metadata     = volume['volume_metadata']
            except:
                element_path = None
                metadata     = None
        # end of if

        remote_copy_type = self._get_metadata(volume).get(FJ_REMOTE_DEST_META, None)
        if remote_copy_type is None:
            self._create_local_cloned_volume(volume, target_volume_instance, 
                                       src_vref, source_volume_instance)
        else:
            source_volume_metadata = self._get_metadata(src_vref)
            self._create_remote_cloned_volume(volume, metadata, src_vref, source_volume_metadata, remote_copy_type)
        # end of if

        LOG.debug(_('*****create_cloned_volume,Exit method'))

        return (element_path, metadata)

    @FJDXLockutils('vol', 'cinder-', True)
    def _create_local_cloned_volume(self, volume, target_volume_instance, src_vref, source_volume_instance):
        '''
        Create local clone of the specified volume.
        '''
        # sysnames                   : FUJITSU_SrogareProduct
        # t_volumename               : target volumename on ETERNUS
        # s_volumename               : source volumename on ETERNUS
        # eternus_pool               : poolname
        # pool                       : pool instance
        # pooltype                   : RAID(2) or TPP(5)
        # repservice                 : FUJITSU_ReplicationService
        # msg                        : message
        # rc                         : result of invoke method
        # errordesc                  : error message
        # job                        : job information
        # element_path               : element path
        # metadata                   : additional metadata

        LOG.debug(_('*****_create_local_cloned_volume,Enter method'))

        # initialize
        systemnamelist             = None
        systemname                 = None
        t_volumename               = None
        s_volumename               = None
        eternus_pool               = None
        pool                       = None
        pooltype                   = 0
        repservice                 = None
        target_volume_instancename = None
        msg                        = None
        rc                         = 0
        errordesc                  = None
        job                        = None

        # main processing

        self.conn    = self._get_eternus_connection()
        s_volumename = self._create_volume_name(src_vref['id'])
        t_volumename = self._create_volume_name(volume['id'])

        # get eternus model for metadata
        # systemname = systemnamelist[0]['IdentifyingNumber']
        # ex) ET092DC4511133A10
        try:
            systemnamelist = self._enum_eternus_instances(
                'FUJITSU_StorageProduct')
        except:
            msg=(_('create_cloned_volume,'
                   'volume:%(volume)s,'
                   'EnumerateInstances,'
                   'cannot connect to ETERNUS.')
                  % {'volume':volume})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        systemname = systemnamelist[0]['IdentifyingNumber']

        LOG.debug(_('*****create_cloned_volume,'
                    'volumename:%(volumename)s,'
                    'sourcevolumename:%(sourcevolumename)s,'
                    'source volume instance:%(source_volume_instance)s,')
                   % {'volumename': t_volumename,
                      'sourcevolumename': s_volumename,
                      'source_volume_instance': str(source_volume_instance.path)})

        # get replicationservice for CreateElementReplica
        repservice = self._find_eternus_service(REPL)

        if repservice is None:
            msg = (_('create_cloned_volume,'
                     'Replication Service not found'))
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        # get poolname from driver configuration file
        eternus_pool = self._get_drvcfg('EternusPool')
        # Existence check the pool
        pool = self._find_pool(eternus_pool)
        if pool is None:
            msg = (_('create_cloned_volume,'
                     'eternus_pool:%(eternus_pool)s,'
                     'not found.')
                    % {'eternus_pool': eternus_pool})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        # set pooltype
        if 'RSP' in pool['InstanceID']:
            pooltype = RAIDGROUP
        else:
            pooltype = TPPOOL
        # end of if

        # Invoke method for create cloned volume from volume
        rc, errordesc, job = self._exec_eternus_service(
            'CreateElementReplica',
            repservice,
            TargetPool=pool,
            SyncType=pywbem.Uint16(8),
            SourceElement=source_volume_instance.path,
            TargetElement=target_volume_instance.path)

        if rc != 0L and rc != 4096L:
            msg = (_('create_cloned_volume,'
                     'volumename:%(volumename)s,'
                     'sourcevolumename:%(sourcevolumename)s,'
                     'source volume instance:%(source_volume_instance)s,'
                     'target volume instance:%(target_volume_instance)s,'
                     'Return code:%(rc)lu,'
                     'Error:%(errordesc)s')
                    % {'volumename': t_volumename,
                       'sourcevolumename': s_volumename,
                       'source_volume_instance': str(source_volume_instance.path),
                       'target_volume_instance': str(target_volume_instance.path),
                       'rc': rc,
                       'errordesc':errordesc})
            LOG.error(msg)
            if rc == 5 and str(systemname[4]) == '2':
                msg = (_('create_cloned_volume,'
                         'NOT supported on DX S2[%(backend)s].')
                        % {'backend':systemname})
                LOG.error(msg)
            # end of if
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        LOG.debug(_('*****_create_local_cloned_volume,Exit method'))
        return

    #----------------------------------------------------------------------------------------------#
    # Method : _create_remote_cloned_volume                                                        #
    #         summary      : create remote cloned volume on ETERNUS                                #
    #         return-value :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def _create_remote_cloned_volume(self, volume, volume_metadata, src_vref, src_vref_metadata, remote_copy_type):
        '''
        Create remote clone of the specified volume.
        '''
        LOG.debug(_('*****_create_remote_cloned_volume,Enter method'))
        # execute ccm script
        self._exec_ccm_script("start", source=src_vref_metadata, source_volume=src_vref, 
                                target=volume_metadata, copy_type=remote_copy_type)

        # update metadata of source volume
        lockname = 'ETERNUS_DX-db-update-' + src_vref['id']
        @lockutils.synchronized(lockname, 'cinder-', True)
        def __update_metadata():
            ctxt = context.get_admin_context()
            newest_src_vref = db.volume_get(ctxt, src_vref['id'])
            source_volume_metadata = self._get_metadata(newest_src_vref)
            source_volume_copy_list = eval(source_volume_metadata.get(FJ_REMOTE_SRC_META, "{}"))
            source_volume_copy_list[volume['id']] = remote_copy_type
            db.volume_metadata_update(ctxt.elevated(), src_vref['id'], 
                      {FJ_REMOTE_SRC_META:six.text_type(source_volume_copy_list)}, False)
        # end of def
        
        __update_metadata()
        LOG.debug(_('*****_create_remote_cloned_volume,Exit method'))
        return

    #----------------------------------------------------------------------------------------------#
    # Method : delete_volume                                                                       #
    #         summary      : delete volume on ETERNUS                                              #
    #         return-value : none                                                                  #
    #----------------------------------------------------------------------------------------------#
    def delete_volume(self, volume):
        '''
        Delete volume on ETERNUS.
        '''
        # valid        : volume is valid or invalid (e.g. volume is not exist on ETERNUS..etc)
        # vol_instance : volume instance
        # metadata     : metadata
        # with_format  : flag showing whether volume should be formatted or not when deleted

        LOG.debug(_('*****delete_volume,Enter method'))

        # initialize
        valid        = False
        vol_instance = None
        metadata     = {}
        with_format  = False

        # CCM processing
        metadata = self._get_metadata(volume)
        if (FJ_REMOTE_DEST_META in metadata) and ("FJ_Backend" in metadata):
            self._exec_ccm_script("stop", target=metadata)
        # end of if

        if FJ_REMOTE_SRC_META in metadata:
            self._exec_ccm_script("stop", source=metadata, source_volume=volume)
            copy_list = eval(metadata.get(FJ_REMOTE_SRC_META))

            ctxt = context.get_admin_context()
            for target_volume_id in copy_list.keys():
                try:
                    db.volume_metadata_update(ctxt.elevated(), target_volume_id, {FJ_REMOTE_DEST_META:None}, False)
                except:
                    pass

        # end of if

        # main preprocessing
        valid = self._delete_volume_setting(volume)

        if valid is False:
            return
        # end of if

        vol_instance = self._find_lun(volume)
        if vol_instance is None:
            return

        with_format = self._get_extra_specs(volume, key=FJ_VOL_FORMAT_KEY, default='False')
        if self._get_bool(with_format) is True:
            eternus_pool = self._get_drvcfg('EternusPool')
            pool = self._find_pool(eternus_pool)
            if pool is None:
                msg = (_('delete_volume,'
                         'eternus_pool:%(eternus_pool)s,'
                         'not found.')
                        % {'eternus_pool': eternus_pool})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            # end of if

            if 'RSP' in pool['InstanceID']: # pooltype == RAIDGROUP
                self._format_standard_volume(volume)
            else:                           # pooltype == TPPOOL
                self._format_tpv(volume)
            # end of if

        self._delete_volume(vol_instance)

        LOG.debug(_('*****delete_volume,Exit method'))
        return 

    #----------------------------------------------------------------------------------------------#
    # Method : _delete_volume_setting                                                              #
    #         summary      : Delete volume setting ( HostAffinity, CopySession) on ETERNUS         #
    #         return-value : volume is valid or invalid                                            #
    #----------------------------------------------------------------------------------------------#
    @FJDXLockutils('vol', 'cinder-', True)
    def _delete_volume_setting(self, volume):
        '''
        Delete volume setting ( HostAffinity, CopySession) on ETERNUS.
        '''
        # volumename   : volumename on ETERNUS
        # vol_instance : volume instance
        # cpsession    : copy session instance
        # msg          : message

        LOG.debug(_('*****_delete_volume_setting,Enter method'))

        # initialize
        volumename    = None
        vol_instance  = None
        cpsession     = None
        msg           = None

        # main preprocessing
        # Existence check the volume
        volumename   = self._create_volume_name(volume['id'])
        vol_instance = self._find_lun(volume)

        if vol_instance is None:
            LOG.debug(_('*****_delete_volume_setting,volumename:%(volumename)s,'
                        'volume not found on ETERNUS.'
                        'delete only management data on cinder database.')
                      % {'volumename': volumename})
            return False
        # end of if

        # delete host-affinity setting remained by unexpected error 
        self._unmap_lun(volume, None, force=True)

        # stop the copysession.
        cpsession = self._find_copysession(vol_instance)
        if cpsession is not None:
            LOG.debug(_('*****_delete_volume_setting,volumename:%(volumename)s,'
                        'volume is using by copysession[%(cpsession)s].delete copysession.')
                       % {'volumename': volumename,
                         'cpsession': cpsession})
            self._delete_copysession(cpsession)
        # end of if

        LOG.debug(_('*****_delete_volume_setting,Exit method'))
        return True

    #----------------------------------------------------------------------------------------------#
    # Method : _format_standard_volume                                                             #
    #         summary      : format standard volume                                                #
    #         return-value : none                                                                  #
    #----------------------------------------------------------------------------------------------#
    def _format_standard_volume(self, volume):
        '''
        Format standard volume
        '''
        # wait_sec : the time to wait for next check for format-completion
        # wait_cnt : the number of wait

        LOG.debug(_('*****_format_standard_volume,Enter method'))

        # initialize
        wait_sec = 60 # 1min
        wait_cnt = 0
 
        # main processing
        volumename = self._create_volume_name(volume['id'])
        param_dict = {'volume-name': volumename}
        
        rc, errordesc, job = self._exec_eternus_cli(
                                'format_volume',
                                **param_dict)

        if rc != 0L:
            msg = (_('_format_standard_volume,'
                     'volumename:%(volumename)s,'
                     'Return code:%(rc)lu,'
                     'Error:%(errordesc)s,'
                     'Job:%(job)s')
                    % {'volumename': volumename,
                       'rc': rc,
                       'errordesc':errordesc,
                       'job': job})

            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        while True:
            time.sleep(wait_sec)
            rc, errordesc, job = self._exec_eternus_cli(
                                    "show_volume_progress",
                                    **param_dict)

            if rc != 0L:
                msg = (_('_format_standard_volume,'
                         'show volume progress error,'
                         'volumename:%(volumename)s,'
                         'Return code:%(rc)lu,'
                         'Error:%(errordesc)s,'
                         'Job:%(job)s')
                        % {'volumename': volumename,
                           'rc': rc,
                           'errordesc':errordesc,
                           'job': job})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            # end of if

            wait_sec = int(job)

            if wait_sec == 0:
                LOG.debug(_('*****_format_standard_volume,'
                            'format complete'))
                break
            elif wait_cnt < 3:
                wait_sec /= 3
                wait_cnt += 1
            # end of if

            LOG.debug(_('*****_format_standard_volume,'
                        'format remain time : %s sec') % job)
            # end of if
        # end of while

        LOG.debug(_('*****_format_standard_volume,Exit method'))
        return

    #----------------------------------------------------------------------------------------------#
    # Method : _format_tpv                                                                         #
    #         summary      : format tpv                                                            #
    #         return-value : none                                                                  #
    #----------------------------------------------------------------------------------------------#
    def _format_tpv(self, volume, poolname=None):
        '''
        Format tpv
        '''
        # volumename : volumename on ETERNUS
        # poolname   : pool name on ETERNUS
        # lockname   : lock name (for OSVD Lock)

        LOG.debug(_('*****_format_tpv,Enter method'))

        volumename = self._create_volume_name(volume['id'])

        if poolname is None:
            poolname        = self._get_drvcfg('EternusPool')
        # end of if

        lockname = 'ETERNUS_DX-format-' + poolname + '-' + self._get_drvcfg('EternusIP').replace('.','_')

        @lockutils.synchronized(lockname, 'cinder-', True)
        def __format_tpv(volumename, poolname):
            # initialize
            wait_sec = 60 # 1min
            wait_cnt = 0

            # main processing
            format_param_dict = {'volume-name': volumename}

            rc, errordesc, job = self._exec_eternus_cli(
                                    'format_tpv',
                                    **format_param_dict)

            if rc != 0L:
                msg = (_('_format_tpv,'
                         'volumename:%(volumename)s,'
                         'Return code:%(rc)lu,'
                         'Error:%(errordesc)s,'
                         'Job:%(job)s')
                        % {'volumename': volumename,
                           'rc': rc,
                           'errordesc':errordesc,
                           'job': job})

                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            # end of if

            show_param_dict = {'pool-name': poolname}
            while True:
                time.sleep(wait_sec)
                rc, errordesc, job = self._exec_eternus_cli(
                                        "show_tpv_progress",
                                        **show_param_dict)

                if rc != 0L:
                    msg = (_('_format_tpv,'
                             'show volume progress error,'
                             'volumename:%(volumename)s,'
                             'Return code:%(rc)lu,'
                             'Error:%(errordesc)s,'
                             'Job:%(job)s')
                            % {'volumename': volumename,
                               'rc': rc,
                               'errordesc':errordesc,
                               'job': job})
                    LOG.error(msg)
                    raise exception.VolumeBackendAPIException(data=msg)
                # end of if

                wait_sec = int(job)

                if wait_sec == 0:
                    LOG.debug(_('*****_format_tpv,'
                                'format complete'))
                    break
                elif wait_cnt < 3:
                    wait_sec /= 3
                    wait_cnt += 1
                # end of if

                LOG.debug(_('*****_format_tpv,'
                            'format wait : %s sec') % job)
                # end of if
            # end of while
        # end of def
        __format_tpv(volumename, poolname)

        LOG.debug(_('*****_format_tpv,Exit method'))
        return


    #----------------------------------------------------------------------------------------------#
    # Method : _delete_volume                                                                      #
    #         summary      : delete volume on ETERNUS                                              #
    #         return-value : none                                                                  #
    #----------------------------------------------------------------------------------------------#
    @FJDXLockutils('vol', 'cinder-', True)
    def _delete_volume(self, vol_instance):
        '''
        Delete volume on ETERNUS.
        '''
        # volumename   : volumename on ETERNUS
        # cpsession    : copy session instance
        # configservice: FUJITSU_StorageConfigurationService
        # msg          : message
        # rc           : result of invoke method
        # errordesc    : error message
        # job          : unused

        LOG.debug(_('*****_delete_volume,Enter method'))

        # initialize
        volumename    = None
        cpsession     = None
        configservice = None
        msg           = None
        rc            = 0
        errordesc     = None
        job           = None


        # main preprocessing
        self.conn  = self._get_eternus_connection()
        volumename = vol_instance['ElementName']

        configservice = self._find_eternus_service(STOR_CONF)
        if configservice is None:
            msg = (_('delete_volume,volumename:%(volumename)s,'
                     'Storage Configuration Service not found.')
                    % {'volumename': volumename})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        LOG.debug(_('*****delete_volume,volumename:%(volumename)s,'
                    'vol_instance:%(vol_instance)s,'
                    'Method: ReturnToStoragePool')
                   % {'volumename': volumename,
                      'vol_instance': str(vol_instance.path)})

        # Invoke method for delete volume
        rc, errordesc, job = self._exec_eternus_service(
            'ReturnToStoragePool',
            configservice,
            TheElement=vol_instance.path)

        if rc != 0L and rc != 4096L:
            msg = (_('delete_volume,volumename:%(volumename)s,'
                     'Return code:%(rc)lu,'
                     'Error:%(errordesc)s')
                    % {'volumename': volumename,
                       'rc': rc,
                       'errordesc': errordesc})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        LOG.debug(_('*****delete_volume,volumename:%(volumename)s,'
                    'Return code:%(rc)lu,'
                    'Error:%(errordesc)s,'
                    'Exit Method')
                  % {'volumename': volumename,
                     'rc': rc,
                     'errordesc': errordesc})


        LOG.debug(_('*****_delete_volume,Exit method'))
        return

    #----------------------------------------------------------------------------------------------#
    # Method : create_snapshot                                                                     #
    #         summary      : create snapshot using SnapOPC                                         #
    #         return-value : none                                                                  #
    #----------------------------------------------------------------------------------------------#
    @FJDXLockutils('vol', 'cinder-', True)
    def create_snapshot(self, snapshot):
        '''
        create snapshot using SnapOPC
        '''
        # snapshotname  : snapshot name on Openstack
        # volumename    : source volume name on Openstack
        # vol_id        : source volume id
        # volume        : source volume dictionary
        # vol_instance  : source volume instance
        # s_volumename  : source volume name on ETERNUS
        # d_volumename  : destination volume name on ETERNUS
        # eternus_pool  : poolname
        # pool          : pool instance
        # configservice : FUJITSU_StorageConfigurationService
        # msg           : message
        # rc            : result of invoke method
        # errordesc     : error message
        # job           : job information
        # element       : element (including ETERNUS SMI-S class information)
        # element_path  : element path

        LOG.debug(_('*****create_snapshot,Enter method'))

        # initialize
        snapshotname  = None
        volumename    = None
        vol_id        = None
        volume        = None
        vol_instance  = None
        d_volumename  = None
        s_volumename  = None
        vol_instance  = None
        configservice = None
        eternus_pool  = None
        pool          = None
        msg           = None
        rc            = 0
        errordesc     = None
        job           = None
        element       = None
        element_path  = {}

        # main processing
        snapshotname  = snapshot['name']
        volumename    = snapshot['volume_name']
        vol_id        = snapshot['volume_id']
        volume        = snapshot['volume']
        d_volumename  = self._create_volume_name(snapshot['id'])
        s_volumename  = self._create_volume_name(vol_id)
        vol_instance  = self._find_lun(volume)
        configservice = self._find_eternus_service(STOR_CONF)
        self.conn     = self._get_eternus_connection()

        # Existence check the volume
        if vol_instance is None:
            # source volume is not found on ETERNUS
            msg = (_('create_snapshot,'
                     'volumename on ETERNUS:%(s_volumename)s,'
                     'source volume is not found on ETERNUS.')
                    % {'s_volumename': s_volumename})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        if configservice is None:
            msg = (_('create_snapshot,'
                     'volumename:%(volumename)s,'
                     'Storage Configuration Service not found.')
                    % {'volumename': volumename})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        # get poolname from driver configuration file
        eternus_pool  = self._get_snap_pool_name()
        # Existence check the pool
        pool          = self._find_pool(eternus_pool)
        if pool is None:
            msg = (_('create_snapshot,'
                     'eternus_pool:%(eternus_pool)s,'
                     'not found.')
                    % {'eternus_pool': eternus_pool})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        LOG.debug(_('*****create_snapshot,'
                    'snapshotname:%(snapshotname)s,'
                    'source volume name:%(volumename)s,'
                    'vol_instance.path:%(vol_instance)s,'
                    'dest_volumename:%(d_volumename)s,'
                    'pool:%(pool)s,'
                    'Invoke CreateReplica')
                   % {'snapshotname': snapshotname,
                      'volumename': volumename,
                      'vol_instance': str(vol_instance.path),
                      'd_volumename': d_volumename,
                      'pool': pool})

        # Invoke method for create snapshot
        rc, errordesc, job = self._exec_eternus_service(
            'CreateReplica',
            configservice,
            ElementName=d_volumename,
            TargetPool=pool,
            CopyType=pywbem.Uint16(4),
            SourceElement=vol_instance.path)

        if rc != 0L and rc != 4096L:
            msg = (_('create_snapshot,'
                     'snapshotname:%(snapshotname)s,'
                     'source volume name:%(volumename)s,'
                     'vol_instance.path:%(vol_instance)s,'
                     'dest_volumename:%(d_volumename)s,'
                     'pool:%(pool)s,'
                     'Return code:%(rc)lu,'
                     'Error:%(errordesc)s')
                    % {'snapshotname': snapshotname,
                       'volumename': volumename,
                       'vol_instance': str(vol_instance.path),
                       'd_volumename': d_volumename,
                       'pool': pool,
                       'rc': rc,
                       'errordesc':errordesc})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        else:
            element = job['TargetElement']
        # end of if

        LOG.debug(_('*****create_snapshot,volumename:%(volumename)s,'
                    'Return code:%(rc)lu,'
                    'Error:%(errordesc)s,'
                    'Exit Method')
                  % {'volumename': volumename,
                     'rc': rc,
                     'errordesc': errordesc})

        # create return value
        if element is not None:
            element_path['classname']   = element.classname
            element_path['keybindings'] = {'CreationClassName':element['CreationClassName'],
                                                'SystemName':element['SystemName'],
                                                'DeviceID':element['DeviceID'],
                                                'SystemCreationClassName':element['SystemCreationClassName']}
        # end of if

        return element_path

    #----------------------------------------------------------------------------------------------#
    # Method : delete_snapshot                                                                     #
    #         summary      : delete snapshot                                                       #
    #         return-value : none                                                                  #
    #----------------------------------------------------------------------------------------------#
    def delete_snapshot(self, snapshot):
        '''
        delete snapshot
        '''
        # Delete snapshot - deletes the target element and the snap session
        # snapshotname  : snapshot display name
        # volumename    : source volume display name on openstack
        # d_volumename  : destination volume name on ETERNUS
        # snapshot['id']: destination volume id for _find_lun
        # cpsession     : Storage Synchronized
        # repservice    : FUJITSU_ReplicationService
        # msg           : message
        # rc            : result of invoke method
        # errordesc     : error message

        LOG.debug(_('*****delete_snapshot,Enter method'))

        # initialize

        # main processing

        self.delete_volume(snapshot)

        LOG.debug(_('*****delete_snapshot,Exit method'))
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
        # targetlist    : target port id list
        # mapdata       : the element which constitutes device_info
        # msg           : message
        # device_number : device number for target lun
        # device_info   : return value
        # metadata      : additional metadata
        # portidlist    : target port id list (for CLI)

        LOG.debug(_('*****initialize_connection,Enter method'))

        # initialize
        targetlist    = []
        mapdata       = {}
        msg           = None
        device_number = {}
        device_info   = {}
        portidlist    = []

        # CCM processing
        metadata = self._get_metadata(volume)
        if metadata.get(FJ_REMOTE_DEST_META, None) == FJ_REC_MIRROR:
            if volume['status'] == "in-use":
                msg = (_('Live migration for backup volume is not allowed'))
                self.invalid_migration_list.append(volume['id'])
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            # end of if

            self._exec_ccm_script("suspend", target=metadata)
        # end of if

        # main processing
        try:
            self.conn              = self._get_eternus_connection()
            targetlist, portidlist = self._get_target_portid(connector)
            device_number          = self._find_device_number(volume, connector)

            if device_number is not None:
                # volume is already mapped
                msg = (_('initialize_connection,'
                         'volume:%(volume)s,'
                         'target_lun:%(target_lun)s,'
                         'Volume is already mapped.')
                        % {'volume':volume['name'],
                           'target_lun':device_number})
                LOG.info(msg)
            else:
                self._map_lun(volume, connector, portidlist)
                device_number = self._find_device_number(volume, connector)
            # end of if

            mapdata                     = self._get_mapdata(device_number, connector, targetlist)
            mapdata['target_discoverd'] = True
            mapdata['volume_id']        = volume['id']

            if self.protocol == 'iSCSI':
                device_info = {'driver_volume_type': 'iscsi',
                               'data': mapdata}
            elif self.protocol == 'fc':
                device_info = {'driver_volume_type': 'fibre_channel',
                               'data': mapdata}
            # end of if

            LOG.debug(_('*****initialize_connection,'
                        'device_info:%(info)s,'
                        'Exit method')
                      % {'info': device_info})
        except Exception as ex:
            # when volume is set to REC Mirror, resume the session
            if metadata.get(FJ_REMOTE_DEST_META, None) == FJ_REC_MIRROR:
                self._exec_ccm_script("resume", target=metadata)
                raise ex
            # end of if

        return device_info

    #----------------------------------------------------------------------------------------------#
    # Method : terminate_connection                                                                #
    #         summary      : remove HostAffinityGroup on ETERNUS                                   #
    #         return-value :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def terminate_connection(self, volume, connector, force=False, **kwargs):
        '''
        Disallow connection from connector
        '''
        LOG.debug(_('*****terminate_connection,Enter method'))

        # main processing
        if volume['id'] in self.invalid_migration_list:
            msg = (_('Live migration for backup volume is not allowed, Terminate Connection Skip'))
            self.invalid_migration_list.remove(volume['id'])
            LOG.info(msg)
            return
        # end of if

        self.conn = self._get_eternus_connection()
        self._unmap_lun(volume, connector)

        # CCM processing
        metadata = self._get_metadata(volume)
        if metadata.get(FJ_REMOTE_DEST_META, None) == FJ_REC_MIRROR:
            self._exec_ccm_script("resume", target=metadata)
        # end of if

        LOG.debug(_('*****terminate_connection,Exit method'))
        return

    #----------------------------------------------------------------------------------------------#
    # Method : extend_volume                                                                       #
    #         summary      : extend volume on ETERNUS                                              #
    #         return-value :                                                                       #
    #----------------------------------------------------------------------------------------------#
    @FJDXLockutils('vol', 'cinder-', True)
    def extend_volume(self, volume, new_size):
        '''
        extend volume on ETERNUS
        '''
        # volumesize              : volume new size Byte
        # volumename              : volumename on ETERNUS
        # source_volume_instance  : source volume instance
        # eternus_pool            : poolname
        # pool                    : pool instance
        # pooltype                : RAID(2) or TPP(5)
        # configservice           : FUJITSU_StorageConfigurationService
        # msg                     : message
        # rc                      : result of invoke method
        # errordesc               : error message
        # job                     : unused

        LOG.debug(_('*****extend_volume,Enter method'))

        # initialize
        systemnamelist         = None
        volumesize             = 0
        volumename             = None
        source_volume_instance = None
        eternus_pool           = None
        pool                   = None
        pooltype               = 0
        configservice          = None
        msg                    = None
        rc                     = 0
        errordesc              = None
        job                    = None

        #main processing
        #conversion of the unit. GB to B
        volumesize = new_size * 1073741824
        #create to volumename on ETERNUS from cinder VolumeID
        volumename = self._create_volume_name(volume['id'])
        #get source volume instance
        source_volume_instance = self._find_lun(volume)
        if source_volume_instance is None:
            msg = (_('extend_volume,'
                     'source_volume:%(volumename)s,'
                     'not found.')
                    % {'volumename': volumename})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        LOG.debug(_('*****extend_volume,volumename:%(volumename)s,'
                    'volumesize:%(volumesize)u,'
                    'source volume instance:%(source_volume_instance)s,'
                  % {'volumename': volumename,
                     'volumesize': volumesize,
                     'source_volume_instance': str(source_volume_instance.path)}))

        self.conn = self._get_eternus_connection()

        # get poolname from driver configuration file
        eternus_pool = self._get_drvcfg('EternusPool')
        # Existence check the pool
        pool = self._find_pool(eternus_pool)
        if pool is None:
            msg = (_('extend_volume,'
                     'eternus_pool:%(eternus_pool)s,'
                     'not found.')
                    % {'eternus_pool': eternus_pool})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        # set pooltype
        if 'RSP' in pool['InstanceID']:
            pooltype = RAIDGROUP
        else:
            pooltype = TPPOOL
        # end of if

        if pooltype == RAIDGROUP:
            extend_size = str(new_size - volume['size']) + 'gb'
            param_dict = {'volume-name': volumename,
                          'rg-name':eternus_pool,
                          'size': extend_size}
            rc, errordesc, job = self._exec_eternus_cli(
                            'expand_volume',
                            **param_dict)

        else: # pooltype == TPPOOL
            configservice = self._find_eternus_service(STOR_CONF)
            if configservice is None:
                msg = (_('extend_volume,volume:%(volume)s,'
                         'volumename:%(volumename)s,'
                         'eternus_pool:%(eternus_pool),'
                         'Error!! Storage Configuration Service is None.')
                        % {'volume':volume,
                           'volumename': volumename,
                           'eternus_pool':eternus_pool})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            # end of if
            LOG.debug(_('*****extend_volume,CreateOrModifyElementFromStoragePool,'
                        'ConfigService:%(service)s,'
                        'ElementName:%(volumename)s,'
                        'InPool:%(eternus_pool)s,'
                        'ElementType:%(pooltype)u,'
                        'Size:%(volumesize)u,'
                        'TheElement:%(source_volume_instance)s')
                       % {'service':configservice,
                          'volumename': volumename,
                          'eternus_pool':eternus_pool,
                          'pooltype':pooltype,
                          'volumesize': volumesize,
                          'source_volume_instance': str(source_volume_instance.path)})

            # Invoke method for extend volume
            rc, errordesc, job = self._exec_eternus_service(
                'CreateOrModifyElementFromStoragePool',
                configservice,
                ElementName=volumename,
                InPool=pool,
                ElementType=pywbem.Uint16(pooltype),
                Size=pywbem.Uint64(volumesize),
                TheElement=source_volume_instance.path)
        # end of if

        if rc != 0L and rc != 4096L:
            msg = (_('extend_volume,'
                     'volumename:%(volumename)s,'
                     'Return code:%(rc)lu,'
                     'Error:%(errordesc)s,'
                     'PoolType:%(pooltype)s')
                    % {'volumename': volumename,
                       'rc': rc,
                       'errordesc':errordesc,
                       'pooltype':POOL_TYPE_dic[pooltype]})

            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        LOG.debug(_('*****extend_volume,'
                    'volumename:%(volumename)s,'
                    'Return code:%(rc)lu,'
                    'Error:%(errordesc)s,'
                    'Pool Name:%(eternus_pool)s,'
                    'Pool Type:%(pooltype)s,'
                    'Leaving extend_volume')
                   % {'volumename': volumename,
                      'rc': rc,
                      'errordesc':errordesc,
                      'eternus_pool':eternus_pool,
                      'pooltype':POOL_TYPE_dic[pooltype]})
        return


    #----------------------------------------------------------------------------------------------#
    # Method : refresh_volume_stats                                                                #
    #         summary      : get pool capacity                                                     #
    #         return-value : self.stats                                                            #
    #----------------------------------------------------------------------------------------------#
    @FJDXLockutils('update', 'cinder-', True)
    def refresh_volume_stats(self):
        '''
        get pool capacity.
        '''
        # eternus_pool : poolname
        # pool         : pool instance
        # GB           : bytes which is equivalent to 1GB

        LOG.debug(_('*****refresh_volume_stats,Enter method'))

        # initialize
        eternus_pool = None
        pool         = None
        GB           = 1073741824

        # main processing
        self.conn    = self._get_eternus_connection()
        eternus_pool = self._get_drvcfg('EternusPool')
        pool         = self._find_pool(eternus_pool,True)

        # Existence check the pool
        if pool is None:
            msg = (_('refresh_volume_stats,'
                     'eternus_pool:%(eternus_pool)s,'
                     'not found.')
                    % {'eternus_pool': eternus_pool})
            LOG.info(msg)

            # create pool(RAID Group)
            self._create_pool(eternus_pool)
            pool = self._find_pool(eternus_pool,True)
            if pool is None:
                msg = (_('refresh_volume_stats,'
                         'eternus_pool:%(eternus_pool)s,'
                         'not found after create_pool.')
                        % {'eternus_pool': eternus_pool})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            # end of if
        # end of if

        self.stats['total_capacity_gb'] = pool['TotalManagedSpace'] / GB
        self.stats['free_capacity_gb']  = pool['RemainingManagedSpace'] / GB

        LOG.debug(_('*****refresh_volume_stats,'
                    'eternus_pool:%(eternus_pool)s,'
                    'total capacity[%(total)s],'
                    'free capacity[%(free)s]')
                   % {'eternus_pool':eternus_pool,
                      'total':self.stats['total_capacity_gb'],
                      'free':self.stats['free_capacity_gb']})

        return self.stats

    #----------------------------------------------------------------------------------------------#
    # Method : create_image_volume                                                                 #
    #         summary      : create image volume on ETERNUS                                        #
    #         return-value : volume information                                                    #
    #----------------------------------------------------------------------------------------------#
    def create_image_volume(self, volume_ref, image_id):
        '''
        create image volume
        '''
        # img_volume   : image volume
        # element_path : provider_location
        # element_path : element path
        # metadata     : metadata
        # storage_name : storage name (ip address assigned in maintenance port)

        LOG.debug(_('*****create_image_volume,Enter method'))

        # initialize
        img_volume   = {}
        element_path = {}
        storage_name = None

        # main processing
        img_volume['id']              ='image-' + uuid.uuid4().hex
        img_volume['display_name']    = volume_ref['display_name']
        img_volume['size']            = volume_ref['size']
        img_volume['volume_type_id']  = volume_ref['volume_type_id']
        img_volume['volume_metadata'] = []
        storage_name                  = self._get_drvcfg('EternusIP')

        LOG.info(_('create_image_volume, '
                   'image volume id:%(img_volid)s, '
                   'storage name:%(storage_name)s')
                   % {'img_volid':img_volume['id'],
                      'storage_name':storage_name})

        (element_path, metadata) = self.create_volume(img_volume)
        img_volume['provider_location'] = six.text_type(element_path)
        

        LOG.debug(_('*****create_image_volume,Exit method'))

        return img_volume

    #----------------------------------------------------------------------------------------------#
    # Method : add_image_volume                                                                    #
    #         summary      : add image volume on ETERNUS                                           #
    #         return-value :                                                                       #
    #----------------------------------------------------------------------------------------------#
    @lockutils.synchronized('ETERNUS_DX-img-update', 'cinder-', True)
    def add_image_volume(self, volume_ref, image_id):
        '''
        add image volume
        '''
        # storage_name : storage name (ip address assigned in maintenance port)
        # with_format  : flag showing whether volume should be formatted or not when deleted
        # use_format   : format volume or not in delete volume

        LOG.debug(_('*****add_image_volume,Enter method'))

        # initialize
        storage_name = None
        with_format  = False
        use_format   = False

        # main processing
        storage_name = self._get_drvcfg('EternusIP')

        with_format = self._get_extra_specs(volume_ref, key=FJ_VOL_FORMAT_KEY, default=False)
        use_format  = self._get_bool(with_format)

        LOG.debug(_('*****add_image_volume,'
                    'image_id:%(image_id)s,'
                    'volume_id:%(volume_id)s,'
                    'size:%(size)s,'
                    'storage_name:%(storage_name)s')
                    % {'image_id':image_id,
                       'volume_id':volume_ref['id'],
                       'size':volume_ref['size'],
                       'storage_name':storage_name})

        self._add_image_volume_info(volume_ref['id'], volume_ref['size'], volume_ref['provider_location'], image_id, storage_name, use_format)

        LOG.debug(_('*****add_image_volume,Exit method'))
        return

    #----------------------------------------------------------------------------------------------#
    # Method : delete_image_volume                                                                 #
    #         summary      : delete image volume on ETERNUS and management file                    #
    #         return-value :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def delete_image_volume(self, volume, image_id, format_volume=False, delete_volume=True):
        '''
        delete image volume
        '''
        LOG.debug(_('*****delete_image_volume,Enter method'))

        @lockutils.synchronized('ETERNUS_DX-img-update', 'cinder-', True)
        def delete_image_volume_prepare():
            '''
            set delete mark to image volume management file
            '''
            self._update_image_volume_info(volume['id'], image_id, value=DELETE_IMGVOL)

        @lockutils.synchronized('ETERNUS_DX-img-update', 'cinder-', True)
        def delete_image_volume_update():
            '''
            remove image volume info from image volume management file
            '''
            self._update_image_volume_info(volume['id'], image_id, remove=True)
        # end of def
            
        # main processing
        delete_image_volume_prepare()

        if delete_volume is True:
            if format_volume is True:
                vol_instance = self._find_lun(volume)

                try:
                    pool = self._assoc_eternus(
                                vol_instance.path,
                                AssocClass ='FUJITSU_AllocatedFromStoragePool',
                                ResultClass = 'CIM_StoragePool')[0]
                except:
                    msg=(_('delete_image_volume,'
                           'vol_instance.path:%(vol)s,')
                           %{'vol': vol_instance.path})

                    LOG.error(msg)
                    raise exception.VolumeBackendAPIException(data=msg)

                if 'RSP' in pool['InstanceID']:
                #pooltype = RAIDGROUP
                    self._format_standard_volume(volume)
                else:
                #pooltype = TPPOOL
                    self._format_tpv(volume, pool['ElementName'])
                # end of if
            # end of if

            self.delete_volume(volume)
        # end of if

        delete_image_volume_update()

        LOG.debug(_('*****delete_image_volume,Exit method'))
        return

    #----------------------------------------------------------------------------------------------#
    # Method : copy_image_volume_to_volume                                                         #
    #         summary      : copy image volume to volume                                           #
    #         return-value : cloned                                                                #
    #----------------------------------------------------------------------------------------------#
    @lockutils.synchronized('ETERNUS_DX-img-update', 'cinder-', True)
    def copy_image_volume_to_volume(self, volume, image_id, src_vref=None):
        '''
        copy image volume to volume
        '''
        # cloned                : whether clone from image volume done or not
        # image_management_file : management file name for image volume
        # src_volume_id         : source volume id (image volume id)
        # storage_name          : storage name (ip address assigned in maintenance port)

        LOG.debug(_('*****copy_image_volume_to_volume,Enter method'))

        # initialize
        cloned                = False
        image_management_file = None
        src_volume_id         = None
        storage_name          = None

        # get image volume path which meets new volume's condition
        if src_vref is None:
            image_management_file = self.configuration.fujitsu_image_management_file
            storage_name          = self._get_drvcfg('EternusIP')
            src_vref              = self._get_imgcfg(image_management_file, volume['size'], image_id, storage_name)

            if src_vref is not None:
                src_volume_id         = src_vref['id']

            LOG.debug(_('*****copy_image_volume_to_volume,'
                        'image_management_file_name:%(image_management_file)s,'
                        'src_volume_id:%(src_volume_id)s,'
                        'storage_name:%(storage_name)s')
                        % {'image_management_file':image_management_file,
                           'src_volume_id':src_volume_id,
                           'storage_name':storage_name})
        else:
            src_volume_id         = src_vref['id']
            LOG.debug(_('*****copy_image_volume_to_volume,'
                        'src_volume_id:%(src_volume_id)s')
                        % {'src_volume_id':src_volume_id})
        # end of if

        # copy image volume to volume using above image volume path
        if src_volume_id is not None:
            self.create_cloned_volume(volume, src_vref, CloneOnly=True)
            self._update_image_volume_info(src_volume_id, image_id)
            cloned = True
        # end of if

        LOG.debug(_('*****copy_image_volume_to_volume (cloned=%s),Exit method') % (cloned))
        return cloned

    #----------------------------------------------------------------------------------------------#
    # Method : monitor_image_volume                                                                #
    #         summary      : monitor image volume on ETERNUS periodically                          #
    #         return-value :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def monitor_image_volume(self):
        '''
        timer function for monitor image volume
        '''
        try:
            self._monitor_image_volume()
        except Exception as e:
            LOG.warn(_('monitor_image_volume, undefined error was occured (%s)') % str(e))

        timer = threading.Timer(MONITOR_IMGVOL_INTERVAL, self.monitor_image_volume)
        timer.start()
        return

    @lockutils.synchronized('ETERNUS_DX-img-monitor', 'cinder-', True)
    def _monitor_image_volume(self):
        '''
        monitor image volume
        '''
        # image_management_file  : management file name for image volume
        # nosession_volume_limit : limitation number of session per 1LUN
        # doc                    : xml document class
        # root                   : xml document root
        # storage_name           : storage name (ip address assigned in maintenance port)
        # f_image                : Image XML Information (Image ID, Image Volume Information)

        LOG.debug(_('*****monitor_image_volume, Enter method'))

        # initialize
        image_management_file  = None
        nosession_volume_limit = 0
        doc                    = None
        root                   = None
        storage_name           = None
        f_image                = None

        # main processing
        image_management_file  = self.configuration.fujitsu_image_management_file
        storage_name           = self._get_drvcfg('EternusIP')
        nosession_volume_limit = int(self.configuration.fujitsu_min_image_volume_per_storage)

        if os.path.exists(image_management_file):
            LOG.debug(_('*****monitor_image_volume,'
                        'image_management_file:%(image_management_file)s,'
                        'storage_name:%(storage_name)s,'
                        'nosession_volume_limit:%(nosession_volume_limit)s')
                        % {'image_management_file':image_management_file,
                           'storage_name':storage_name,
                           'nosession_volume_limit':nosession_volume_limit})
            doc     = xml.dom.minidom.parse(image_management_file)
            root    = doc.documentElement
            f_image = doc.getElementsByTagName('Image')

            for f_img in f_image:
                f_image_id = f_img.getElementsByTagName('ImageID')[0].childNodes[0].data
                f_volume   = f_img.getElementsByTagName('Volume')
                nosession_volume = 0
                for f_vol in f_volume:
                    f_storage_name = f_vol.getElementsByTagName('StorageName')[0].childNodes[0].data
                    if storage_name != f_storage_name:
                        continue
                    # end of if

                    f_volume_id = f_vol.getElementsByTagName('VolumeID')[0].childNodes[0].data

                    try:
                        f_volume_path = f_vol.getElementsByTagName('VolumePath')[0].childNodes[0].data
                    except:
                        f_volume_path = None
                        
                    volume       = {'id' : f_volume_id , 'provider_location' : f_volume_path}

                    try:
                        session_num = self._get_sessionnum_by_srcvol(volume)
                        if session_num == 0:
                            nosession_volume += 1
                            if nosession_volume > nosession_volume_limit:
                                LOG.info(_('monitor_image_volume,'
                                           'delete unused image volume which is not managed by cinder and is made to boost performance,'
                                           'volume id:%(volumeid)s')
                                            % {'volumeid':f_volume_id})

                                format_volume = False
                                try:
                                    f_format = f_vol.getElementsByTagName('Format')[0].childNodes[0].data
                                    format_volume = self._get_bool(f_format)
                                except:
                                    pass
                                # end of getting format_volume

                                self.delete_image_volume(volume, f_image_id, format_volume=format_volume)
                                continue
                            # end of if
                        #end of if

                        self._update_image_volume_info(f_volume_id, f_image_id, str(session_num))
                    except Exception as e:
                        LOG.info(_('monitor_image_volume, image volume update event : %(id)s (%(err)s)') 
                                   % {'id':f_volume_id, 'err':str(e)})

                        self.delete_image_volume(volume, f_image_id, delete_volume=False)
                # end of for volume
            # end of image

        # end of if
        LOG.debug(_('*****monitor_image_volume, Exit method'))
        return


    #----------------------------------------------------------------------------------------------#
    # Method : _find_device_number                                                                 #
    #         summary      : return number of mapping order                                        #
    #         return-value : mapping order                                                         #
    #----------------------------------------------------------------------------------------------#
    def _find_device_number(self, volume, connector):
        '''
        return mapping order
        '''
        # map_num          : number of mapping order
        # volumename       : volumename on ETERNUS
        # vol_instance     : volume instance
        # ag_instance      : affinity group instance
        # aglist           : affinity group list associated with the connector and the volume
        # vo_volmaplist    : volume mapping information list associated with the volume
        # ag_volmaplist    : volume mapping information list associated with the affinitygroup
        # vo_volmap        : volume mapping information associated with the volume
        # ag_volmap        : volume mapping information associated with the affinitygroup
        # volmapinstance   : volume mapping instance
        # found_volmaplist : volume mapping information list

        LOG.debug(_('*****_find_device_number,Enter method'))

        # initialize
        map_num          = None
        volumename       = None
        vol_instance     = None
        ag_instance      = None
        aglist           = []
        vo_volmaplist    = []
        ag_volmaplist    = []
        vo_volmap        = None
        ag_volmap        = None
        volmapinstance   = {}
        iqn              = None
        found_volmaplist = []

        # main processing
        volumename   = self._create_volume_name(volume['id'])
        vol_instance = self._find_lun(volume)

        # Existence check the volume
        if vol_instance is None:
            # volume does not found
            msg=(_('_find_device_number,'
                   'volume:%(volume)s,'
                   'Volume not found.')
                  % {'volume':volume['name']})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        # find affinity group
        # attach the connector and include the volume
        aglist = self._find_affinity_group(connector, vol_instance)
        if len(aglist) == 0 :
            LOG.debug(_('*****_find_device_number,ag_list:%s') % (aglist))
        else:
            try:
                ag_instance = self._get_eternus_instance(aglist[0],LocalOnly=False)
            except:
                msg=(_('_find_device_number,'
                       'aglist[0]:%(aglist0)s,'
                       'GetInstance,'
                       'cannot connect to ETERNUS.')
                      % {'aglist0':aglist[0]})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            # get volume mapping order
            try:
                ag_volmaplist = self._reference_eternus_names(
                          ag_instance.path,
                          ResultClass='CIM_ProtocolControllerForUnit')
                vo_volmaplist = self._reference_eternus_names(
                          vol_instance.path,
                          ResultClass='CIM_ProtocolControllerForUnit')
            except:
                msg=(_('_find_device_number,'
                   'volume:%(volume)s,'
                   'ReferenceNames,'
                   'cannot connect to ETERNUS.')
                  % {'volume':volume})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            LOG.debug(_('*****_find_device_number,'
                    'ag_volmaplist:%(ag_volmaplist)s,'
                    'vo_volmaplist:%(vo_volmaplist)s'
                    )
                   % {'ag_volmaplist':ag_volmaplist,
                      'vo_volmaplist':vo_volmaplist})

            for ag_volmap in ag_volmaplist:
                for vo_volmap in vo_volmaplist:
                    LOG.debug(_('*****_find_device_number,ag_volmap:%s') % (ag_volmap))
                    LOG.debug(_('*****_find_device_number,vo_volmap:%s') % (vo_volmap))
                    if ag_volmap == vo_volmap:
                        found_volmaplist.append(ag_volmap)
                        break
                    # end of if
                if len(found_volmaplist) != 0:
                    break
                # end of if
            # end of for ag_volmaplist
            LOG.debug(_('*****_find_device_number,found_volmaplist:%s') % found_volmaplist)

            for found_volmap in found_volmaplist:
                try:
                    volmapinstance = self._get_eternus_instance(
                    found_volmap,
                    LocalOnly=False)
                except:
                    msg=(_('_find_device_number,'
                           'volume:%(volume)s,'
                           'GetInstance,'
                           'cannot connect to ETERNUS.')
                          % {'volume':volume})
                    LOG.error(msg)
                    raise exception.VolumeBackendAPIException(data=msg)

                map_num = int(volmapinstance['DeviceNumber'], 16)
                LOG.debug(_('*****_find_device_number,found_volmap:%s') % (found_volmap))
                LOG.debug(_('*****_find_device_number,map_num:%s') % (map_num))
            # end of for found_volmaplist
        # end of if

        if map_num is None:
            LOG.debug(_('*****_find_device_number,'
                        'Device number not found for volume,'
                        '%(volumename)s %(vol_instance)s.')
                       % {'volumename': volumename,
                          'vol_instance': str(vol_instance.path)})
        else:
            LOG.debug(_('*****_find_device_number,'
                        'Found device number %(device)d for volume,'
                        ' %(volumename)s %(vol_instance)s.')
                       % {'device': map_num,
                          'volumename': volumename,
                          'vol_instance': str(vol_instance.path)})
        # end of if

        LOG.debug(_('*****_find_device_number,Device number: %(map_num)s.')
                   % {'map_num': map_num})

        return map_num

    #----------------------------------------------------------------------------------------------#
    # Method : _get_mapdata                                                                        #
    #         summary      : return mapping information                                            #
    #         return-value : mapping infomation                                                    #
    #----------------------------------------------------------------------------------------------#
    def _get_mapdata(self, device_number, connector, targetlist = []):
        '''
        return mapping infomation
        '''
        # device_number    : device number for target volume (mapping order)
        # targetlist       : target portid
        # mapdata          : device information

        LOG.debug(_('*****_get_mapdata,Enter method'))

        # initialize
        mapdata        = {}

        if self.protocol == 'iSCSI':
            iqn            = None
            iqns           = None
            target_portal  = None
            target_portals = None

            mapdata = self._get_iscsi_portal_info()
            mapdata['target_lun'] =  device_number
            mapdata['target_luns'] = [device_number] * len(mapdata['target_portals'])

        elif self.protocol == 'fc':
            if len(targetlist) == 0:
                targetlist, _x = self._get_target_portid(connector)
            # end of if

            LOG.debug(_('*****_get_mapdata,'
                        'targetlist:%(targetlist)s')
                       % {'targetlist':targetlist})

            mapdata = {'target_lun': device_number,
                    'target_wwn': targetlist}
        # end of if

        LOG.debug(_('*****_get_mapdata,Device info: %(mapdata)s.')
                   % {'mapdata': mapdata})

        return mapdata


    #----------------------------------------------------------------------------------------------#
    # Method : _get_drvcfg                                                                         #
    #         summary      : read parameter from driver configuration file                         #
    #         return-value : value of tagname                                                      #
    #----------------------------------------------------------------------------------------------#
    def _get_drvcfg(self,tagname,filename=None, multiple=False, allowNone=False):
        '''
        read from driver configuration file.
        '''
        # filename  : driver configuration file name
        # tagname   : xml tagname
        # tree      : element tree from driver configuration file
        # elem      : root element
        # ret       : return value

        LOG.debug(_('*****_get_drvcfg,Enter method'))

        # initialize
        tree = None
        elem = None
        ret  = None

        # main processing
        if filename is None:
            # set default configuration file name
            filename = self.configuration.cinder_eternus_config_file
        # end of if

        LOG.debug(_("*****_get_drvcfg input[%s][%s]") %(filename, tagname))

        tree = parse(filename)
        elem = tree.getroot()

        if multiple is False:
            ret = elem.findtext(".//"+tagname)
        else:
            ret = []
            for e in elem.findall(".//"+tagname):
                if e.text not in ret:
                    ret.append(e.text)
                # end of if
            # end of for elem
        # end of if

        if ret is None or ret == "":
            if allowNone is False:
                msg = (_('_get_drvcfg,'
                         'filename:%(filename)s,'
                         'tagname:%(tagname)s,'
                         'data is None!!  '
                         'Please edit driver configuration file and correct an error. ')
                        % {'filename':filename,
                           'tagname':tagname})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            else:
                ret = None
            # end of if
        # end of if

        LOG.debug(_("*****_get_drvcfg output[%s]") %(ret))

        return ret

    #----------------------------------------------------------------------------------------------#
    # Method : _get_imgcfg                                                                         #
    #         summary      : read parameter from image management file                             #
    #         return-value : volume info                                                           #
    #----------------------------------------------------------------------------------------------#
    def _get_imgcfg(self, filename, volume_size, image_id, storage_name):
        '''
        read from image management file.
        '''
        # tree      : element tree from driver configuration file
        # elem      : root element
        # ret       : return value
        # limit_session_per_volume : limitation of OPC session by volume

        LOG.debug(_("*****_get_imgcfg input[%s][%s][%s]") %(filename, image_id, str(volume_size)))

        # initialize
        tree                     = None
        elem                     = None
        ret                      = None
        limit_session_per_volume = 8

        # main processing
        if os.path.exists(filename):
            try:
                tree = parse(filename)
                elem = tree.getroot()

                for child in elem:
                    f_image_id = child.findtext(".//ImageID")

                    if image_id != f_image_id:
                        continue
                    # end of if

                    volumes = child.findall(".//Volume")
                    for volume in volumes:
                        f_session_text= volume.findtext(".//Session")
                        if f_session_text == DELETE_IMGVOL:
                            continue

                        f_session_num = int(f_session_text)
                        f_volume_size = int(volume.findtext(".//VolumeSize"))
                        f_storage_name = volume.findtext(".//StorageName")

                        try:
                            f_volume_path = volume.findtext(".//VolumePath")
                        except:
                            f_volume_path = None


                        if ( f_session_num < limit_session_per_volume ) and ( volume_size >= f_volume_size ) and ( storage_name == f_storage_name ):
                            ret = {'id' : volume.findtext(".//VolumeID"),
                                   'provider_location': f_volume_path}
                            break
                        else:
                            continue
                        # end of if
                    # end of for volume

                    if ret is not None:
                        break
                    # end of if
                # end of for elem
            except:
                LOG.info(_("_get_imgcfg, management file is invalid."))
        # end of if

        LOG.debug(_("*****_get_imgcfg output[%s]") %(str(ret)))
        return ret

    #----------------------------------------------------------------------------------------------#
    # Method : _get_eternus_connection                                                             #
    #         summary      : return WBEM connection                                                #
    #         return-value : WBEM connection                                                       #
    #----------------------------------------------------------------------------------------------#
    def _get_eternus_connection(self, filename=None):
        '''
        return WBEM connection
        '''
        # filename  : driver configuration file name
        # ip        : SMI-S IP address
        # port      : SMI-S port
        # user      : SMI-S username
        # password  : SMI-S password
        # url       : SMI-S connection url
        # conn      : WBEM connection

        LOG.debug(_("*****_get_eternus_connection [%s],"
                    "Enter method")
                   % filename)

        # initialize
        ip       = None
        port     = None
        user     = None
        password = None
        url      = None
        conn     = None

        # main processing
        ip     = self._get_drvcfg('EternusIP', filename)
        port   = self._get_drvcfg('EternusPort', filename)
        user   = self._get_drvcfg('EternusUser', filename)
        passwd = self._get_drvcfg('EternusPassword', filename)
        url    = 'http://'+ip+':'+port

        conn   = pywbem.WBEMConnection(url, (user, passwd),
                                     default_namespace='root/eternus')

        if conn is None:
            msg = (_('_get_eternus_connection,'
                     'filename:%(filename)s,'
                     'ip:%(ip)s,'
                     'port:%(port)s,'
                     'user:%(user)s,'
                     'passwd:%(passwd)s,'
                     'url:%(url)s,'
                     'FAILED!!.')
                    % {'filename':filename,
                       'ip':ip,
                       'port':port,
                       'user':user,
                       'passwd':passwd,
                       'url':url})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        LOG.debug(_('*****_get_eternus_connection,[%s],Exit method') % (conn))

        return conn

    #----------------------------------------------------------------------------------------------#
    # Method : _create_volume_name                                                                 #
    #         summary      : create volume_name on ETERNUS from id on OpenStack.                   #
    #         return-value : volumename on ETERNUS                                                 #
    #----------------------------------------------------------------------------------------------#
    def _create_volume_name(self, id_code):
        '''
        create volume_name on ETERNUS from id on OpenStack.
        '''
        # id_code         : volume_id, snapshot_id etc..
        # m               : hashlib.md5 instance
        # ret             : volumename on ETERNUS
        # systemnamelist  : ETERNUS information list
        # systemname      : ETERNUS model information

        LOG.debug(_('*****_create_volume_name [%s],Enter method.')
                   % id_code)

        # initialize
        m               = None
        ret             = None
        systemnamelist  = None
        systemname      = None

        # main processing
        if id_code is None:
            msg=(_('_create_volume_name,'
                   'id_code is None.'))
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        m = hashlib.md5()
        m.update(id_code)
        ret = VOL_PREFIX + str(base64.urlsafe_b64encode(m.digest()))

        # get eternus model for volumename length
        # systemname = systemnamelist[0]['IdentifyingNumber']
        # ex) ET092DC4511133A10
        try:
            systemnamelist = self._enum_eternus_instances(
                'FUJITSU_StorageProduct')
        except:
            msg=(_('create_volume_name,'
                   'id_code:%(id_code)s,'
                   'EnumerateInstances,'
                   'cannot connect to ETERNUS.')
                  % {'id_code':id_code})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        systemname = systemnamelist[0]['IdentifyingNumber']

        LOG.debug(_('*****_create_volume_name,'
                    'systemname:%(systemname)s,'
                    'storage is DX S%(model)s')
                   % {'systemname':systemname,
                      'model':systemname[4]})

        # shorten volumename when storage is DX S2 series
        if str(systemname[4]) == '2':
            LOG.debug(_('*****_create_volume_name,'
                        'volumename is 16 digit.'))
            ret = ret[:16]
        # end of if

        LOG.debug(_('*****_create_volume_name,'
                    'ret:%(ret)s,'
                    'Exit method.')
                   % {'ret':ret})

        return ret

    #----------------------------------------------------------------------------------------------#
    # Method : _get_snap_pool_name                                                                 #
    #         summary      : get pool name for sdv                                                 #
    #         return-value : poolname                                                              #
    #----------------------------------------------------------------------------------------------#
    def _get_snap_pool_name(self):
        '''
        get pool name for SDV
        '''
        # snap_pool_name : poolname for sdv

        LOG.debug(_('*****_get_snap_pool_name,Enter method'))

        # initialize
        snap_pool_name = None

        # main processing
        snap_pool_name = self._get_drvcfg('EternusSnapPool', allowNone=True)

        if snap_pool_name is None:
            snap_pool_name = self._get_drvcfg('EternusPool')
        # end of if

        LOG.debug(_('*****_get_snap_pool_name,Exit method'))
        return snap_pool_name

    #----------------------------------------------------------------------------------------------#
    # Method : _get_pool_instance_id                                                               #
    #         summary      : get pool instacne_id from pool name on ETERNUS                        #
    #         return-value : pool instance id                                                      #
    #----------------------------------------------------------------------------------------------#
    def _get_pool_instance_id(self, eternus_pool):
        '''
        get pool instacne_id from pool name on ETERNUS
        '''
        # eternus_pool  : pool name on ETERNUS.
        # poolinstanceid: ETERNUS pool instance id(return value)
        # tppoollist    : list of thinprovisioning pool on ETERNUS.
        # rgpoollist    : list of raid group on ETERNUS.
        # tppool        : Thinprovisioning Pool
        # rgpool        : RAID Group
        # msg           : message

        LOG.debug(_('*****_get_pool_instance_id,'
                    'Enter method'))

        # initialize
        poolinstanceid = None
        tppoollist    = []
        rgpoollist    = []
        tppool        = None
        rgpool        = None
        poolname      = None
        msg           = None


        # main processing
        try:
            rgpoollist = self._enum_eternus_instances(
                'FUJITSU_RAIDStoragePool')
            tppoollist = self._enum_eternus_instances(
                'FUJITSU_ThinProvisioningPool')
        except:
            msg=(_('_get_pool_instance_id,'
                   'eternus_pool:%(eternus_pool)s,'
                   'EnumerateInstances,'
                   'cannot connect to ETERNUS.')
                  % {'eternus_pool':eternus_pool})

            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)


        for rgpool in rgpoollist:
            poolname = rgpool['ElementName']
            if str(eternus_pool) == str(poolname):
                poolinstanceid = rgpool['InstanceID']
                break
            # end of if
        else:
            for tppool in tppoollist:
                poolname = tppool['ElementName']
                if str(eternus_pool) == str(poolname):
                    poolinstanceid = tppool['InstanceID']
                    break
                # end of if
            # end of for tppoollist
        # end of for rgpoollist

        if poolinstanceid is None:
            msg = (_('_get_pool_instance_id,'
                     'eternus_pool:%(eternus_pool)s,'
                     'poolinstanceid is None.')
                    % {'eternus_pool': eternus_pool})
            LOG.info(msg)
        # end of if

        LOG.debug(_('*****_get_pool_instance_id,'
                    'Exit method'))

        return poolinstanceid

    #----------------------------------------------------------------------------------------------#
    # Method : _create_pool                                                                        #
    #         summary      : create raidgroup on ETERNUS                                           #
    #         return-value : None                                                                  #
    #----------------------------------------------------------------------------------------------#
    def _create_pool(self, eternus_pool):
        '''
          create raidgroup on ETERNUS.
        '''
        #  create raidgroup on ETERNUS.
        #  raidgroup name is eternus_pool.
        #  raidlevel and diskdrives are automatically selected.
        #
        # eternus_pool : poolname on ETERNUS
        # configservice: FUJITSU_StorageConfigurationService
        # msg          : message
        # rc           : result of invoke method
        # errordesc    : error message
        # job          : unused

        LOG.debug(_('*****_create_pool,'
                    'eternus_pool:%(eternus_pool)s,'
                    'Enter method.')
                   % {'eternus_pool': eternus_pool})

        # initialize
        poolinstance  = None
        configservice = None
        msg           = None
        rc            = 0
        errordesc     = None
        job           = None

        # main processing
        configservice = self._find_eternus_service(STOR_CONF)
        if configservice is None:
            msg = (_('_create_pool,'
                     'eternus_pool:%(eternus_pool)s,'
                     'configservice is None.')
                    % {'eternus_pool': eternus_pool})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        # Invoke method for create pool
        # Raid Level and DiskDrives are automatically selected.
        rc, errordesc, job = self._exec_eternus_service(
            'CreateOrModifyStoragePool',
            configservice,
            ElementName=eternus_pool)

        if rc != 0L and rc != 4096L:
            msg=(_('_create_pool,'
                   'eternus_pool:%(eternus_pool)s,'
                   'Return code:%(rc)lu,'
                   'Error:%(errordesc)s')
                  % {'eternus_pool':eternus_pool,
                     'rc':rc,
                     'errordesc':errordesc})

            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        LOG.debug(_('*****_create_pool,'
                    'eternus_pool:%(eternus_pool)s,'
                    'Exit method.')
                   % {'eternus_pool': eternus_pool})

        return


    #----------------------------------------------------------------------------------------------#
    # Method : _find_pool                                                                          #
    #         summary      : find Instance or Name of pool by pool name on ETERNUS.                #
    #         return-value : pool instance or instance name                                        #
    #----------------------------------------------------------------------------------------------#
    def _find_pool(self, eternus_pool, detail=False):
        '''
        find Instance or InstanceName of pool by pool name on ETERNUS.
        '''
        # eternus_pool  : poolname
        # detail        : False >return EnumerateInstanceNames
        #               : True  >return EnumerateInstances
        # poolinstanceid: pool instance id
        # poolinstance  : pool instance
        # msg           : message
        # tppoollist    : list of thinprovisioning pool on ETERNUS.
        # rgpoollist    : list of raid group on ETERNUS.

        LOG.debug(_('*****_find_pool,Enter method'))

        # initialize
        poolinstanceid = None
        poolinstance   = None
        msg            = None
        tppoollist     = []
        rgpoollist     = []

        #main processing
        poolinstanceid = self._get_pool_instance_id(eternus_pool)
        #if pool instance is None then create pool on ETERNUS.
        if poolinstanceid is None:
            msg = (_('_find_pool,'
                     'eternus_pool:%(eternus_pool)s,'
                     'poolinstanceid is None.')
                    % {'eternus_pool': eternus_pool})
            LOG.info(msg)

        else:

            if detail is True:
                try:
                    tppoollist = self._enum_eternus_instances(
                        'FUJITSU_ThinProvisioningPool')
                    rgpoollist = self._enum_eternus_instances(
                        'FUJITSU_RAIDStoragePool')
                except:
                    msg=(_('_find_pool,'
                           'eternus_pool:%(eternus_pool)s,'
                           'EnumerateInstances,'
                           'cannot connect to ETERNUS.')
                          % {'eternus_pool':eternus_pool})
                    LOG.error(msg)
                    raise exception.VolumeBackendAPIException(data=msg)

            else:
                try:
                    tppoollist = self._enum_eternus_instance_names(
                        'FUJITSU_ThinProvisioningPool')
                    rgpoollist = self._enum_eternus_instance_names(
                        'FUJITSU_RAIDStoragePool')
                except:
                    msg=(_('_find_pool,'
                           'eternus_pool:%(eternus_pool)s,'
                           'EnumerateInstanceNames,'
                           'cannot connect to ETERNUS.')
                          % {'eternus_pool':eternus_pool})
                    LOG.error(msg)
                    raise exception.VolumeBackendAPIException(data=msg)
            # end of if

            for tppool in tppoollist:
                if str(poolinstanceid) == str(tppool['InstanceID']):
                    poolinstance = tppool
                    break
                # end of if
            else:
                for rgpool in rgpoollist:
                    if str(poolinstanceid) == str(rgpool['InstanceID']):
                        poolinstance = rgpool
                        break
                    # end of if
                # end of for rgpoollist
            # end of for tppoollist
        # end of if
        LOG.debug(_('*****_find_pool,'
                    'poolinstance: %(poolinstance)s,'
                    'Exit method.')
                   % {'poolinstance': str(poolinstance)})

        return poolinstance

    #----------------------------------------------------------------------------------------------#
    # Method : _find_eternus_service                                                               #
    #         summary      : find CIM instance                                                     #
    #         return-value : CIM instance                                                          #
    #----------------------------------------------------------------------------------------------#
    def _find_eternus_service(self, classname):
        '''
        find CIM instance
        '''
        # ret      : CIM instance
        # services : CIM instance service name

        LOG.debug(_('*****_find_eternus_service,'
                    'classname:%(a)s,'
                    'Enter method')
                   % {'a':str(classname)})

        # initialize
        ret      = None
        services = None

        # main processing
        try:
            services = self._enum_eternus_instance_names(
                str(classname))
        except:
            msg=(_('_find_eternus_service,'
                   'classname:%(classname)s,'
                   'EnumerateInstanceNames,'
                   'cannot connect to ETERNUS.')
                  % {'classname':str(classname)})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        ret = services[0]
        LOG.debug(_('*****_find_eternus_service,'
                    'classname:%(classname)s,'
                    'ret:%(ret)s,'
                    'Exit method')
                   % {'classname':classname,
                      'ret':(str(ret))})
        return ret

    #----------------------------------------------------------------------------------------------#
    # Method : _exec_eternus_service                                                               #
    #         summary      : Execute SMI-S Method                                                  #
    #         return-value : status code, error description, data                                  #
    #----------------------------------------------------------------------------------------------#
    @FJDXLockutils('SMIS-exec', 'cinder-', True)
    def _exec_eternus_service(self, classname, instanceNameList, retry=20, retry_interval=5, retry_code=[32787L], **param_dict):
        '''
        Execute SMI-S Method
        '''
        # rc       : result of InvokeMethod
        # retdata  : return data
        # errordesc: error description

        LOG.debug(_('*****_exec_eternus_service,'
                    'classname:%(a)s,'
                    'instanceNameList:%(b)s,'
                    'parameters:%(c)s,'
                    'Enter method')
                   % {'a':str(classname),
                      'b':str(instanceNameList),
                      'c':str(param_dict)})

        # initialize
        rc        = None
        retdata   = None
        errordesc = None

        for retry_num in range(retry):
            # main processing
            # use InvokeMethod
            try:
                rc, retdata = self.conn.InvokeMethod(
                    classname,
                    instanceNameList,
                    **param_dict)
            except:
                if rc is None:
                    msg=(_('_exec_eternus_service,'
                           'classname:%(classname)s,'
                           'InvokeMethod,'
                           'cannot connect to ETERNUS.')
                          % {'classname':str(classname)})
                    LOG.info(msg)
                    continue
                # end of if

            if rc not in retry_code:
                break
            else:
                LOG.info(_('_exec_eternus_service, retry'
                           'RetryCode:%(rc)s,'
                           'TryNum:%(rn)s')
                          % {'rc':str(rc),
                             'rn':str(retry_num+1)
                            })
                time.sleep(retry_interval)
                continue
             # end of if
        else:
            if rc is None:
                msg=(_('_exec_eternus_service,'
                       'classname:%(classname)s,'
                       'InvokeMethod,'
                       'cannot connect to ETERNUS.')
                      % {'classname':str(classname)})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            else:
                LOG.info(_('_exec_eternus_service, Retry was exceeded.'))
            # end of if
        # end of for retry

        # convert errorcode to error description
        try:
            errordesc = RETCODE_dic[str(rc)]
        except:
            errordesc = 'Undefined Error!!'
        ret = (rc, errordesc, retdata)

        LOG.debug(_('*****_exec_eternus_service,'
                    'classname:%(a)s,'
                    'instanceNameList:%(b)s,'
                    'parameters:%(c)s,'
                    'Return code:%(rc)s,'
                    'Error:%(errordesc)s,'
                    'Exit method')
                   % {'a':str(classname),
                      'b':str(instanceNameList),
                      'c':str(param_dict),
                      'rc':str(rc),
                      'errordesc':errordesc})

        return ret


    #----------------------------------------------------------------------------------------------#
    # Method : _enum_eternus_instances                                                             #
    #         summary      :                                                                       #
    #         return-value :                                                                       #
    #----------------------------------------------------------------------------------------------#
    @FJDXLockutils('SMIS-other', 'cinder-', True)
    def _enum_eternus_instances(self, classname, retry=20, retry_interval=5):
        '''
        Enumerate Instances
        '''
        for retry_num in range(retry):
            try:
                ret = self.conn.EnumerateInstances(classname)
                break
            except Exception as e:
                LOG.info(_('_enum_eternus_instances,'
                           ' reason:%(reason)s,'
                           ' try (%(retrynum)s)'
                          % {'reason': str(e.args),
                             'retrynum': str(retry_num+1)}))
                time.sleep(retry_interval)
                continue
        else:
            msg = (_('_enum_eternus_instances, Error'))
            raise exception.VolumeBackendAPIException(data=msg)
        # end of for rety

        return ret


    #----------------------------------------------------------------------------------------------#
    # Method : _enum_eternus_instance_names                                                        #
    #         summary      :                                                                       #
    #         return-value :                                                                       #
    #----------------------------------------------------------------------------------------------#
    @FJDXLockutils('SMIS-other', 'cinder-', True)
    def _enum_eternus_instance_names(self, classname, conn=None, retry=20, retry_interval=5):
        '''
        Enumerate Instance Names
        '''
        if conn is None:
            conn = self.conn
        # end of if

        for retry_num in range(retry):
            try:
                ret = conn.EnumerateInstanceNames(classname)
                break
            except Exception as e:
                LOG.info(_('_enum_eternus_instance_names,'
                           ' reason:%(reason)s,'
                           ' try (%(retrynum)s)'
                          % {'reason': str(e.args),
                             'retrynum': str(retry_num+1)}))
                time.sleep(retry_interval)
                continue
        else:
            msg = (_('_enum_eternus_instance_names, Error'))
            raise exception.VolumeBackendAPIException(data=msg)
        # end of for rety

        return ret

    #----------------------------------------------------------------------------------------------#
    # Method : _get_eternus_instance                                                               #
    #         summary      :                                                                       #
    #         return-value :                                                                       #
    #----------------------------------------------------------------------------------------------#
    @FJDXLockutils('SMIS-getinstance', 'cinder-', True)
    def _get_eternus_instance(self, classname, conn=None, AllowNone=False, retry=20, retry_interval=5, **param_dict):
        '''
        Get Instance
        '''
        if conn is None:
            conn = self.conn
        # end of if 

        ret = None
        for retry_num in range(retry):
            try:
                ret = conn.GetInstance(classname, **param_dict)
                break
            except Exception as e:
                if ( e.args[0] == 6 ) and ( AllowNone is True ):
                    break
                else:
                    LOG.info(_('_get_eternus_instance,'
                               ' reason:%(reason)s,'
                               ' try (%(retrynum)s)'
                              % {'reason': str(e.args),
                                 'retrynum': str(retry_num+1)}))
                    time.sleep(retry_interval)
                    continue
        else:
            msg = (_('_get_eternus_instance, Error'))
            raise exception.VolumeBackendAPIException(data=msg)
        # end of for rety

        return ret

    #----------------------------------------------------------------------------------------------#
    # Method : _assoc_eternus                                                                      #
    #         summary      :                                                                       #
    #         return-value :                                                                       #
    #----------------------------------------------------------------------------------------------#
    @FJDXLockutils('SMIS-other', 'cinder-', True)
    def _assoc_eternus(self, classname, retry=20, retry_interval=5, **param_dict):
        '''
        Associator
        '''
        for retry_num in range(retry):
            try:
                ret = self.conn.Associators(classname, **param_dict)
                break
            except Exception as e:
                LOG.info(_('_assoc_eternus,'
                           ' reason:%(reason)s,'
                           ' retry (%(retrynum)s)'
                          % {'reason': str(e.args),
                             'retrynum': str(retry_num+1)}))
                time.sleep(retry_interval)
                continue
        else:
            msg = (_('_assoc_eternus, Error'))
            raise exception.VolumeBackendAPIException(data=msg)
        # end of for rety

        return ret

    #----------------------------------------------------------------------------------------------#
    # Method : _assoc_eternus_names                                                                #
    #         summary      :                                                                       #
    #         return-value :                                                                       #
    #----------------------------------------------------------------------------------------------#
    @FJDXLockutils('SMIS-other', 'cinder-', True)
    def _assoc_eternus_names(self, classname, retry=20, retry_interval=5, **param_dict):
        '''
        Associator Names
        '''
        for retry_num in range(retry):
            try:
                ret = self.conn.AssociatorNames(classname, **param_dict)
                break
            except Exception as e:
                LOG.info(_('_assoc_eternus_names,'
                           ' reason:%(reason)s,'
                           ' try (%(retrynum)s)'
                          % {'reason': str(e.args),
                             'retrynum': str(retry_num+1)}))
                time.sleep(retry_interval)
                continue
        else:
            msg = (_('_assoc_eternus_names, Error'))
            raise exception.VolumeBackendAPIException(data=msg)
        # end of for rety

        return ret

    #----------------------------------------------------------------------------------------------#
    # Method : _refernce_eternus_names                                                             #
    #         summary      :                                                                       #
    #         return-value :                                                                       #
    #----------------------------------------------------------------------------------------------#
    @FJDXLockutils('SMIS-other', 'cinder-', True)
    def _reference_eternus_names(self, classname, retry=20, retry_interval=5, **param_dict):
        '''
        Refference Names
        '''
        for retry_num in range(retry):
            try:
                ret = self.conn.ReferenceNames(classname, **param_dict)
                break
            except Exception as e:
                LOG.info(_('_reference_eternus_names,'
                           ' reason:%(reason)s,'
                           ' try (%(retrynum)s)'
                          % {'reason': str(e.args),
                             'retrynum': str(retry_num+1)}))
                time.sleep(retry_interval)
                continue
        else:
            msg = (_('_reference_eternus_names, Error'))
            raise exception.VolumeBackendAPIException(data=msg)
        # end of for rety

        return ret

    #----------------------------------------------------------------------------------------------#
    # Method : _exec_eternus_cli                                                                   #
    #         summary      : Execute ETERNUS CLI                                                   #
    #         return-value : status code, error description, data                                  #
    #----------------------------------------------------------------------------------------------#
    @FJDXLockutils('SMIS-exec', 'cinder-', True)
    def _exec_eternus_cli(self, command, retry=20, retry_interval=5, retry_code=[32787L], **param_dict):
        '''
        Execute ETERNUS CLI
        '''
        # out, _err  : output of exec CLI
        # rc, rc_str : result of exec CLI
        # retdata    : return data
        # errordesc  : error description
        # user       : user name
        # storage_ip : storage ip

        LOG.debug(_('*****_exec_eternus_cli,'
                    'command:%(a)s,'
                    'parameters:%(b)s,'
                    'Enter method')
                   % {'a':str(command),
                      'b':str(param_dict)})

        # initialize
        out        = None
        _err       = None
        result     = None
        rc         = None
        rc_str     = None
        retdata    = None
        errordesc  = None
        user       = self._get_drvcfg('EternusUser')
        storage_ip = self._get_drvcfg('EternusIP')

        # main processing
        for retry_num in range(retry):
            # execute ETERNUS CLI & get return value
            try:
                if param_dict:
                    (out, _err) = utils.execute('eternus_dx_cli', '-u', user, 
                                                '-l', storage_ip, '-c', command, '-o', str(param_dict), run_as_root=True)
                else:
                    (out, _err) = utils.execute('eternus_dx_cli', '-u', user, 
                                                '-l', storage_ip, '-c', command, run_as_root=True)
                # end of if

                out_dict = eval(out)
                result   = out_dict.get('result')
                rc_str   = out_dict.get('rc')
                retdata  = out_dict.get('message')
            except Exception as ex:
                msg=(_('_exec_eternus_cli,'
                       'stdout: %(out)s,'
                       'stderr: %(err)s,'
                       'unexpected error:'
                       % {'out': out,
                          'err': _err}))
                LOG.error(msg)
                LOG.error(ex)
                raise exception.VolumeBackendAPIException(data=msg)

            # check ssh result
            if result == 255:
                LOG.info(_('_exec_eternus_cli, retry,'
                           'command:%(command)s,'
                           'option:%(option)s,'
                           'ip:%(ip)s,'
                           'SSH Result:%(result)s,'
                           'retdata:%(retdata)s,'
                           'TryNum:%(rn)s')
                           %{'command':command,
                             'option':str(param_dict),
                             'ip':storage_ip,
                             'result': str(result),
                             'retdata': retdata,
                             'rn':str(retry_num+1)
                            })
                time.sleep(retry_interval)
                continue
            elif result != 0:
                msg = (_('_exec_eternus_cli,'
                       'unexpected error,'
                       'command:%(command)s,'
                       'option:%(option)s,'
                       'ip:%(ip)s,'
                       'result:%(result)s,'
                       'retdata:%(retdata)s')
                       %{'command':command,
                         'option':str(param_dict),
                         'ip':storage_ip,
                         'result':str(result),
                         'retdata': retdata})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            # end of if

            # check CLI return code
            if rc_str.isdigit():
                # SMI-S style return code
                rc = int(rc_str)

                try:
                    errordesc = RETCODE_dic[str(rc)]
                except:
                    errordesc = 'Undefined Error!!'

                if rc in retry_code:
                    LOG.info(_('_exec_eternus_cli, retry,'
                               'ip:%(ip)s,'
                               'RetryCode:%(rc)s,'
                               'TryNum:%(rn)s')
                              % {'ip':storage_ip,
                                 'rc':str(rc),
                                 'rn':str(retry_num+1)
                                })
                    time.sleep(retry_interval)
                    continue
                # end of if

                break
            else:
                # CLI style return code
                LOG.warn(_('_exec_eternus_cli,'
                           'WARNING!!,'
                           'ip:%(ip)s,'
                           'ReturnCode:%(rc_str)s,'
                           'ReturnData:%(retdata)s')
                          %{'ip':storage_ip,
                            'rc_str': rc_str,
                            'retdata': retdata})

                errordesc = retdata
                retdata   = rc_str
                rc = 4 # Failed
                break
             # end of if
        else:
            if result > 0:
                msg = (_('_exec_eternus_cli,'
                         'cannot connect to ETERNUS.'
                         'SSH Result:%(result)s,'
                         'retdata:%(retdata)s')
                         % {'result': str(result),
                            'retdata': retdata,
                            'rn':str(retry_num+1)
                           })

                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            else:
                LOG.info(_('_exec_eternus_cli, Retry was exceeded.'))
            # end of if
        # end of for retry

        ret = (rc, errordesc, retdata)

        LOG.debug(_('*****_exec_eternus_cli,'
                    'command:%(a)s,'
                    'parameters:%(b)s,'
                    'ip:%(ip)s,'
                    'Return code:%(rc)s,'
                    'Error:%(errordesc)s,'
                    'Exit method')
                   % {'a':str(command),
                      'b':str(param_dict),
                      'ip':storage_ip,
                      'rc':rc,
                      'errordesc':errordesc})

        return ret

    #----------------------------------------------------------------------------------------------#
    # Method : _exec_ccm_script                                                                    #
    #         summary      : Execute CCM Script                                                    #
    #         return-value :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def _exec_ccm_script(self, command, source=None, source_volume=None, target=None, target_volume=None, copy_type=None):
        '''
        Execute ccm script
        
        '''
        # d_serial_no : target volume ETERNSU Serial No
        # s_option    : s option
        # d_option    : d option
        # t_option    : t option
        
        LOG.debug(_('*****_exec_ccm_script,Enter method'))

        # initialize
        s_option = ""
        d_option = ""
        t_option = ""

        if source is not None:
            s_olu_no    = None
            if source.has_key('FJ_Volume_No'):
                s_olu_no = source['FJ_Volume_No']
            else:
                vol_instance = self._find_lun(source_volume, use_service_name=True)
                s_olu_no = "0x" + vol_instance['DeviceID'][24:28]
            # end of if

            s_serial_no = source['FJ_Backend'][-10:]
            s_option = " -s \"%s/%s\"" % (s_serial_no, s_olu_no)
            command += s_option
        # end of if

        if target is not None:
            d_olu_no = target['FJ_Volume_No']
            d_serial_no = target['FJ_Backend'][-10:]
            d_option = " -d \"%s/%s\"" % (d_serial_no, d_olu_no)
            command += d_option
        # end of if

        if copy_type is not None:
            t_option = " -t %s" % ((copy_type[0]).lower() + copy_type[1:])
            command += t_option
        # end of if

        try:
            (out, _err) = utils.execute('acrec', command, run_as_root=True)
            LOG.debug(_('*****_exec_ccm_script,'
                        'command:%(command)s,'
                        's_option:%(s_option)s,'
                        'd_option:%(d_option)s,'
                        't_option:%(t_option)s')
                        % {'command': command,
                           's_option': s_option,
                           'd_option': d_option,
                           't_option': t_option})

        except Exception as ex:
            LOG.error(ex)
            LOG.error(_('_exec_ccm_script,'
                        'command:%(command)s,'
                        's_option:%(s_option)s,'
                        'd_option:%(d_option)s,'
                        't_option:%(t_option)s')
                        % {'command':command, 's_option': s_option, 
                           'd_option': d_option,'t_option': t_option})
            raise exception.VolumeBackendAPIException(ex)

        LOG.debug(_('*****_exec_ccm_script,Exit method'))
        return


    #----------------------------------------------------------------------------------------------#
    # Method : _create_volume_instance_name                                                        #
    #         summary      : create volume instance name from metadata                             #
    #         return-value : instancename                                                          #
    #----------------------------------------------------------------------------------------------#
    def _create_volume_instance_name(self, classname, bindings):
        instancename = None
             
        try:
            instancename = pywbem.CIMInstanceName(
                classname,
                namespace='root/eternus',
                keybindings=bindings)
        except NameError:
            instancename = None

        return instancename

    #----------------------------------------------------------------------------------------------#
    # Method : _find_lun                                                                           #
    #         summary      : find lun instance from volume class or volumename on ETERNUS.         #
    #         return-value : volume instance                                                       #
    #----------------------------------------------------------------------------------------------#
    def _find_lun(self, volume, use_service_name=False):
        '''
        find lun instance from volume class or volumename on ETERNUS.
        '''
        # volumename           : volume name on ETERNUS
        # namelist             : volume list
        # name                 : volume instanceName
        # vol_instance         : volume instance for temp
        # volume_instance_name : volume instance name
        # volumeinstance       : volume instance for return
        # location             : provider location (dictionary)
        # classname            : SMI-S class name
        # bindings             : SMI-S detail information

        LOG.debug(_('*****_find_lun,Enter method'))

        # initialize
        volumename           = None
        namelist             = []
        name                 = None
        vol_instance         = None
        volume_instance_name = None
        volumeinstance       = None

        # main processing
        volumename = self._create_volume_name(volume['id'])

        if use_service_name is False:
            conn = self.conn
        else:
            service_name  = volume['host'].split('@',1)[1].split('#')[0]
            conf_filename = Configuration(FJ_ETERNUS_DX_OPT_list,
                                 config_group=service_name).cinder_eternus_config_file
            conn = self._get_eternus_connection(conf_filename)
        # end of if


        try:
            location = eval(volume['provider_location'])
            classname = location['classname'] 
            bindings  = location['keybindings'] 

            if (classname is not None) and (bindings is not None):
                LOG.debug(_('*****_find_lun,'
                            'classname:%(classname)s,'
                            'bindings:%(bindings)s')
                            % {'classname':classname,
                               'bindings':bindings})
                volume_instance_name = self._create_volume_instance_name(classname, bindings)

                LOG.debug(_('*****_find_lun,'
                            'volume_insatnce_name:%(volume_instance_name)s')
                            % {'volume_instance_name':volume_instance_name})

                vol_instance = self._get_eternus_instance(volume_instance_name, conn=conn, AllowNone=True)

                if vol_instance['ElementName'] == volumename:
                    volumeinstance = vol_instance
        except:
            volumeinstance = None

        if volume_instance_name is None:
            #for old version

            LOG.debug(_('*****_find_lun,'
                        'volumename:%(volumename)s')
                       % {'volumename':volumename})

            # get volume instance from volumename on ETERNUS
            try:
                namelist = self._enum_eternus_instance_names(
                    'FUJITSU_StorageVolume')
            except:
                msg=(_('_find_lun,'
                       'volumename:%(volumename)s,'
                       'EnumerateInstanceNames,'
                       'cannot connect to ETERNUS.')
                      % {'volumename':volumename})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            for name in namelist:
                try:
                    vol_instance = self._get_eternus_instance(
                        name, conn=conn, AllowNone=True)

                    if vol_instance['ElementName'] == volumename:
                        volumeinstance = vol_instance

                        LOG.debug(_('*****_find_lun,'
                                    'volumename:%(volumename)s,'
                                    'vol_instance:%(vol_instance)s.')
                                 % {'volumename': volumename,
                                    'vol_instance': str(volumeinstance.path)})
                        break
                    # end of if
                except:
                    vol_instance = None
            else:
                LOG.debug(_('*****_find_lun,'
                            'volumename:%(volumename)s,'
                            'volume not found on ETERNUS.')
                           % {'volumename': volumename})
            # end of for namelist

        LOG.debug(_('*****_find_lun,Exit method'))

        #return volume instance
        return volumeinstance


    #----------------------------------------------------------------------------------------------#
    # Method : _find_copysession                                                                   #
    #         summary      : find copysession from volumename on ETERNUS                           #
    #         return-value : volume instance                                                       #
    #----------------------------------------------------------------------------------------------#
    def _find_copysession(self, vol_instance):
        '''
        find copysession from volumename on ETERNUS
        '''
        # volumename             : volumename on ETERNUS
        # cpsession              : copysession
        # repservice             : FUJITSU_ReplicationService
        # rc                     : Invoke Method return code
        # replicarellist         : copysession information list
        # replicales             : copysession information
        # snapshot_vol_instance  : snapshot volume instance
        # msg                    : message
        # errordesc              : error description
        # cpsession_instance     : copysession instance

        LOG.debug(_('*****_find_copysession, Enter method'))

        # initialize
        volumename            = None
        cpsession             = None
        repservice            = None
        rc                    = 0
        replicarellist        = None
        replicarel            = None
        snapshot_vol_instance = None
        msg                   = None
        errordesc             = None
        cpsession_instance    = None

        # main processing
        volumename   = vol_instance['ElementName']
        LOG.debug(_('*****_find_copysession,'
                    'volumename:%s.')
                   % volumename)

        if vol_instance is not None:
            # get copysession list
            repservice = self._find_eternus_service(REPL)
            if repservice is None:
                msg = (_('_find_copysession,'
                         'Cannot find Replication Service to '
                         'find copysession'))
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            # end of if


            # find copysession where volume is copy_source
            while True:
                LOG.debug(_('*****_find_copysession,source_volume while copysession'))
                cpsession = None

                rc, errordesc, replicarellist = self._exec_eternus_service(
                    'GetReplicationRelationships',
                    repservice,
                    Type=pywbem.Uint16(2),
                    Mode=pywbem.Uint16(2),
                    Locality=pywbem.Uint16(2))

                if rc != 0L and rc != 4096L:
                    msg = (_('_find_copysession,'
                             'source_volumename:%(volumename)s,'
                             'Return code:%(rc)lu,'
                             'Error:%(errordesc)s')
                            % {'volumename': volumename,
                               'rc': rc,
                               'errordesc':errordesc})
                    LOG.error(msg)
                    raise exception.VolumeBackendAPIException(data=msg)
                # end of if

                for replicarel in replicarellist['Synchronizations']:
                    LOG.debug(_('*****_find_copysession,'
                                'source_volume,'
                                'replicarel:%(replicarel)s')
                              % {'replicarel':replicarel})
                    try:
                        snapshot_vol_instance = self._get_eternus_instance(
                            replicarel['SystemElement'],
                            LocalOnly=False)
                    except:
                        msg=(_('_find_copysession,'
                               'source_volumename:%(volumename)s,'
                               'GetInstance,'
                               'cannot connect to ETERNUS.')
                              % {'volumename': volumename})
                        LOG.error(msg)
                        raise exception.VolumeBackendAPIException(data=msg)

                    LOG.debug(_('*****_find_copysession,'
                                'snapshot ElementName:%(elementname)s,'
                                'source_volumename:%(volumename)s')
                               % {'elementname': snapshot_vol_instance['ElementName'],
                                  'volumename': volumename})

                    if volumename == snapshot_vol_instance['ElementName']:
                        #find copysession
                        cpsession = replicarel
                        LOG.debug(_('*****_find_copysession,'
                                    'volumename:%(volumename)s,'
                                    'Storage Synchronized instance:%(sync)s')
                                 % {'volumename': volumename,
                                    'sync': str(cpsession)})
                        msg=(_('_find_copysession,'
                               'source_volumename:%(volumename)s,'
                               'wait for end of copysession')
                              % {'volumename': volumename})
                        LOG.info(msg)

                        try:
                            cpsession_instance = self._get_eternus_instance(
                                replicarel, AllowNone=True)

                            if cpsession_instance is None:
                                break
                            # end of if
                        except:
                            break

                        LOG.debug(_('*****_find_copysession,'
                                    'status:%(status)s')
                                  % {'status':cpsession_instance['CopyState']})
                        if cpsession_instance['CopyState'] == BROKEN:
                            msg=(_('_find_copysession,'
                                   'source_volumename:%(volumename)s,'
                                   'copysession state is BROKEN')
                                  % {'volumename': volumename})
                            LOG.error(msg)
                            raise exception.VolumeBackendAPIException(data=msg)
                        # end of if
                        time.sleep(10)
                        break
                    # end of if
                else:
                    LOG.debug(_('*****_find_copysession,'
                                'volumename:%(volumename)s,'
                                'Storage Synchronized not found.')
                               % {'volumename': volumename})
                # end of for replicarellist
                if cpsession is None:
                    break
            # end of while

            # find copysession where volume is target
            for replicarel in replicarellist['Synchronizations']:
                LOG.debug(_('*****_find_copysession,'
                            'replicarel:%(replicarel)s')
                          % {'replicarel':replicarel})

                # target volume
                try:
                    snapshot_vol_instance = self._get_eternus_instance(
                        replicarel['SyncedElement'],
                        LocalOnly=False)
                except:
                    msg=(_('_find_copysession,'
                           'target_volumename:%(volumename)s,'
                           'GetInstance,'
                           'cannot connect to ETERNUS.')
                          % {'volumename': volumename})
                    LOG.error(msg)
                    raise exception.VolumeBackendAPIException(data=msg)
                LOG.debug(_('*****_find_copysession,'
                            'snapshot ElementName:%(elementname)s,'
                            'volumename:%(volumename)s')
                           % {'elementname': snapshot_vol_instance['ElementName'],
                              'volumename': volumename})

                if volumename == snapshot_vol_instance['ElementName']:
                    # find copysession
                    cpsession = replicarel
                    LOG.debug(_('*****_find_copysession,'
                                'volumename:%(volumename)s,'
                                'Storage Synchronized instance:%(sync)s')
                             % {'volumename': volumename,
                                'sync': str(cpsession)})
                    break
                # end of if

            else:
                LOG.debug(_('*****_find_copysession,'
                            'volumename:%(volumename)s,'
                            'Storage Synchronized not found.')
                           % {'volumename': volumename})
            # end of for replicarellist

        else:
            # does not find target_volume of copysession
            msg = (_('_find_copysession,'
                     'volumename:%(volumename)s,'
                     'not found.')
                    % {'volumename':volumename})
            LOG.info(msg)
        # end of if

        LOG.debug(_('*****_find_copysession,Exit method'))

        return cpsession


    #----------------------------------------------------------------------------------------------#
    # Method : _delete_copysession                                                                 #
    #         summary      : delete copysession                                                    #
    #         return-value :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def _delete_copysession(self,cpsession):
        '''
        delete copysession
        '''
        # cpsession         : copysession
        # snapshot_instance : copysession instance
        # operation         : 8  stop OPC and EC
        #                   : 19 stop SnapOPC
        # repjservice       : FUJITSU_ReplicationService
        # msg               : message
        # rc                : result of invoke method
        # errordesc         : error message
        # job               : unused

        LOG.debug(_('*****_delete_copysession,Entering'))
        LOG.debug(_('*****_delete_copysession,[%s]') % cpsession)

        # initialize
        operation         = 0
        snapshot_instance = None
        repservice        = None
        msg               = None
        rc                = 0
        errordesc         = None
        job               = None

        # main processing

        # get copysession type
        # 4:SnapOPC, 5:OPC
        try:
            snapshot_instance = self._get_eternus_instance(
                cpsession,
                LocalOnly=False,
                AllowNone=True)
        except:
            msg=(_('_delete_copysession,'
                   'copysession:%(cpsession)s,'
                   'GetInstance,'
                   'cannot connect to ETERNUS.')
                  % {'cpsession':cpsession})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        if snapshot_instance is None:
            LOG.info(_('_delete_copysession,'
                        'The copysession was already completed.'))
        else:
            copytype = snapshot_instance['CopyType']

            # set oparation code
            # 19:SnapOPC. 8:OPC
            operation = OPERATION_dic[copytype]

            repservice = self._find_eternus_service(REPL)
            if repservice is None:
                msg = (_('_delete_copysession,'
                         'Cannot find Replication Service to '
                         'delete copysession'))
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            # end of if

            # Invoke method for delete copysession
            rc, errordesc, job = self._exec_eternus_service(
                'ModifyReplicaSynchronization',
                repservice,
                Operation=pywbem.Uint16(operation),
                Synchronization=cpsession,
                Force=True,
                WaitForCopyState=pywbem.Uint16(15))

            LOG.debug(_('*****_delete_copysession,'
                        'copysession:%(cpsession)s,'
                        'operation:%(operation)s,'
                        'Return code:%(rc)lu,'
                        'Error:%(errordesc)s,'
                        'Exit method')
                       % {'cpsession': cpsession,
                          'operation': operation,
                          'rc': rc,
                          'errordesc': errordesc})

            if rc != 0L and rc != 4096L:
                msg = (_('_delete_copysession,'
                         'copysession:%(cpsession)s,'
                         'operation:%(operation)s,'
                         'Return code:%(rc)lu,'
                         'Error:%(errordesc)s')
                        % {'cpsession': cpsession,
                           'operation': operation,
                           'rc': rc,
                           'errordesc': errordesc})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            # end of if
        # end of if
        return

    #----------------------------------------------------------------------------------------------#
    # Method : _get_target_portid                                                                  #
    #         summary      : return tuple of target_portid                                         #
    #         return-value :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def _get_target_portid(self, connector):
        '''
        return target_portid
        '''
        # target_portidlist    : target_portid list
        # tgtportlist          : target port list
        # tgtport              : target port
        # target_cliportidlist : target_portid list (for CLI)
        # lunportlist          : target port list

        LOG.debug(_('*****_get_target_portid,Enter method'))

        # initialize
        target_portidlist    = []
        tgtportlist          = []
        tgtport              = None
        target_cliportidlist = []
        lunportlist          = []

        lunportinslist = self._enum_eternus_instances('FUJITSU_LUNMappingController')

        for lunportins in lunportinslist:
            lunportlist.append(
                    self._conv_port_id(lunportins['CeID'] if lunportins.has_key('CeID') else None,
                                       lunportins['CMSlotNumber'],
                                       lunportins['CASlotNumber'],
                                       lunportins['PortNo']))
        # end of for lunportinslist

        # main processing
        if self.protocol == 'fc':
            # Protocol id FibreChannel
            try:
                tgtportlist = self._enum_eternus_instances(
                    'FUJITSU_SCSIProtocolEndpoint')
            except:
                msg=(_('_get_target_portid,'
                       'connector:%(connector)s,'
                       'EnumerateInstances,'
                       'cannot connect to ETERNUS.')
                      % {'connector':connector})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            for tgtport in tgtportlist:
                cliportid = self._conv_port_id(tgtport['CeID'] if tgtport.has_key('CeID') else None,
                                               tgtport['CMSlotNumber'],
                                               tgtport['CASlotNumber'],
                                               tgtport['PortNo'])
                if (tgtport['ConnectionType'] == 2) and \
                   ((tgtport['RAMode'] & 0x7F) == 0x00 or (tgtport['RAMode'] & 0x7F) == 0x04) and \
                   (not tgtport.has_key('SCGroupNo')) and \
                   (cliportid not in lunportlist):
                    target_portidlist.append(tgtport['Name'])
                    target_cliportidlist.append(cliportid)

                LOG.debug(_('*****_get_target_portid,'
                            'wwn:%(wwn)s,'
                            'connection type:%(cont)s,'
                            'ramode:%(ramode)s')
                           % {'wwn': tgtport['Name'],
                              'cont': tgtport['ConnectionType'],
                              'ramode': tgtport['RAMode']})
            # end of for tgtportlist

            LOG.debug(_('*****_get_target_portid,'
                        'target wwns: %(target_portid)s ')
                       % {'target_portid': target_portidlist})
            LOG.debug(_('*****_get_target_portid,'
                        'target portid: %(target_portid)s ')
                       % {'target_portid': target_cliportidlist})

        elif self.protocol == 'iSCSI':
            # Protocol is iSCSI
            try:
                tgtportlist = self._enum_eternus_instances(
                    'FUJITSU_iSCSIProtocolEndpoint')
            except:
                msg=(_('_get_target_portid,'
                       'connector:%(connector)s,'
                       'EnumerateInstances,'
                       'cannot connect to ETERNUS.')
                      % {'connector':connector})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            for tgtport in tgtportlist:
                cliportid = self._conv_port_id(tgtport['CeID'] if tgtport.has_key('CeID') else None,
                                               tgtport['CMSlotNumber'],
                                               tgtport['CASlotNumber'],
                                               tgtport['PortNo'])
                if (tgtport['ConnectionType'] == 7) and \
                   ((tgtport['RAMode'] & 0x7F) == 0x00 or (tgtport['RAMode'] & 0x7F) == 0x04) and \
                   (not tgtport.has_key('SCGroupNo')) and \
                   (cliportid not in lunportlist):
                    target_portidlist.append(tgtport['Name'])
                    target_cliportidlist.append(cliportid)
                LOG.debug(_('*****_get_target_portid,'
                            'iSCSIname:%(iscsiname)s,'
                            'connection type:%(cont)s,'
                            'ramode: %(ramode)s')
                           % {'iscsiname': tgtport['Name'],
                              'cont': tgtport['ConnectionType'],
                              'ramode': tgtport['RAMode']})
            # end of for tgtportlist

            LOG.debug(_('*****_get_target_portid,'
                        'target iSCSIname: %(target_portid)s ')
                       % {'target_portid': target_portidlist})
            LOG.debug(_('*****_get_target_portid,'
                        'target portid: %(target_portid)s ')
                       % {'target_portid': target_cliportidlist})
        # end of if

        if len(target_portidlist) == 0:
            msg = (_('_get_target_portid,'
                     'protcol:%(protocol)s,'
                     'connector:%(connector)s,'
                     'target_portid does not found.')
                   % {'protocol': self.protocol,
                      'connector': connector})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        LOG.debug(_('*****_get_target_portid,Exit method'))

        return (target_portidlist, target_cliportidlist)


    #----------------------------------------------------------------------------------------------#
    # Method : _map_lun                                                                            #
    #         summary      : map volume to host                                                    #
    #         return-value :                                                                       #
    #----------------------------------------------------------------------------------------------#
    @FJDXLockutils('connect', 'cinder-', True)
    def _map_lun(self, volume, connector, portidlist = []):
        '''
        map volume to host.
        '''
        # vol_instance : volume instance
        # volumename   : volume name on ETERNUS
        # volume_uid   : volume UID
        #                ex)600000E00D110000001104EC00DB0000
        # initiatorlist: inisiator id
        #                ex)[u'10000000c978c574', u'10000000c978c575']
        # aglist       : assigned AffinityGroup list
        # ag           : AffinityGroup
        # configservice: FUJITSU_StorageConfigurationService
        # msg          : message
        # rc           : result of invoke method
        # errordesc    : error message
        # job          : unused
        # volume_lun   : volume LUN
        #                ex)0
        # portidlist   : ETERNUS port id
        #                ex)[u'000', u'100']
        # devid_preset : DeviceID prefix set

        LOG.debug(_('*****_map_lun,'
                    'volume:%(volume)s,'
                    'connector:%(con)s,'
                    'Enter method')
                   % {'volume': volume['display_name'],
                      'con': connector})

        # initialize
        vol_instance  = None
        volumename    = None
        volume_uid    = None
        initiatorlist = []
        aglist        = []
        ag            = None
        configservice = None
        msg           = None
        rc            = 0
        errordesc     = None
        job           = None
        volume_lun    = None
        portid        = None
        devid_preset  = set()

        # main processing
        volumename    = self._create_volume_name(volume['id'])
        vol_instance  = self._find_lun(volume)
        volume_uid    = vol_instance['Name']
        volume_lun    = vol_instance['LUN']
        initiatorlist = self._find_initiator_names(connector)
        aglist        = self._find_affinity_group(connector)
        configservice = self._find_eternus_service(CTRL_CONF)

        if len(portidlist) == 0:
            _x, portidlist = self._get_target_portid(connector)
        # end of if

        if configservice is None:
            msg = (_('_map_lun,'
                     'vol_instance.path:%(vol)s,'
                     'volumename:%(volumename)s,'
                     'volume_uid:%(uid)s,'
                     'volume_lun:%(lun)s,'
                     'initiator:%(initiator)s,'
                     'portid:%(portid)s,'
                     'aglist:%(aglist)s,'
                     'Cannot find Controller Configuration')
                    % {'vol': str(vol_instance.path),
                       'volumename': [volumename],
                       'uid': [volume_uid],
                       'lun': [volume_lun],
                       'initiator': initiatorlist,
                       'portid': portidlist,
                       'aglist': aglist})

            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if
        LOG.debug(_('*****_map_lun,'
                    'vol_instance.path:%(vol)s,'
                    'volumename:%(volumename)s,'
                    'initiator:%(initiator)s,'
                    'portid:%(portid)s')
                  % {'vol': str(vol_instance.path),
                     'volumename': [volumename],
                     'initiator': initiatorlist,
                     'portid': portidlist})

        if len(aglist) == 0:
            initiatorlistwk = [sss.lower() for sss in initiatorlist]
            hostnolist = []
            command = None
            hostname = None
            agnum = None

            if vol_instance.has_key('SCGroupNo'):
                msg = (_('_map_lun,'
                         'vol_instance.path:%(vol)s,'
                         'volumename:%(volumename)s,'
                         'volume_uid:%(uid)s,'
                         'volume_lun:%(lun)s,'
                         '%(errordesc)s')
                        % {'vol': str(vol_instance.path),
                           'volumename': [volumename],
                           'uid': [volume_uid],
                           'lun': [volume_lun],
                           'errordesc': RETCODE_dic['32812']})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            # end of if

            ########################################
            # get host name
            command = None

            if self.protocol == 'iSCSI':
                command = 'show_host_iscsi_names'
            elif self.protocol == 'fc':
                command = 'show_host_wwn_names'
            # end of if

            if command:
                rc, emsg, clidata = self._exec_eternus_cli(command)

                if rc != 0L:
                    msg = (_('_map_lun,'
                             'Return code:%(rc)lu, '
                             'Error code:%(clidata)s, '
                             'Message:%(emsg)s')
                             % {'rc': rc,
                                'clidata': clidata,
                                'emsg': emsg})
                    LOG.error(msg)
                    raise exception.VolumeBackendAPIException(data=msg)
                # end of if

                for hostdata in clidata:
                    if hostdata['Host Name'].lower() in initiatorlistwk:
                        hostnolist.append(str(hostdata['Host Num']))
                        initiatorlistwk.remove(hostdata['Host Name'].lower())
                    # end of if
                # end of for clidata
            # end of if

            ########################################
            # create new host
            if self.protocol == 'iSCSI':
                command = 'create_host_iscsi_name'
                hostname = 'iscsi-name'
            elif self.protocol == 'fc':
                command = 'create_host_wwn_name'
                hostname = 'wwn'
            # end of if

            if command:
                for initiator in initiatorlist:
                    if initiator.lower() in initiatorlistwk:
                        option = {hostname : initiator}
                        rc, emsg, clidata = self._exec_eternus_cli(command, **option)

                        if rc == 0L:
                            try:
                                hostnolist.append(str(int(clidata[0], 16)))
                            except:
                                msg = (_('_map_lun,'
                                         'Return code:%(rc)lu, '
                                         'Error code:%(clidata)s, '
                                         'Message:%(emsg)s')
                                         % {'rc': rc,
                                            'clidata': clidata,
                                            'emsg': emsg})
                                LOG.error(msg)
                                raise exception.VolumeBackendAPIException(data=msg)
                        else:
                            msg = (_('_map_lun,'
                                     'Return code:%(rc)lu, '
                                     'Error code:%(clidata)s, '
                                     'Message:%(emsg)s')
                                     % {'rc': rc,
                                        'clidata': clidata,
                                        'emsg': emsg})
                            LOG.error(msg)
                            raise exception.VolumeBackendAPIException(data=msg)
                        # end of if
                    # end of if
                # end of for initiatorlist
            # end of if

            ########################################
            # create new affinity group
            option = {'volume-number' : volume_lun,
                      'lun' : '0'}
            rc, emsg, clidata = self._exec_eternus_cli('create_affinity_group', **option)

            if rc != 0L:
                msg = (_('_map_lun,'
                         'Return code:%(rc)lu, '
                         'Error code:%(clidata)s, '
                         'Message:%(emsg)s')
                         % {'rc': rc,
                            'clidata': clidata,
                            'emsg': emsg})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            # end of if

            try:
                agnum = int(clidata[0], 16)
            except:
                msg = (_('_map_lun,'
                         'Return code:%(rc)lu, '
                         'Error code:%(clidata)s, '
                         'Message:%(emsg)s')
                         % {'rc': rc,
                            'clidata': clidata,
                            'emsg': emsg})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            if hostnolist:
                ########################################
                # set host affinity
                option = {'ag-number' : agnum,
                          'host-number' : ','.join(hostnolist),
                          'port' : ','.join(portidlist)}
                rc, emsg, clidata = self._exec_eternus_cli('set_host_affinity', **option)

                if rc != 0L:
                    msg = (_('_map_lun,'
                             'Return code:%(rc)lu, '
                             'Error code:%(clidata)s, '
                             'Message:%(emsg)s')
                             % {'rc': rc,
                                'clidata': clidata,
                                'emsg': emsg})
                    LOG.error(msg)

                    ########################################
                    # delete affinity group
                    option = {'ag-number' : agnum}
                    rc, emsg, clidata = self._exec_eternus_cli('delete_affinity_group', **option)

                    if rc != 0L:
                        msg = (_('_map_lun,'
                                 'Return code:%(rc)lu, '
                                 'Error code:%(clidata)s, '
                                 'Message:%(emsg)s')
                                 % {'rc': rc,
                                    'clidata': clidata,
                                    'emsg': emsg})
                        LOG.error(msg)
                    # end of if

                    raise exception.VolumeBackendAPIException(data=msg)
                # end of if
            # end of if
        else:
            # add lun to affinity group
            for ag in aglist:
                LOG.debug(_('*****_map_lun,'
                            'ag:%(ag)s,'
                            'lun_name:%(volume_uid)s')
                           % {'ag': str(ag),
                              'volume_uid':volume_uid})

                devid_pre = ag['DeviceID'][:8]

                if devid_pre not in devid_preset:
                    rc, errordesc, job = self._exec_eternus_service(
                        'ExposePaths',
                        configservice, LUNames=[volume_uid],
                        DeviceAccesses=[pywbem.Uint16(2)],
                        ProtocolControllers=[ag])

                    LOG.debug(_('*****_map_lun,'
                                'Error:%(errordesc)s,'
                                'Return code:%(rc)lu,'
                                'Add lun affinitygroup')
                               % {'errordesc':errordesc,
                                  'rc':rc})

                    if rc != 0L and rc != 4096L:
                        msg = (_('_map_lun,'
                                 'lun_name:%(volume_uid)s,'
                                 'Initiator:%(initiator)s,'
                                 'portid:%(portid)s,'
                                 'Return code:%(rc)lu,'
                                 'Error:%(errordesc)s')
                                % {'volume_uid': [volume_uid],
                                   'initiator': initiatorlist,
                                   'portid': portid,
                                   'rc': rc,
                                   'errordesc':errordesc})
                        LOG.warn(msg)
                    # end of if

                    devid_preset.add(devid_pre)
                # end of if
            # end of for aglist
        # end of if
        LOG.debug(_('*****_map_lun,'
                    'volumename:%(volumename)s,'
                    'Exit method')
                   % {'volumename':volumename})
        return

    #----------------------------------------------------------------------------------------------#
    # Method : _find_initiator_names                                                               #
    #         summary      : return initiator names                                                #
    #         return-value : initiator name                                                        #
    #----------------------------------------------------------------------------------------------#
    def _find_initiator_names(self, connector):
        '''
        return initiator names
        '''
        # initiatornamelist : initiator name
        # msg               : None

        LOG.debug(_('*****_find_initiator_names,Enter method'))

        # initialize
        initiatornamelist = []
        msg               = None

        # main processing
        if self.protocol == 'iSCSI' and connector['initiator'] is not None:
            initiatornamelist.append(connector['initiator'])
        elif self.protocol == 'fc' and connector['wwpns'] is not None:
            initiatornamelist = connector['wwpns']
        # end of if

        if len(initiatornamelist) == 0:
            msg = (_('_find_initiator_names,'
                     'connector:%(connector)s,'
                     'not found initiator')
                    % {'connector':connector})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        LOG.debug(_('*****_find_initiator_names,'
                    'initiator:%(initiator)s.'
                    'Exit method')
                  % {'initiator': initiatornamelist})

        return initiatornamelist


    #----------------------------------------------------------------------------------------------#
    # Method : _find_affinity_group                                                                #
    #         summary      : return affinity group from connector                                  #
    #         return-value : initiator name                                                        #
    #----------------------------------------------------------------------------------------------#
    def _find_affinity_group(self,connector,vol_instance=None):
        '''
        find affinity group from connector
        '''
        # affinity_grouplist: affinity group list(return value)
        # initiatorlist     : initiator list
        # initiator         : initiator
        # aglist            : affinity group list(temp)
        # ag                : affinity group
        # hostaglist        : host affinity group information listr
        # hostag            : host affinity group information

        LOG.debug(_('*****_find_affinity_group,'
                    'Enter method'))

        # initialize
        affinity_grouplist  = []
        initiatorlist       = []
        initiator           = None
        aglist              = []
        ag                  = None
        hostaglist          = []
        hostag              = None

        # main processing
        initiatorlist = self._find_initiator_names(connector)

        if vol_instance is None:
            try:
                aglist = self._enum_eternus_instance_names(
                    'FUJITSU_AffinityGroupController')
            except:
                msg=(_('_find_affinity_group,'
                       'connector:%(connector)s,'
                       'EnumerateInstanceNames,'
                       'cannot connect to ETERNUS.')
                      % {'connector':connector})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            LOG.debug(_('*****_find_affinity_group,'
                        'affinity_groups:%s')
                       % aglist)
        else:
            try:
                aglist = self._assoc_eternus_names(
                    vol_instance.path,
                    AssocClass ='CIM_ProtocolControllerForUnit',
                    ResultClass='FUJITSU_AffinityGroupController')
            except:
                msg=(_('_find_affinity_group,'
                       'connector:%(connector)s,'
                       'AssociatorNames,'
                       'cannot connect to ETERNUS.')
                      % {'connector':connector})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            LOG.debug(_('*****_find_affinity_group,'
                        'vol_instance.path:%(vol)s,'
                        'affinity_groups:%(aglist)s')
                       % {'vol':vol_instance.path,
                          'aglist':aglist})
        # end of if
        for ag in aglist:
            try:
                hostaglist = self._assoc_eternus(
                    ag,
                    AssocClass ='CIM_AuthorizedTarget',
                    ResultClass='FUJITSU_AuthorizedPrivilege')
            except:
                hostaglist = []

            for hostag in hostaglist:
                for initiator in initiatorlist:
                    if initiator.lower() not in hostag['InstanceID'].lower():
                        continue
                    # end of if
                    LOG.debug(_('*****_find_affinity_group,'
                                'AffinityGroup:%(ag)s')
                               % {'ag':ag})
                    affinity_grouplist.append(ag)
                    break
                # end of for initiatorlist
                break
            # end of for hostaglist
        # end of for aglist

        LOG.debug(_('*****_find_affinity_group,'
                    'initiators:%(initiator)s,'
                    'affinity_group:%(affinity_group)s.'
                    'Exit method')
                   % {'initiator': initiatorlist,
                      'affinity_group': affinity_grouplist})

        return affinity_grouplist

    #----------------------------------------------------------------------------------------------#
    # Method : _unmap_lun                                                                          #
    #         summary      : unmap volume from host                                                #
    #         return-value :                                                                       #
    #----------------------------------------------------------------------------------------------#
    @FJDXLockutils('connect', 'cinder-', True)
    def _unmap_lun(self, volume, connector, force=False):
        '''
        unmap volume from host
        '''

        # vol_instance : volume instance
        # volumename   : volume name on ETERNUS
        # volume_uid   : volume UID
        #                ex)600000E00D110000001104EC00DB0000
        # aglist       : assigned AffinityGroup list
        # ag           : AffinityGroup
        # configservice: FUJITSU_StorageConfigurationService
        # msg          : message
        # rc           : result of invoke method
        # errordesc    : error message
        # job          : unused

        LOG.debug(_('*****_unmap_lun,Enter method'))

        # initialize
        vol_instance   = None
        volumename     = None
        volume_uid     = None
        device_number  = None
        configservice  = None
        msg            = None
        aglist         = None
        ag             = None
        msg            = None
        rc             = 0
        errordesc      = None
        job            = None

        # main processing
        volumename    = self._create_volume_name(volume['id'])
        vol_instance  = self._find_lun(volume)
        if vol_instance is None:
            LOG.info(_('_unmap_lun,'
                       'volumename:%(volumename)s,'
                       'volume not found.'
                       'Exit method')
                      % {'volumename':volumename})
            return
        # end of if

        volume_uid    = vol_instance['Name']
        configservice = self._find_eternus_service(CTRL_CONF)

        if force is False:
            try:
                device_number = self._find_device_number(volume, connector)
            except Exception as ex:
                device_number = None

            if device_number is None:
                LOG.info(_('_unmap_lun,'
                           'volumename:%(volumename)s,'
                           'volume is not mapped.'
                           'Exit method')
                          % {'volumename':volumename})
                return
            # end of if

            aglist = self._find_affinity_group(connector,vol_instance)
        else:
            try:
                aglist = self._assoc_eternus_names(
                    vol_instance.path,
                    AssocClass ='CIM_ProtocolControllerForUnit',
                    ResultClass='FUJITSU_AffinityGroupController')
            except:
                msg=(_('_unmap_lun,'
                        'vol_instance.path:%(vol)s,'
                       'AssociatorNames,'
                       'cannot connect to ETERNUS.')
                       % {'vol':vol_instance.path})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            LOG.debug(_('*****_unmap_lun,'
                        'vol_instance.path:%(vol)s,'
                        'affinity_groups:%(aglist)s')
                        % {'vol':vol_instance.path,
                           'aglist':aglist})
        # end of if

        if configservice is None:
            msg = (_('_unmap_lun,'
                     'vol_instance.path:%(vol)s,'
                     'volumename:%(volumename)s,'
                     'volume_uid:%(uid)s,'
                     'aglist:%(aglist)s',
                     'Cannot find Controller Configuration')
                    % {'vol': str(vol_instance.path),
                       'volumename': [volumename],
                       'uid': [volume_uid],
                       'aglist': aglist})

            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        for ag in aglist:
            LOG.debug(_('*****_unmap_lun,'
                        'volumename:%(volumename)s,'
                        'volume_uid:%(volume_uid)s,'
                        'AffinityGroup:%(ag)s')
                       % {'volumename': volumename,
                          'volume_uid': volume_uid,
                          'ag': ag})

            rc, errordesc, job = self._exec_eternus_service(
                'HidePaths',
                configservice,
                LUNames=[volume_uid],
                ProtocolControllers=[ag])

            LOG.debug(_('*****_unmap_lun,'
                        'Error:%(errordesc)s,'
                        'Return code:%(rc)lu')
                       % {'errordesc':errordesc,
                          'rc':rc})

            if rc == 4097L:
                LOG.debug(_('_unmap_lun,'
                           'volumename:%(volumename)s,'
                           'Invalid LUNames')
                          % {'volumename':volumename})
            elif rc != 0L and rc != 4096L:
                msg = (_('_unmap_lun,'
                         'volumename:%(volumename)s,'
                         'volume_uid:%(volume_uid)s,'
                         'AffinityGroup:%(ag)s,'
                         'Return code:%(rc)lu,'
                         'Error:%(errordesc)s')
                        % {'volumename': volumename,
                           'volume_uid': volume_uid,
                           'ag': ag,
                           'rc': rc,
                           'errordesc':errordesc})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            # end of if
        # end of for aglist
        LOG.debug(_('*****_unmap_lun,'
                    'volumename:%(volumename)s,'
                    'Exit method')
                   % {'volumename':volumename})

        return

    #----------------------------------------------------------------------------------------------#
    # Method : _get_iscsi_portal_info                                                              #
    #         summary      : get target iqn                                                        #
    #         return-value : iqn                                                                   #
    #----------------------------------------------------------------------------------------------#
    def _get_iscsi_portal_info(self):
        '''
        get target port iqns and target_portals
        '''
        # iscsiip                          : target iscsi ip address
        # iscsiip_list                     : [iscsiip1, iscsiip2, ...]
        # ip_endpointlist                  : ip protocol endpoint list
        # ip_endpoint                      : ip protocol endpoint
        # target_ip_endpoint_instance_list : target ip protocol endpoint instance list
        # tcp_endpointlist                 : tcp protocol endpoint list
        # tcp_endpoint                     : tcp protocol endpoint
        # iscsi_endpointlist               : iscsi protocol endpoint list
        # iscsi_endpoint                   : iscsi protocol endpoint
        # ip_endpoint_instance             : ip protocol endpoint instance
        # ip_address                       : ip address of ip protocol endpoint instance
        # target_portal                    : iscsi-ip+iscsi-port-no
        # target_portals                   : {'iqn1':[target_portal1,..],'iqn2':[],..}
        # target_iqn                       : iSCSI Qualified Name associated with the volume and the affinitygroup
        # target_iqns                      : [iqn1, iqn2, ..]
        # iqn, portal                      : temporary variable for iqns, target_portals

        LOG.debug(_('*****_get_iscsi_portal_info,Enter method'))

        # initialize
        iscsiip                          = None
        iscsiip_list                     = []
        ip_endpointlist                  = []
        ip_endpoint                      = None
        target_ip_endpoint_instance_list = []
        tcp_endpointlist                 = []
        tcp_endpoint                     = None
        iscsi_endpointlist               = []
        iscsi_endpoint                   = None
        ip_endpoint_instance             = None
        ip_address                       = None
        target_portal                    = None
        target_portals                   = []
        target_iqn                       = None
        target_iqns                      = []
        iqn                              = None
        portal                           = None

        iscsiip      = self._get_valid_iscsi_ip()
        iscsiip_list = self._get_drvcfg('EternusISCSIIP', multiple=True)

        if iscsiip is None:
            iscsiip = self._get_drvcfg('EternusISCSIIP')
        # end of if

        try:
            ip_endpointlist = self._enum_eternus_instance_names(
                'FUJITSU_IPProtocolEndpoint')
        except:
            msg=(_('_get_iscsi_portal_info,'
                   'iscsiip:%(iscsiip)s,'
                   'EnumerateInstanceNames,'
                   'cannot connect to ETERNUS.')
                  % {'iscsiip':iscsiip})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        for ip_endpoint in ip_endpointlist:
            try:
                ip_endpoint_instance = self._get_eternus_instance(
                    ip_endpoint)
                ip_address = ip_endpoint_instance['IPv4Address']
                LOG.debug(_('*****_get_iscsi_portal_info,'
                            'ip_endpoint_instance[IPv4Address]:%(ip_endpoint_instance)s,'
                            'iscsiip:%(iscsiip)s')
                           % {'ip_endpoint_instance':ip_address,
                              'iscsiip':iscsiip})
            except:
                msg=(_('_get_iscsi_portal_info,'
                       'iscsiip:%(iscsiip)s,'
                       'GetInstance,'
                       'cannot connect to ETERNUS.')
                      % {'iscsiip':iscsiip})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            if ip_address in iscsiip_list:
                target_ip_endpoint_instance_list.append(ip_endpoint_instance)
                iscsiip_list.remove(ip_address)

            if len(iscsiip_list) == 0:
                break
        # end of for ip_endpoint

        for ip_endpoint_instance in target_ip_endpoint_instance_list:
            LOG.debug(_('*****_get_iscsi_portal_info,find iscsiip'))

            ip_endpoint = ip_endpoint_instance.path
            ip_address  = ip_endpoint_instance['IPv4Address']

            try:
                tcp_endpointlist = self._assoc_eternus_names(
                    ip_endpoint,
                    AssocClass='CIM_BindsTo',
                    ResultClass='FUJITSU_TCPProtocolEndpoint')
            except:
                msg=(_('_get_iscsi_portal_info,'
                       'iscsiip:%(iscsiip)s,'
                       'AssociatorNames,'
                       'cannot connect to ETERNUS.')
                      % {'iscsiip':iscsiip})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            for tcp_endpoint in tcp_endpointlist:
                try:
                    iscsi_endpointlist = self._assoc_eternus(
                        tcp_endpoint,
                        AssocClass='CIM_BindsTo',
                        ResultClass='FUJITSU_iSCSIProtocolEndpoint')
                except:
                    msg=(_('_get_iscsi_portal_info,'
                           'iscsiip:%(iscsiip)s,'
                           'AssociatorNames,'
                           'cannot connect to ETERNUS.')
                          % {'iscsiip':iscsiip})
                    LOG.error(msg)
                    raise exception.VolumeBackendAPIException(data=msg)

                for iscsi_endpoint in iscsi_endpointlist:
                    iqn    = iscsi_endpoint['Name'].split(',')[0]
                    portal = '%s:%s' % (ip_address, self.configuration.iscsi_port)

                    target_iqns.append(iqn)
                    target_portals.append(portal)

                    if  ip_address == iscsiip:
                        target_iqn    = iqn    
                        target_portal = portal
                        LOG.debug(_('*****_get_iscsi_portal_info,'
                                    'iscsi_endpoint[Name]:%(iscsi_endpoint)s')
                                   % {'iscsi_endpoint':iscsi_endpoint['Name']})
                    break
                # end of for iscsi_endpointlist
                break
            # end of for tcp_endpointlist
        # end of for target_ip_endpoint

        if target_iqn is None:
            msg = (_('_get_iscsi_portal_info,'
                     'iscsiip:%(iscsiip)s,'
                     'not found iqn')
                    % {'iscsiip':iscsiip})

            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of for ip_endpointlist

        LOG.debug(_('*****_get_iscsi_portal_info,%s,Exit method') % iqn )

        return {'target_portal':target_portal,
                'target_portals':target_portals,
                'target_iqn':target_iqn,
                'target_iqns':target_iqns}

    #----------------------------------------------------------------------------------------------#
    # Method : _get_valid_iscsi_ip                                                                 #
    #         summary      : get iSCSI IP which is able to reach iSCSI target                      #
    #         return-value : valid iSCSI IP                                                        #
    #----------------------------------------------------------------------------------------------#
    def _get_valid_iscsi_ip(self):
        '''
        get valid target iSCSI IP address
        '''
        # default_iscsiip   : target iscsi ip address ( default value)
        # ret               : return value

        LOG.debug(_('*****_get_valid_iscsi_ip,Enter method'))

        ret               = None
        default_iscsiip   = self.configuration.iscsi_ip_address

        if default_iscsiip is None:
            default_iscsiip = self._get_drvcfg('EternusISCSIIP')
        # end of if

        # confirm whether default ip address is valid or not
        if self._is_target_alive(default_iscsiip):
            ret = default_iscsiip

        # confirm whether default ip address is valid or not
        if ret is None:
            iscsiip_list = self._get_drvcfg('EternusISCSIIP', multiple=True)
            for iscsiip in iscsiip_list:
                if iscsiip == default_iscsiip:
                    continue
                # end of if

                LOG.info(_("_get_valid_iscsi_ip, Retry iSCSI discovery using %s") % iscsiip)

                # the case of finding valid alternative iscsi ip address
                if self._is_target_alive(iscsiip):
                    self.configuration.iscsi_ip_address = iscsiip
                    ret = iscsiip
                    LOG.info(_("_get_valid_iscsi_ip, Retry iSCSI discovery using %s => Success") % iscsiip)
                    break
                else:
                    LOG.warn(_("_get_valid_iscsi_ip, Retry iSCSI discovery using %s => Failure") % iscsiip)
                # end of if
            else:
                msg = (_('_get_valid_iscsi_ip,'
                         'iscsiip:%(iscsiip_list)s,'
                         'All iSCSI IP in configuration file are invalid')
                         % {'iscsiip_list':iscsiip_list})
                LOG.error(msg)
            # end of for iscsiip_list
        # enf of if

        LOG.debug(_('*****_is_get_valid_iscsi,%s,Exit method') % ret )
        return ret


    #----------------------------------------------------------------------------------------------#
    # Method : _is_target_alive                                                                    #
    #         summary      : confirm whether the target is alive and valid iSCSI target or not     #
    #         return-value : True/False                                                            #
    #----------------------------------------------------------------------------------------------#
    def _is_target_alive(self, ip):
        '''
        confirm whether target is alive or not
        '''
        # ret       : return value

        LOG.debug(_('*****_is_target_alive,Enter method'))

        # initialize
        ret = None

        for i in range(3):
            try:
                (out, _err) = utils.execute('ping', '-c', '1', ip)
                break
            except processutils.ProcessExecutionError as ex:
                continue
        else:
            LOG.warn(_('_is_target_alive, target(%(ip)s) did not respond to icmp')
                       % {'ip':ip})
            ret = False
        # end of for range(3)

        if ret is not False:
            try:
                (out, _err) = utils.execute('iscsiadm', '-m', 'discovery',
                                            '-t', 'sendtargets', '-p',
                                            ip, run_as_root=True)
                ret = True
            except processutils.ProcessExecutionError as ex:
                ret = False
                LOG.warn(_("_is_target_alive, iSCSI discovery was failed: %(msg)s")
                           % {'msg':ex.stderr})
        # end of if

        LOG.debug(_('*****_is_target_alive,%s,Exit method') % ret )
        return ret

    #----------------------------------------------------------------------------------------------#
    # Method : _add_image_volume_info                                                              #
    #         summary      : add image volume information to image management cfg                  #
    #         return-value :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def _add_image_volume_info(self, volume_id, volume_size, volume_path, image_id, storage_name, use_format):
        '''
        add image volume information to image management file
        '''
        # image_management_file  : management file name for image volume
        # doc                    : xml document class
        # root                   : xml document root
        # image                  : Image Information (Image ID, Image Volume Information)

        LOG.debug(_('*****_add_image_volume_info,Enter method'))

        # initialize
        image_management_file = None
        doc                   = None
        root                  = None
        image                 = None

        # main processing
        image_management_file = self.configuration.fujitsu_image_management_file

        # if file is not exist, then make formatted file
        if not os.path.exists(image_management_file):
            LOG.debug(_('*****_add_image_volume_info, create new management file'))
            doc = xml.dom.minidom.Document()
            root = doc.createElement('FUJITSU')
            doc.appendChild(root)

            f = codecs.open(image_management_file, 'wb', 'UTF-8')
            doc.writexml(f,'','  ','\n',encoding='UTF-8')
            f.close()

        # add image volume information
        doc = xml.dom.minidom.parse(image_management_file)
        root = doc.documentElement
        image = doc.getElementsByTagName('Image')

        for img in image:
            f_image_id = img.getElementsByTagName('ImageID')[0].childNodes[0].data
            if image_id != f_image_id:
                continue

            volume = doc.createElement('Volume')
            img.appendChild(doc.createTextNode('\n   '))
            img.appendChild(volume)
            break
        else:
            image = doc.createElement('Image')
            root.appendChild(doc.createTextNode('\n '))
            root.appendChild(image)

            f_image_id = doc.createElement('ImageID')
            image.appendChild(f_image_id)
            f_image_id.appendChild(doc.createTextNode(image_id))

            volume = doc.createElement('Volume')
            image.appendChild(doc.createTextNode('\n   '))
            image.appendChild(volume)
        # end of if

        f_volume_id = doc.createElement('VolumeID')
        volume.appendChild(f_volume_id)
        f_volume_id.appendChild(doc.createTextNode(volume_id))

        f_volume_size = doc.createElement('VolumeSize')
        volume.appendChild(f_volume_size)
        f_volume_size.appendChild(doc.createTextNode(str(volume_size)))

        f_volume_path = doc.createElement('VolumePath')
        volume.appendChild(f_volume_path)
        f_volume_path.appendChild(doc.createTextNode(volume_path))

        f_storage_name = doc.createElement('StorageName')
        volume.appendChild(f_storage_name)
        f_storage_name.appendChild(doc.createTextNode(storage_name))

        f_session = doc.createElement('Session')
        volume.appendChild(f_session)
        f_session.appendChild(doc.createTextNode('0'))

        f_format = doc.createElement('Format')
        volume.appendChild(f_format)
        f_format.appendChild(doc.createTextNode(str(use_format)))

        f = codecs.open(image_management_file, 'wb', 'UTF-8')
        doc.writexml(f,encoding='UTF-8')
        f.close()
        LOG.debug(_('*****_add_image_volume_info'
                    'image_management_file:%(image_management_file)s,'
                    'image_id:%(image_id)s,'
                    'volume_id:%(volume_id)s,'
                    'volume_size:%(volume_size)s,'
                    'storage_name:%(storage_name)s')
                    % {'image_management_file':image_management_file,
                       'image_id':image_id,
                       'volume_id':volume_id,
                       'volume_size':str(volume_size),
                       'storage_name':storage_name})
        LOG.debug(_('*****_add_image_volume_info,Exit method'))

    #----------------------------------------------------------------------------------------------#
    # Method : _update_image_volume_info                                                           #
    #         summary      : update opc copy session information for image volume                  #
    #         return-value :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def _update_image_volume_info(self, volume_id, image_id, value=None, remove=False):
        '''
        update image volume information in image management file
        '''
        # image_management_file  : management file name for image volume
        # doc                    : xml document class
        # root                   : xml document root
        # f_image                : Image Information (Image ID, Image Volume Information)
        # f_volume               : Volume Information (Volume ID, Session)

        LOG.debug(_('*****_update_image_volume_info,Enter method'))

        # initialize
        image_management_file = None
        doc                   = None
        root                  = None
        f_image               = None
        f_volume              = None

        # main processing
        image_management_file = self.configuration.fujitsu_image_management_file
        doc = xml.dom.minidom.parse(image_management_file)
        root = doc.documentElement

        for f_img in doc.getElementsByTagName('Image'):
            f_image_id = f_img.getElementsByTagName('ImageID')[0].childNodes[0].data
            if f_image_id == image_id:
                f_image = f_img
                break
            # end of if
        # end of for image

        if f_image is not None:
            for f_vol in f_image.getElementsByTagName('Volume'):
                f_volume_id = f_vol.getElementsByTagName('VolumeID')[0].childNodes[0].data
                if f_volume_id == volume_id:
                    f_volume = f_vol
                    break
                # end of if
            # end of for volume
        # end of if

        if f_volume is not None:
            if remove is False:
                f_session = f_volume.getElementsByTagName('Session')

                if value is None:
                    f_session_num = str(int(f_session[0].childNodes[0].data) + 1)
                else:
                    f_session_num = value
                # end of if

                f_session[0].childNodes[0].data = f_session_num
                LOG.debug(_('*****_update_image_volume_info, update,'
                            'image_id:%(image_id)s,'
                            'volume_id:%(volume_id)s,'
                            'session_num:%(session_num)s')
                            % {'image_id':f_image_id,
                               'volume_id':f_volume_id,
                               'session_num':f_session_num})
            else:
                f_volume.previousSibling.data=''
                f_image.removeChild(f_volume)
                LOG.debug(_('*****_update_image_volume_info, remove,'
                            'image_id:%(image_id)s,'
                            'volume_id:%(volume_id)s,')
                            % {'image_id':f_image_id,
                               'volume_id':f_volume_id})
            # end of if

            f = codecs.open(image_management_file, 'wb', 'UTF-8')
            doc.writexml(f,encoding='UTF-8')
            f.close()
        # end of if

        LOG.debug(_('*****_update_image_volume_info,Exit method'))

    #----------------------------------------------------------------------------------------------#
    # Method : _get_sessionnum_by_srcvol                                                           #
    #         summary      : get the number of session where specified volume is source            #
    #         return-value : the number of session                                                 #
    #----------------------------------------------------------------------------------------------#
    def _get_sessionnum_by_srcvol(self, volume):
        '''
        get the number of session where specified volume is source
        '''
        # vol_instance     : volume instance
        # all_session_info : information list of session where specified volume is included
        # session_info     : information list of session where specified volume is source
        # session_num      : the number of session

        LOG.debug(_('*****_get_sessionnum_by_srcvol,Enter method'))

        # initialize
        vol_instance     = None
        all_session_info = []
        session_info     = []
        session_num      = 0

        # main processing
        vol_instance = self._find_lun(volume)
        all_session_info = self._reference_eternus_names(
                              vol_instance.path,
                              ResultClass='FUJITSU_StorageSynchronized')

        for session in all_session_info:
            if vol_instance.path != session['SyncedElement']:
                session_info.append(session)
           # end of if
        # end of for all_session_info

        session_num = len(session_info)
        LOG.debug(_('*****_get_sessionnum_by_srcvol,'
                    ' session_num:%(session_num)s,'
                    ' session_info:%(session_info)s')
                   % {'session_num':session_num,
                      'session_info':session_info})

        LOG.debug(_('*****_get_sessionnum_by_srcvol,Exit method'))
        return session_num

    #----------------------------------------------------------------------------------------------#
    # Method : _check_user                                                                         #
    #         summary      : check whether user's role is accessible to ETERNUS and Software       #
    #         return-value : True/False                                                            #
    #----------------------------------------------------------------------------------------------#
    def _check_user(self):
        '''
        check whether user's role is accessible to ETERNUS and Software
        '''
        # ret : return value (True/False)

        LOG.debug(_('*****_check_user,Enter method'))

        ret = True
        rc, errordesc, job = self._exec_eternus_cli(
                'check_user_role')
        if rc != 0L:
            msg = (_('_check_user,'
                     'Return code:%(rc)lu, '
                     'Error:%(errordesc)s, '
                     'Job:%(job)s')
                     % {'rc': rc,
                        'errordesc': errordesc,
                        'job': job})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        if job != 'Software':
            msg = (_('_check_user,'
                     'Specified user(%(user)s) does not have Software role: %(role)s')
                    % {'user': self._get_drvcfg('EternusUser'),
                       'role': job})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        LOG.debug(_('*****_check_user,Exit method'))
        return ret

    #----------------------------------------------------------------------------------------------#
    # Method : _set_qos                                                                            #
    #         summary      : set qos using ETERNUS CLI                                             #
    #         return-value :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def _set_qos(self, volume):
        '''
        set qos using ETERNUS CLI
        '''
        # qos_specs_dict : qos specs
        # category       : qos category value

        LOG.debug(_('*****_set_qos,Enter method'))

        # initialize
        qos_specs_dict = {}
        category = 0

        # main processing
        qos_specs_dict = self._get_qos_specs(volume)
        for key, value in qos_specs_dict.iteritems():
            if key in FJ_QOS_KEY_list:
                category = self._get_qos_category_by_value(key, value)
                break
            # end of if
        # end of for

        if category > 0:
            volumename = self._create_volume_name(volume['id'])
            param_dict = {'volume-name': volumename, 'bandwidth-limit': str(category)}
            rc, errordesc, job = self._exec_eternus_cli(
                'set_volume_qos',
                **param_dict)
            if rc != 0L:
                msg = (_('_set_qos,'
                         'Return code:%(rc)lu, '
                         'Error:%(errordesc)s, '
                         'Job:%(job)s')
                         % {'rc': rc,
                            'errordesc': errordesc,
                            'job': job})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
     

        LOG.debug(_('*****_set_qos,Exit method'))
        return


    #----------------------------------------------------------------------------------------------#
    # Method : _get_extra_specs                                                                    #
    #         summary      : get extra specs information from volume information                   #
    #         return-value : extra specs information                                               #
    #----------------------------------------------------------------------------------------------#
    def _get_extra_specs(self, volume, key=None, default=None):
        '''
        get extra specs information from volume information
        '''
        # ctxt              : context
        # volume_type_id    : volume type id
        # volume_type       : volume type
        # extra_specs       : extra specs
        # extra_specs_value : extra specs value using specified key
        # ret               : return value

        LOG.debug(_('*****_get_extra_specs,Enter method'))

        # initialize
        ctxt           = None
        volume_type_id = None
        volume_type    = {}
        extra_specs    = {}
        ret            = default

        # main processing
        volume_type_id = volume.get('volume_type_id')

        if volume_type_id is not None:
            ctxt = context.get_admin_context()
            volume_type = volume_types.get_volume_type(ctxt, volume_type_id)
            extra_specs = volume_type.get('extra_specs')

        if extra_specs:
            if key is None:
                ret = extra_specs
            else:
                ret = extra_specs.get(key, default)
            # end of if
        # end of if

        LOG.debug(_('*****_get_extra_specs,Exit method'))
        return ret

    #----------------------------------------------------------------------------------------------#
    # Method : _get_qos_specs                                                                      #
    #         summary      : get qos specs information from volume information                     #
    #         return-value : qos specs information                                                 #
    #----------------------------------------------------------------------------------------------#
    def _get_qos_specs(self, volume):
        '''
        get qos specs information from volume information
        '''
        # ctxt           : context
        # volume_type_id : volume type id
        # volume_type    : volume type
        # qos_specs_dict : qos specs
        # qos_specs_id   : qos specs id

        LOG.debug(_('*****_get_qos_specs,Enter method'))

        # initialize
        ctxt           = None
        volume_type_id = None
        volume_type    = {}
        qos_specs_dict = {}
        qos_specs_id   = None

        # main processing
        volume_type_id = volume.get('volume_type_id')

        if volume_type_id is not None:
            ctxt = context.get_admin_context()
            volume_type = volume_types.get_volume_type(ctxt, volume_type_id)
            qos_specs_id = volume_type.get('qos_specs_id')

        if qos_specs_id is not None:
            qos_specs_dict = qos_specs.get_qos_specs(ctxt, qos_specs_id)['specs']

        LOG.debug(_('*****_get_qos_specs,Exit method'))
        return qos_specs_dict

    #----------------------------------------------------------------------------------------------#
    # Method : _get_qos_category_by_value                                                          #
    #         summary      : get qos category  using value                                         #
    #         return-value : qos category value                                                    #
    #----------------------------------------------------------------------------------------------#
    def _get_qos_category_by_value(self, key, value):
        '''
        get qos category  using value
        '''
        LOG.debug(_('*****_get_qos_category_by_value,Enter method'))

        def _get_qos_category_by_value_error():
            msg = (_('_get_qos_category_by_value,'
                     'Invalid value is input,'
                     'key: %(key)s,'
                     'value: %(value)s')
                    % {'key': key,
                       'value': value})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of def

        ret = 0
        if (key == "maxBWS"):
            try:
                digit = int(float(value))
            except:
                _get_qos_category_by_value_error()

            if digit >= 800:
                ret = 1
            elif digit >= 700:
                ret = 2
            elif digit >= 600:
                ret = 3
            elif digit >= 500:
                ret = 4
            elif digit >= 400:
                ret = 5
            elif digit >= 300:
                ret = 6
            elif digit >= 200:
                ret = 7
            elif digit >= 100:
                ret = 8
            elif digit >= 70:
                ret = 9
            elif digit >= 40:
                ret = 10
            elif digit >= 25:
                ret = 11
            elif digit >= 20:
                ret = 12
            elif digit >= 15:
                ret = 13
            elif digit >= 10:
                ret = 14
            elif digit > 0:
                ret = 15
            else:
                _get_qos_category_by_value_error()
            # end of if
        # end of if

        LOG.debug(_('*****_get_qos_category_by_value (%s),Exit method') % str(ret))
        return ret

    #----------------------------------------------------------------------------------------------#
    # Method : _get_metadata                                                                       #
    #         summary      : get metadata using volume information                                 #
    #         return-value : dictionary : {'meta-key':'meta-value', ...}                           #
    #----------------------------------------------------------------------------------------------#
    def _get_metadata(self, volume):
        '''
        get metadata using volume information and keys
        '''
        # ret      : return value ( dictionary format style metadata )
        # metadata : metadata

        LOG.debug(_('*****_get_metadata,Exit method'))

        # initialize
        ret      = {}
        metadata = []

        # main processing
        metadata = volume.get('volume_metadata', [])
        if metadata is None:
            metadata = [] 
        # end of if

        for data in metadata:
            ret[data['key']] = data['value']
        # end of if


        LOG.debug(_('*****_get_metadata,Exit method'))
        return ret

    #----------------------------------------------------------------------------------------------#
    # Method : _get_bool                                                                           #
    #         summary      : get True/False from String 'True'/'False'                             #
    #         return-value : True/False                                                            #
    #----------------------------------------------------------------------------------------------#
    def _get_bool(self, value):
        '''
        get True/False from String 'True'/'False'
        '''
        LOG.debug(_('*****_get_bool,Exit method'))
        ret = False

        if str(value).lower() == "true":
            ret = True
        # end of if

        LOG.debug(_('*****_get_bool,Exit method'))
        return ret

    #----------------------------------------------------------------------------------------------#
    # Method : _conv_port_id                                                                       #
    #         summary      : converted to port id                                                  #
    #         parameters   : ce_id, cm_slot_num, ca_slot_num, ca_port_num                          #
    #         return-value : port id                                                               #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def _conv_port_id(self, ce_id, cm_slot_num, ca_slot_num, ca_port_num):
        if ce_id is None:
            port_id = '%x%x%x' % (cm_slot_num, ca_slot_num, ca_port_num)
        else:
            port_id = '%x%x%x%x' % (ce_id, cm_slot_num, ca_slot_num, ca_port_num)
        # end of if

        return port_id


# Copyright (C) 2012 Hewlett-Packard Development Company, L.P.
# Copyright (c) 2014 TrilioData, Inc
# Copyright (c) 2015 EMC Corporation
# Copyright (C) 2015 Kevin Fox <kevin@efox.cc>
# Copyright (C) 2015 Tom Barron <tpb@dyncloud.net>
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

"""Implementation of a backup service that uses oNest as the backend
**Related Flags**
"""

import os
import sys

from cinder import exception
from cinder.i18n import _, _LI, _LE
from cinder.backup import chunkeddriver
from onestsdk import onest_client
from onestsdk import onest_common
from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import timeutils

LOG = logging.getLogger(__name__)

oNestbackup_service_opts = [
    cfg.IntOpt('backup_oNest_object_size',
               default=52428800,
               help='The size in bytes of oNest backup objects.'),
    cfg.IntOpt('backup_oNest_block_size',
               default=32768,
               help='The size in bytes that changes are tracked '
                    'for incremental backups. backup_oNest_object_size '
                    'has to be multiple of backup_oNest_block_size.'),
    cfg.BoolOpt('backup_oNest_enable_progress_timer',
                default=True,
                help='Enable or Disable the timer to send the periodic '
                     'progress notifications to Ceilometer when backing '
                     'up the volume to the oNest backend storage. The '
                     'default value is True to enable the timer.'),
    cfg.StrOpt('backup_oNest_container',
               default='volumebackups',
               help='The default oNest container to use.'),
    cfg.StrOpt('auth_protocol_version',
               default='CMCC',
               help='Auth protocol version. '
                    'This version only "CMCC"'),
    cfg.StrOpt('accesskey',
               default=None,
               help='Access key.'),
    cfg.StrOpt('secretkey',
               default=None,
               help='Secret key.'),
    cfg.BoolOpt('is_secure',
               default=False,
               help='Whether to use https access.'),
    cfg.BoolOpt('is_random_access_addr',
               default=False,
               help='Whether accessed via a random address, '
                    'we need to modify the corresponding host.'),
    cfg.StrOpt('oNesthost',
               default=None,
               help='Host address, like:10.24.1.48:18080.'),
    cfg.IntOpt('access_net_mode',
               default=3,
               help='Get LVS address mode, divided into three types: '
                    '1:Obtain the business address of the internal network. '
                    '2:Obtain the management address of the internal network. '
                    '3:Obtain external network address.'),
]

CONF = cfg.CONF
CONF.register_opts(oNestbackup_service_opts)

reload(sys)
sys.setdefaultencoding('utf8')

class oNestBackupDriver(chunkeddriver.ChunkedBackupDriver):
    """Provides backup, restore and delete of backup objects within oNest."""

    def __init__(self, context, db_driver=None):
        chunk_size_bytes = CONF.backup_oNest_object_size
        sha_block_size_bytes = CONF.backup_oNest_block_size
        backup_default_container = CONF.backup_oNest_container
        enable_progress_timer = CONF.backup_oNest_enable_progress_timer
        super(oNestBackupDriver, self).__init__(context, chunk_size_bytes,
                                                sha_block_size_bytes,
                                                backup_default_container,
                                                enable_progress_timer,
                                                db_driver)

        # Get oNest configuration info from cinder.conf
        auth_protocol_version = CONF.auth_protocol_version
        accesskey = CONF.accesskey
        secretkey = CONF.secretkey
        is_secure = CONF.is_secure
        is_random_access_addr = CONF.is_random_access_addr
        oNesthost = CONF.oNesthost
        access_net_mode = CONF.access_net_mode

        if not accesskey or not secretkey or not oNesthost:
            raise exception.BackupDriverException(_(
                'Please check the cinder.conf to make sure '
                'configure accesskey, secretkey and oNesthost.'))

        authinfo = onest_common.AuthInfo(
            auth_protocol_version, accesskey, secretkey,
            is_secure, is_random_access_addr, oNesthost, access_net_mode);
        self.onest = onest_client.OnestClient(authinfo)

    class oNestObjectWriter(object):
        def __init__(self, container, object_name, onest):
            self.container = container
            self.object_name = object_name
            self.onest = onest
            self.data = ''

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            self.close()

        def write(self, data):
            self.data += data

        def close(self):
            path = os.path.join(r'/tmp/', self.object_name)
            if not os.path.exists(path):
                with open(path, 'w') as file_object:
                    file_object.write(self.data)
            else:
                err = _('The tmp file %s exist, create failed!'), path
                LOG.error(err)
                raise exception.InvalidBackup(reason=err)

            file_obj = file(path)
            obj = onest_common.OnestObject(file_obj, {})
            if self.onest.put_object(self.container, self.object_name, obj.data):
                LOG.debug('Success to write object:%s.', self.object_name)
            else:
                err = _('oNestObjectWriter write object error!')
                LOG.error(err)
                raise exception.InvalidBackup(reason=err)

            if os.path.exists(path):
                os.remove(path)

    class oNestObjectReader(object):
        def __init__(self, container, object_name, onest):
            self.container = container
            self.object_name = object_name
            self.onest = onest

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            pass

        def read(self):
            data = self.onest.get_object_data(self.container, self.object_name)
            if data:
                LOG.debug('Read object successful, name:%s.', self.object_name)
                return data
            else:
                err = _('Read object failed!')
                LOG.error(err)
                raise exception.InvalidBackup(reason=err)

    def put_container(self, container):
        """Create the container if needed. No failure if it pre-exists."""
        if self.onest.create_bucket(container):
            LOG.debug('Container:%s not exist, create successful.', container)
        else:
            # If return false, means exist
            LOG.info(_LI('Container:%s exist, just use it.'), container)

    def get_container_entries(self, container, prefix):
        """Get container entry names."""
        response = self.onest.list_objects_of_bucket(container, {'prefix': prefix})
        if response:
            object_names = []
            for entry in response.entries:
                object_name = self._get_object_name(entry.object_uri)
                if object_name:
                    object_names.append(object_name)
            LOG.debug('Get container object names successful, names:%s.', object_names)
            return object_names
        else:
            err = _('Get container object names failed!')
            LOG.error(err)
            raise exception.InvalidBackup(reason=err)

    def get_object_writer(self, container, object_name, extra_metadata=None):
        """Returns a writer object that stores a chunk of volume data in a
           oNest object store.
        """
        return self.oNestObjectWriter(container, object_name, self.onest)

    def get_object_reader(self, container, object_name, extra_metadata=None):
        """Returns a reader object for the backed up chunk."""
        return self.oNestObjectReader(container, object_name, self.onest)

    def delete_object(self, container, object_name):
        """Delete object from container."""
        if self.onest.delete_object(container, object_name):
            LOG.debug('Delete object:%s successful.', object_name)
        else:
            err = _('Delete object:%s failed!'), object_name
            LOG.error(err)
            raise exception.InvalidBackup(reason=err)

    def _generate_object_name_prefix(self, backup):
        """Generates a oNest backup object name prefix.
           Warning: oNest Object name has a limited length.
        """ 
        timestamp = timeutils.strtime(fmt="%Y%m%d%H%M%S")
        prefix = timestamp + '_' + backup['id']
        LOG.debug('Object name prefix: %s.', prefix)
        return prefix

    def update_container_name(self, backup, container):
        """This method exists so that sub-classes can override the container name
           as it comes in to the driver in the backup object.  Implementations
           should return None if no change to the container name is desired.
        """
        return container

    def get_extra_metadata(self, backup, volume):
        """This method allows for collection of extra metadata in prepare_backup()
           which will be passed to get_object_reader() and get_object_writer().
           Subclass extensions can use this extra information to optimize
           data transfers.  Return a json serializable object.
        """
        return None

    def _get_object_name(self, object_url):
        """Get object name from oNest object url.
           If object url change to another type, this method need to modify too.
           This version the type shows as follow:
                container name/folder name/.../file name
            Examples:
                bucket/1M.txt
                bucket/folder1/
                bucket/folder1/folder2/
                bucket/test15.rar
        """
        infos = str(object_url).split('/')
        return infos[len(infos) - 1]

def get_backup_driver(context):
    return oNestBackupDriver(context)

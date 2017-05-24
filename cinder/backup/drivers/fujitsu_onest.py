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

import eventlet
import hashlib
import os
import sys

from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import timeutils
from oslo_utils import units
from oslo_utils import excutils

from cinder import exception
from cinder.i18n import _, _LI, _LE
from cinder.backup import chunkeddriver
from cinder.openstack.common import loopingcall

from pythonsdk import onest_client
from pythonsdk import onest_common

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
            is_secure, is_random_access_addr, oNesthost, access_net_mode)
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
                err = (_('oNestObjectWriter, '
                         'the tmp file(%(path)s) exist, create failed! '
                         'Container: %(container)s, object: %(obj_name)s.'),
                       {'container': self.container,
                        'obj_name': self.object_name,
                        'path': path})
                LOG.error(err)
                raise exception.InvalidBackup(reason=err)

            file_obj = file(path)
            obj = onest_common.OnestObject(file_obj, {})
            if self.onest.put_object(
                    self.container, self.object_name, obj.data):
                LOG.debug('oNestObjectWriter, write success. '
                          'Container: %(container)s, object: %(obj_name)s.',
                          {'container': self.container,
                           'obj_name': self.object_name})
            else:
                err = (_('oNestObjectWriter, write failed! '
                         'Container: %(container)s, object: %(obj_name)s.'),
                       {'container': self.container,
                        'obj_name': self.object_name})
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
                LOG.debug('oNestObjectReader, read success. '
                          'Container: %(container)s, object: %(obj_name)s.',
                          {'container': self.container,
                           'obj_name': self.object_name})
                return data
            else:
                err = (_('oNestObjectReader, read failed! '
                         'Container: %(container)s, object: %(obj_name)s.'),
                       {'container': self.container,
                        'obj_name': self.object_name})
                LOG.error(err)
                raise exception.InvalidBackup(reason=err)

    def put_container(self, container):
        """Create the container if needed. No failure if it pre-exists."""
        if self.onest.create_bucket(container):
            LOG.debug('put_container, create success. '
                      'Container: %s.', container)
        else:
            # If return false, means exist
            LOG.info(_LI('put_container, '
                         'container(%s) exist, just use it.'), container)

    def get_container_entries(self, container, prefix):
        """Get container entry names."""
        response = self.onest.list_objects_of_bucket(
            container, {'prefix': prefix})
        if response:
            object_names = []
            for entry in response.entries:
                object_name = self._get_object_name(entry.object_uri)
                if object_name:
                    object_names.append(object_name)
            LOG.debug('Get container object names successful. '
                      'Container: %(container)s, names: %(names)s.',
                      {'container': container,
                       'names': object_names})
            return object_names
        else:
            err = (_('Get container object names failed! '
                     'Container: %(container)s, prefix: %(prefix)s.'),
                   {'container': container,
                    'prefix': prefix})
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
            LOG.debug('Delete object success. '
                      'Container: %(container)s, object: %(object_name)s.',
                      {'container': container,
                       'object_name': object_name})
        else:
            err = (_('Delete object failed! '
                     'Container: %(container)s, object: %(object_name)s.'),
                   {'container': container,
                    'object_name': object_name})
            LOG.error(err)
            raise exception.InvalidBackup(reason=err)

    def _generate_object_name_prefix(self, backup):
        """Generates a oNest backup object name prefix.
           Warning: oNest Object name has a limited length.
        """
        timestamp = timeutils.strtime(fmt="%Y%m%d%H%M%S")
        prefix = timestamp + '_' + backup['id']
        LOG.debug('Object name prefix: %(prefix)s, backup id: %(bk_id)s.',
                  {'prefix': prefix,
                   'bk_id': backup['id']})
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
           If object url change to another type, this method need to modify.
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

    def backup(self, backup, volume_file, backup_metadata=True):
        """Backup the given volume.

           If backup['parent_id'] is given, then an incremental backup
           is performed.
        """
        if self.chunk_size_bytes % self.sha_block_size_bytes:
            err = _('Chunk size is not multiple of '
                    'block size for creating hash.')
            raise exception.InvalidBackup(reason=err)

        # Read the shafile of the parent backup if backup['parent_id']
        # is given.
        parent_backup_shafile = None
        parent_backup = None
        if backup['parent_id']:
            parent_backup = self.db.backup_get(self.context,
                                               backup['parent_id'])
            parent_backup_shafile = self._read_sha256file(parent_backup)
            parent_backup_shalist = parent_backup_shafile['sha256s']
            if (parent_backup_shafile['chunk_size'] !=
                    self.sha_block_size_bytes):
                err = (_('Hash block size has changed since the last '
                         'backup. New hash block size: %(new)s. Old hash '
                         'block size: %(old)s. Do a full backup.')
                       % {'old': parent_backup_shafile['chunk_size'],
                          'new': self.sha_block_size_bytes})
                raise exception.InvalidBackup(reason=err)
            # If the volume size increased since the last backup, fail
            # the incremental backup and ask user to do a full backup.
            if backup['size'] > parent_backup['size']:
                err = _('Volume size increased since the last '
                        'backup. Do a full backup.')
                raise exception.InvalidBackup(reason=err)

        (object_meta, object_sha256, extra_metadata, container,
         volume_size_bytes) = self._prepare_backup(backup)

        counter = 0
        total_block_sent_num = 0

        # There are two mechanisms to send the progress notification.
        # 1. The notifications are periodically sent in a certain interval.
        # 2. The notifications are sent after a certain number of chunks.
        # Both of them are working simultaneously during the volume backup,
        # when swift is taken as the backup backend.
        def _notify_progress():
            self._send_progress_notification(self.context, backup,
                                             object_meta,
                                             total_block_sent_num,
                                             volume_size_bytes)
        timer = loopingcall.FixedIntervalLoopingCall(
            _notify_progress)
        if self.enable_progress_timer:
            timer.start(interval=self.backup_timer_interval)

        sha256_list = object_sha256['sha256s']
        shaindex = 0
        length_bytes = 0
        write_length_bytes = 0
        index = 1
        while True:
            data_offset = volume_file.tell()
            data = volume_file.read(self.chunk_size_bytes)
            if data == '':
                break

            # Calculate new shas with the datablock.
            shalist = []
            off = 0
            datalen = len(data)
            while off < datalen:
                chunk_start = off
                chunk_end = chunk_start + self.sha_block_size_bytes
                if chunk_end > datalen:
                    chunk_end = datalen
                chunk = data[chunk_start:chunk_end]
                sha = hashlib.sha256(chunk).hexdigest()
                shalist.append(sha)
                off += self.sha_block_size_bytes
            sha256_list.extend(shalist)

            # If parent_backup is not None, that means an incremental
            # backup will be performed.
            if parent_backup:
                # Find the extent that needs to be backed up.
                extent_off = -1
                for idx, sha in enumerate(shalist):
                    if sha != parent_backup_shalist[shaindex]:
                        if extent_off == -1:
                            # Start of new extent.
                            extent_off = idx * self.sha_block_size_bytes
                    else:
                        if extent_off != -1:
                            # We've reached the end of extent.
                            extent_end = idx * self.sha_block_size_bytes
                            segment = data[extent_off:extent_end]
                            backup_bytes = self._backup_chunk(
                                backup, container, segment,
                                data_offset + extent_off,
                                object_meta, extra_metadata)
                            length_bytes += len(segment)
                            write_length_bytes += backup_bytes
                            extent_off = -1
                    shaindex += 1

                # The last extent extends to the end of data buffer.
                if extent_off != -1:
                    extent_end = datalen
                    segment = data[extent_off:extent_end]
                    backup_bytes = self._backup_chunk(
                        backup, container, segment,
                        data_offset + extent_off,
                        object_meta, extra_metadata)
                    length_bytes += len(segment)
                    write_length_bytes += backup_bytes
                    extent_off = -1
            else:  # Do a full backup.
                backup_bytes = self._backup_chunk(
                    backup, container, data, data_offset,
                    object_meta, extra_metadata)
                length_bytes += len(data)
                write_length_bytes += backup_bytes

            # Notifications
            total_block_sent_num += self.data_block_num
            counter += 1
            if counter == self.data_block_num:
                # Send the notification to Ceilometer when the chunk
                # number reaches the data_block_num.  The backup percentage
                # is put in the metadata as the extra information.
                self._send_progress_notification(self.context, backup,
                                                 object_meta,
                                                 total_block_sent_num,
                                                 volume_size_bytes)
                # Reset the counter
                counter = 0

            LOG.debug(('Backup volume, '
                       'backup id: %(bk_id)s, volume id: %(vol_id)s, '
                       'chunk index: %(index)s, '
                       'total write size before '
                       'compression: %(length_bytes)s bytes, '
                       'total write size actually: %(w_length)s bytes.'),
                      {'bk_id': backup['id'],
                       'vol_id': backup['volume_id'],
                       'index': index,
                       'length_bytes': length_bytes,
                       'w_length': write_length_bytes})
            index += 1

        # Stop the timer.
        timer.stop()
        # All the data have been sent, the backup_percent reaches 100.
        self._send_progress_end(self.context, backup, object_meta)

        object_sha256['sha256s'] = sha256_list
        if backup_metadata:
            try:
                self._backup_metadata(backup, object_meta)
            # Whatever goes wrong, we want to log, cleanup, and re-raise.
            except Exception as err:
                with excutils.save_and_reraise_exception():
                    LOG.exception(_LE("Backup volume metadata failed: %s."),
                                  err)
                    self.delete(backup)

        length_mb = length_bytes / units.Mi
        write_length_mb = (write_length_bytes + units.Mi - 1) / units.Mi

        self._finalize_backup(backup, container, object_meta, object_sha256)
        LOG.info(_LI('Backup volume, '
                     'backup id:%(bk_id)s, volume id:%(vol_id)s, '
                     'write size before compression: %(length)s MB, '
                     'write size actually: %(w_length)s MB.'),
                 {'bk_id': backup['id'],
                  'vol_id': backup['volume_id'],
                  'length': length_mb,
                  'w_length': write_length_mb})
        return {'size': write_length_mb, 'container': container}

    def _backup_chunk(self, backup, container, data, data_offset,
                      object_meta, extra_metadata):
        """Backup data chunk based on the object metadata and offset."""
        object_prefix = object_meta['prefix']
        object_list = object_meta['list']

        object_id = object_meta['id']
        object_name = '%s-%05d' % (object_prefix, object_id)
        obj = {}
        obj[object_name] = {}
        obj[object_name]['offset'] = data_offset
        obj[object_name]['length'] = len(data)
        LOG.debug('reading chunk of data from volume')
        if self.compressor is not None:
            algorithm = CONF.backup_compression_algorithm.lower()
            obj[object_name]['compression'] = algorithm
            data_size_bytes = len(data)
            data = self.compressor.compress(data)
            comp_size_bytes = len(data)
            LOG.debug('compressed %(data_size_bytes)d bytes of data '
                      'to %(comp_size_bytes)d bytes using '
                      '%(algorithm)s',
                      {
                          'data_size_bytes': data_size_bytes,
                          'comp_size_bytes': comp_size_bytes,
                          'algorithm': algorithm,
                      })
        else:
            LOG.debug('not compressing data')
            obj[object_name]['compression'] = 'none'

        LOG.debug('About to put_object')
        write_length_bytes = len(data)
        with self.get_object_writer(
                container, object_name, extra_metadata=extra_metadata
        ) as writer:
            writer.write(data)
        md5 = hashlib.md5(data).hexdigest()
        obj[object_name]['md5'] = md5
        LOG.debug('backup MD5 for %(object_name)s: %(md5)s',
                  {'object_name': object_name, 'md5': md5})
        object_list.append(obj)
        object_id += 1
        object_meta['list'] = object_list
        object_meta['id'] = object_id

        LOG.debug('Calling eventlet.sleep(0)')
        eventlet.sleep(0)
        return write_length_bytes


def get_backup_driver(context):
    return oNestBackupDriver(context)

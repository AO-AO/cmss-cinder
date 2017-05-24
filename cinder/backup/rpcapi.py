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
Client side of the volume backup RPC API.
"""


import random

from oslo_config import cfg
from oslo_log import log as logging
import oslo_messaging as messaging

from cinder import rpc


CONF = cfg.CONF

LOG = logging.getLogger(__name__)


class BackupAPI(object):
    """Client side of the volume rpc API.

    API version history:

        1.0 - Initial version.
    """

    BASE_RPC_API_VERSION = '1.0'

    def __init__(self):
        super(BackupAPI, self).__init__()
        target = messaging.Target(topic=CONF.backup_topic,
                                  version=self.BASE_RPC_API_VERSION)
        self.client = rpc.get_client(target, '1.0')

    def create_backup(self, ctxt, host, backup_id, volume_id):
        LOG.debug("create_backup in rpcapi backup_id %s", backup_id)
        cctxt = self.client.prepare(server=host)
        cctxt.cast(ctxt, 'create_backup', backup_id=backup_id)

    def restore_backup(self, ctxt, host, backup_id, volume_id):
        LOG.debug("restore_backup in rpcapi backup_id %s", backup_id)
        cctxt = self.client.prepare(server=host)
        cctxt.cast(ctxt, 'restore_backup', backup_id=backup_id,
                   volume_id=volume_id)

    def delete_backup(self, ctxt, host, backup_id):
        LOG.debug("delete_backup  rpcapi backup_id %s", backup_id)
        cctxt = self.client.prepare(server=host)
        cctxt.cast(ctxt, 'delete_backup', backup_id=backup_id)

    def export_record(self, ctxt, host, backup_id):
        LOG.debug("export_record in rpcapi backup_id %(id)s "
                  "on host %(host)s.",
                  {'id': backup_id,
                   'host': host})
        cctxt = self.client.prepare(server=host)
        return cctxt.call(ctxt, 'export_record', backup_id=backup_id)

    def import_record(self,
                      ctxt,
                      host,
                      backup_id,
                      backup_service,
                      backup_url,
                      backup_hosts):
        LOG.debug("import_record rpcapi backup id %(id)s "
                  "on host %(host)s for backup_url %(url)s.",
                  {'id': backup_id,
                   'host': host,
                   'url': backup_url})
        cctxt = self.client.prepare(server=host)
        cctxt.cast(ctxt, 'import_record',
                   backup_id=backup_id,
                   backup_service=backup_service,
                   backup_url=backup_url,
                   backup_hosts=backup_hosts)

    def reset_status(self, ctxt, host, backup_id, status):
        LOG.debug("reset_status in rpcapi backup_id %(id)s "
                  "on host %(host)s.",
                  {'id': backup_id,
                   'host': host})
        cctxt = self.client.prepare(server=host)
        return cctxt.cast(ctxt, 'reset_status', backup_id=backup_id,
                          status=status)

    def create_instance_backup(self, ctxt, instance_uuid, inst_backup_kwargs):
        LOG.debug("create_instance_backup in rpcapi instance_uuid"
                  " %(instance_uuid)s",
                  {'instance_uuid': instance_uuid})
        # for backup in inst_backup_kwargs:
        #     cctxt = self.client.prepare(server=backup['host'])
        #     cctxt.cast(ctxt, 'create_instance_backup',
        #                instance_uuid=instance_uuid,
        #                backup_id=backup['backup_id'])
        #     # Before backing up each volume, need to freeze file system,
        #     # which takes a few seconds. It's a workaround to make sure
        #     # no concurrency request is sent to cinder-backup manager.
        #     time.sleep(4)

        # In Nanji environment:
        # 1. Each cinder node has cinder-volume and cinder-backup running
        # 2. All cinder backends are enabled on each cinder volume nodes.
        # 3. We have bcec backup driver as a proxy to forword backup request
        #    to the specific backup driver.
        # So it is OK to cast the rpc request to a random host.
        host = random.choice(inst_backup_kwargs)['host']
        cctxt = self.client.prepare(server=host)
        cctxt.cast(ctxt, 'create_instance_backup',
                   instance_uuid=instance_uuid,
                   inst_backup_kwargs=inst_backup_kwargs)

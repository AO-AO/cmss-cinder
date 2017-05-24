# Copyright (c) 2011 X.commerce, a business unit of eBay Inc.
# a Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# Copyright 2014 IBM Corp.
# All Rights Reserved.
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

# from oslo.config import cfg
from oslo_log import log as logging

# from cinder import exception
from cinder.backup.driver import BackupDriver

from cinder.backup.drivers import fujitsu_onest as fujistu_driver
from cinder.backup.drivers import sheepdog as sheepdog_driver
# from cinder.backup.drivers import ceph as ceph_driver
# from cinder.i18n import _LW


LOG = logging.getLogger(__name__)

# CONF = cfg.CONF
# CONF.register_opts(service_opts)


class BcecBackupDriver(BackupDriver):

    def __init__(self, context, db_driver=None, execute=None):
        super(BcecBackupDriver, self).__init__(context, db_driver)

    def backup(self, backup, volume_file):
        if volume_file == "sheepdog":
            # volume_file is fixed to "sheepdog" in Nanji
            LOG.debug("Call SheepdogBackupDriver to create backup %s",
                      backup.id)
            return sheepdog_driver.SheepdogBackupDriver(self.context).\
                backup(backup, volume_file)
        else:
            # Need to call fujistu's backup method
            # pass
            LOG.debug("Call fujistu backup driver to create backup %s",
                      backup.id)
            return fujistu_driver.oNestBackupDriver(self.context).\
                backup(backup, volume_file)

    def restore(self, backup, target_volume_id, volume_file):
        if volume_file == "sheepdog":
            # volume_file is fixed to "sheepdog" in Nanji
            LOG.debug("Call SheepdogBackupDriver to restore backup %s",
                      backup.id)
            sheepdog_driver.SheepdogBackupDriver(self.context).\
                restore(backup, target_volume_id, volume_file)
        else:
            # Need to call fujistu's restore method
            # pass
            LOG.debug("Call fujistu backup driver to restore backup %s",
                      backup.id)
            fujistu_driver.oNestBackupDriver(self.context).\
                restore(backup, target_volume_id, volume_file)

    def delete(self, backup):
        # 'container' is used to identify the backup driver.
        # For EBS, 'sheepdog' is fixed in EBS side.
        if backup.container == 'sheepdog':
            LOG.debug("Call SheepdogBackupDriver to delete backup %s",
                      backup.id)
            sheepdog_driver.SheepdogBackupDriver(self.context).delete(backup)
        else:
            LOG.debug("Call FujistuBackupDriver to delete backup %s",
                      backup.id)
            fujistu_driver.oNestBackupDriver(self.context).delete(backup)


def get_backup_driver(context):
    return BcecBackupDriver(context)

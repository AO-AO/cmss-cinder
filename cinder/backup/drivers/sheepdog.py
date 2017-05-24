#coding:utf-8
import time
import json
import urllib2
from oslo.config import cfg
from cinder import exception
from oslo_log import log as logging
from cinder.backup.driver import BackupDriver

LOG = logging.getLogger(__name__)

service_opts = [
    cfg.StrOpt('cinder_ip',
               default='172.16.172.250:8776',
               help='ebs management node ip.'),
]

CONF = cfg.CONF
CONF.register_opts(service_opts)

class SheepdogBackupDriver(BackupDriver):

    def __init__(self, context, db_driver=None):
        super(SheepdogBackupDriver, self).__init__(db_driver)

        self.context    = context
        self._server_ip = self._utf8(CONF.cinder_ip)

    @staticmethod
    def _utf8(s):
        """Ensure string s is utf8 (i.e. not unicode)."""
        if isinstance(s, str):
            return s
        return s.encode('utf8')

    def backup(self, backup, volume_file):
        LOG.info('Starting backup...... Creating a new backup for volume:%s.' % backup['volume_id'])

        backup_id = backup['id']

        url = 'http://' + self._server_ip + '/v2/admin/backups'
        data = {
            "backup":{
                "container"  : backup['container'],
                "description": backup['display_description'],
                "name"        : backup['display_name'],
                "volume_id"  : backup['volume_id'],
                "backupid"   : backup_id
            }
        }

        jdata = json.dumps(data)
        req = urllib2.Request(url, jdata)
        req.add_header('Content-type', 'application/json')

        try:
            response = urllib2.urlopen(req)
            LOG.debug(response.read())
        except urllib2.HTTPError, e:
            LOG.debug(e.code)
            msg = "redirect backup cmd failed!"
            raise exception.BackupOperationError(msg)

        while True:
            url = 'http://' + self._server_ip + '/v2/admin/backups/' + backup_id

            try:
                response = urllib2.urlopen(url)
                ret = response.read()
                LOG.debug("RET: %r" % ret)
                data = json.loads(ret)
            except urllib2.HTTPError, e:
                LOG.debug(e.code)
                msg = "confirm backup cmd failed!"
                raise exception.BackupOperationError(msg)

            if data['backup']['status'] == 'available':
                size = data['backup']['object_count']
                LOG.debug("size %s MB." % size)
                LOG.info('backup finished.')
                break

            time.sleep(3)

        return size

    def restore(self, backup, target_volume_id, volume_file):
        LOG.info('Starting restore...... restore from src_volume:%(src)s to dst_volume:%(dst)s' %
              {'src': backup['volume_id'], 'dst': str("volume-" + target_volume_id)})

        backup_id = backup['id']

        url = 'http://' + self._server_ip + '/v2/admin/backups/' + backup_id + '/restore'
        data = {
            "restore":{
                "volume_id": target_volume_id
            }
        }

        jdata = json.dumps(data)
        req = urllib2.Request(url, jdata)
        req.add_header('Content-type', 'application/json')

        try:
            response = urllib2.urlopen(req)
            LOG.debug(response.read())
        except urllib2.HTTPError, e:
            LOG.debug(e.code)
            msg = "redirect restore cmd failed!"
            raise exception.BackupOperationError(msg)

        while True:
            url = 'http://' + self._server_ip + '/v2/admin/backups/' + backup_id

            try:
                response = urllib2.urlopen(url)
                ret = response.read()
                LOG.debug("RET: %r" % ret)
                data = json.loads(ret)
            except urllib2.HTTPError, e:
                LOG.debug(e.code)
                msg = "confirm restore cmd failed!"
                raise exception.BackupOperationError(msg)

            if data['backup']['status'] == 'available':
                LOG.info('restore finished.')
                break

            time.sleep(3)

    def delete(self, backup):
        LOG.info('Starting delete...... backupid:%s' % backup['id'])

        backup_id = backup['id']

        url = 'http://' + self._server_ip + '/v2/admin/backups/' + backup_id

        req = urllib2.Request(url)
        req.add_header('Content-Type', 'application/json')
        req.get_method = lambda:'DELETE'

        try:
            response = urllib2.urlopen(req)
            LOG.debug(response.read())
        except urllib2.HTTPError, e:
            LOG.debug(e.code)
            if e.code == 404:
                msg = "backup does not exist!"
                LOG.info(msg)
                raise exception.BackupOperationError(msg)
                #help to decide the volume whether belongs to ebs
            else:
                msg = "redirect delete cmd failed!"
                raise exception.BackupOperationError(msg)

        while True:
            url = 'http://' + self._server_ip + '/v2/admin/backups/' + backup_id

            try:
                urllib2.urlopen(url)

            except urllib2.HTTPError, e:
                LOG.debug(e.code)
                if e.code == 404:
                    """backup does not exist! already success!"""
                    LOG.info('delete finished.')
                    break
                else:
                    msg = "confirm delete cmd failed!"
                    raise exception.BackupOperationError(msg)

            time.sleep(3)

def get_backup_driver(context):
    return SheepdogBackupDriver(context)

if __name__ == '__main__':
    driver = SheepdogBackupDriver()

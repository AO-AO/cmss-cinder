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

from oslo_log import log as logging

from cinder.api import common


LOG = logging.getLogger(__name__)


class ViewBuilder(common.ViewBuilder):
    """Model backup API responses as a python dictionary."""

    _collection_name = "backups"

    def __init__(self):
        """Initialize view builder."""
        super(ViewBuilder, self).__init__()

    def summary_list(self, request, backups, origin_backup_count):
        """Show a list of backups without many details."""
        return self._list_view(self.summary, request, backups,
                               origin_backup_count)

    def detail_list(self, request, backups, origin_backup_count):
        """Detailed view of a list of backups ."""
        return self._list_view(self.detail, request, backups,
                               origin_backup_count)

    def summary(self, request, backup):
        """Generic, non-detailed view of a backup."""
        return {
            'backup': {
                'id': backup['id'],
                'name': backup['display_name'],
                'links': self._get_links(request,
                                         backup['id']),
            },
        }

    def instance_backup_summary(self, request, backups):
        """Generic, non-detailed view of a instance volumes backup."""
        backup_list = []
        for backup in backups:
            values = {
                'id': backup['id'],
                'name': backup['display_name'],
                'description': backup['display_description'],
                'volume_id': backup['volume_id'],
                'links': self._get_links(request,
                                         backup['id']),
            }
            backup_list.append(values)
        return {'backup': backup_list}

    def restore_summary(self, request, restore):
        """Generic, non-detailed view of a restore."""
        return {
            'restore': {
                'backup_id': restore['backup_id'],
                'volume_id': restore['volume_id'],
            },
        }

    def detail(self, request, backup):
        """Detailed view of a single backup."""
        return {
            'backup': {
                'id': backup.get('id'),
                'status': backup.get('status'),
                'size': backup.get('size'),
                'object_count': backup.get('object_count'),
                'availability_zone': backup.get('availability_zone'),
                'container': backup.get('container'),
                'created_at': backup.get('created_at'),
                'name': backup.get('display_name'),
                'description': backup.get('display_description'),
                'fail_reason': backup.get('fail_reason'),
                'volume_id': backup.get('volume_id'),
                'links': self._get_links(request, backup['id']),
                'parent_id': backup.get('parent_id')
            }
        }

    def _list_view(self, func, request, backups, origin_backup_count):
        """Provide a view for a list of backups."""
        backups_list = [func(request, backup)['backup'] for backup in backups]
        backups_links = self._get_collection_links(request,
                                                   backups,
                                                   self._collection_name,
                                                   origin_backup_count)
        backups_dict = dict(backups=backups_list)

        if backups_links:
            backups_dict['backups_links'] = backups_links

        return backups_dict

    def export_summary(self, request, export):
        """Generic view of an export."""
        return {
            'backup-record': {
                'backup_service': export['backup_service'],
                'backup_url': export['backup_url'],
            },
        }

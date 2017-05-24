# Copyright 2013 IBM Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""
Handles all requests to Nova.
"""


from novaclient import exceptions as nova_exceptions
from novaclient import extension
from novaclient import service_catalog
from novaclient.v1_1 import client as nova_client
from novaclient.v1_1.contrib import assisted_volume_snapshots
from novaclient.v1_1.contrib import list_extensions
from oslo_config import cfg
from oslo_log import log as logging
from requests import exceptions as request_exceptions

from cinder import context as ctx
from cinder.db import base
from cinder import exception
from cinder.i18n import _LW, _LE


nova_opts = [
    cfg.StrOpt('nova_catalog_info',
               default='compute:Compute Service:publicURL',
               help='Match this value when searching for nova in the '
                    'service catalog. Format is: separated values of '
                    'the form: '
                    '<service_type>:<service_name>:<endpoint_type>'),
    cfg.StrOpt('nova_catalog_admin_info',
               default='compute:Compute Service:adminURL',
               help='Same as nova_catalog_info, but for admin endpoint.'),
    cfg.StrOpt('nova_endpoint_template',
               default=None,
               help='Override service catalog lookup with template for nova '
                    'endpoint e.g. http://localhost:8774/v2/%(project_id)s'),
    cfg.StrOpt('nova_endpoint_admin_template',
               default=None,
               help='Same as nova_endpoint_template, but for admin endpoint.'),
    cfg.StrOpt('os_region_name',
               default=None,
               help='Region name of this node'),
    cfg.StrOpt('nova_ca_certificates_file',
               default=None,
               help='Location of ca certificates file to use for nova client '
                    'requests.'),
    cfg.BoolOpt('nova_api_insecure',
                default=False,
                help='Allow to perform insecure SSL requests to nova'),
]

CONF = cfg.CONF
CONF.register_opts(nova_opts)

LOG = logging.getLogger(__name__)

nova_extensions = (assisted_volume_snapshots,
                   extension.Extension('list_extensions', list_extensions))

QEMU_GA_NOT_AVAILABLE = "Guest agent not available for now"
QEMU_GA_NOT_ENABLE = "QEMU guest agent is not enabled"
QEMU_GA_REPEAT_FREEZE = "domain is already quiesced"
NOT_PERMIT_VM_STATE = "Desired state must be specified"
QEMU_GA_REPEAT_THAW = "domain is not quiesced"


def novaclient(context, admin_endpoint=False, privileged_user=False,
               timeout=None):
    """Returns a Nova client

    @param admin_endpoint: If True, use the admin endpoint template from
        configuration ('nova_endpoint_admin_template' and 'nova_catalog_info')
    @param privileged_user: If True, use the account from configuration
        (requires 'os_privileged_user_name', 'os_privileged_user_password' and
        'os_privileged_user_tenant' to be set)
    @param timeout: Number of seconds to wait for an answer before raising a
        Timeout exception (None to disable)
    """
    # FIXME: the novaclient ServiceCatalog object is mis-named.
    #        It actually contains the entire access blob.
    # Only needed parts of the service catalog are passed in, see
    # nova/context.py.
    compat_catalog = {
        'access': {'serviceCatalog': context.service_catalog or []}
    }
    sc = service_catalog.ServiceCatalog(compat_catalog)

    nova_endpoint_template = CONF.nova_endpoint_template
    nova_catalog_info = CONF.nova_catalog_info

    if admin_endpoint:
        nova_endpoint_template = CONF.nova_endpoint_admin_template
        nova_catalog_info = CONF.nova_catalog_admin_info
    service_type, service_name, endpoint_type = nova_catalog_info.split(':')

    # Extract the region if set in configuration
    if CONF.os_region_name:
        region_filter = {'attr': 'region', 'filter_value': CONF.os_region_name}
    else:
        region_filter = {}

    if privileged_user and CONF.os_privileged_user_name:
        context = ctx.RequestContext(
            CONF.os_privileged_user_name, None,
            auth_token=CONF.os_privileged_user_password,
            project_name=CONF.os_privileged_user_tenant,
            service_catalog=context.service_catalog)

        # When privileged_user is used, it needs to authenticate to Keystone
        # before querying Nova, so we set auth_url to the identity service
        # endpoint. We then pass region_name, endpoint_type, etc. to the
        # Client() constructor so that the final endpoint is chosen correctly.
        url = sc.url_for(service_type='identity',
                         endpoint_type=endpoint_type,
                         **region_filter)

        LOG.debug('Creating a Nova client using "%s" user',
                  CONF.os_privileged_user_name)
    else:
        if nova_endpoint_template:
            url = nova_endpoint_template % context.to_dict()
        else:
            url = sc.url_for(service_type=service_type,
                             service_name=service_name,
                             endpoint_type=endpoint_type,
                             **region_filter)

        LOG.debug('Nova client connection created using URL: %s', url)

    c = nova_client.Client(context.user_id,
                           context.auth_token,
                           context.project_name,
                           auth_url=url,
                           insecure=CONF.nova_api_insecure,
                           timeout=timeout,
                           region_name=CONF.os_region_name,
                           endpoint_type=endpoint_type,
                           cacert=CONF.nova_ca_certificates_file,
                           extensions=nova_extensions)

    if not privileged_user:
        # noauth extracts user_id:project_id from auth_token
        c.client.auth_token = (context.auth_token or '%s:%s'
                               % (context.user_id, context.project_id))
        c.client.management_url = url
    return c


class API(base.Base):
    """API for interacting with novaclient."""

    def has_extension(self, context, extension, timeout=None):
        try:
            client = novaclient(context, timeout=timeout)

            # Pylint gives a false positive here because the 'list_extensions'
            # method is not explicitly declared. Overriding the error.
            # pylint: disable-msg=E1101
            nova_exts = client.list_extensions.show_all()
        except request_exceptions.Timeout:
            raise exception.APITimeout(service='Nova')
        return extension in [e.name for e in nova_exts]

    def update_server_volume(self, context, server_id, attachment_id,
                             new_volume_id):
        novaclient(context).volumes.update_server_volume(server_id,
                                                         attachment_id,
                                                         new_volume_id)

    def create_volume_snapshot(self, context, volume_id, create_info):
        nova = novaclient(context, admin_endpoint=True, privileged_user=True)

        nova.assisted_volume_snapshots.create(
            volume_id,
            create_info=create_info)

    def delete_volume_snapshot(self, context, snapshot_id, delete_info):
        nova = novaclient(context, admin_endpoint=True, privileged_user=True)

        nova.assisted_volume_snapshots.delete(
            snapshot_id,
            delete_info=delete_info)

    def get_server(self, context, server_id, privileged_user=False,
                   timeout=None):
        try:
            return novaclient(context, privileged_user=privileged_user,
                              timeout=timeout).servers.get(server_id)
        except nova_exceptions.NotFound:
            raise exception.ServerNotFound(uuid=server_id)
        except request_exceptions.Timeout:
            raise exception.APITimeout(service='Nova')

    def freeze_filesystem(self, context, server_id, timeout=None):
        server = self.get_server(context, server_id, timeout=timeout)
        if server.status in ["SHUTOFF", "PAUSED", "SUSPENDED",
                             "SHELVED_OFFLOADED"]:
            LOG.warn(_LW("VM %(server_id)s is in %(state)s state, do not need"
                         " to freeze filesystem"), {'server_id': server_id,
                                                    'state': server.status})
            return
        try:
            return novaclient(context, timeout=timeout
                              ).servers.freeze_filesystem(server)
        except nova_exceptions.NotFound:
            raise exception.ServerNotFound(uuid=server_id)
        except request_exceptions.Timeout:
            raise exception.APITimeout(service='Nova')
        except nova_exceptions.Conflict as e:
            if isinstance(e, basestring):
                errmsg = e
            else:
                errmsg = e.message
            LOG.warn(_LW('vm %(server_id)s freeze fs meet error,'
                         'error message is %(message)s'),
                     {'server_id': server_id, 'message': e.message})
            if errmsg.find(QEMU_GA_NOT_ENABLE) != -1:
                raise exception.QemuGANotEnable()
            elif errmsg.find(QEMU_GA_NOT_AVAILABLE) != -1:
                raise exception.QemuGANotAvailable()
            elif errmsg.find(QEMU_GA_REPEAT_FREEZE) != -1:
                raise exception.QemuGARepeatFreeze()
            else:
                raise

    def thaw_filesystem(self, context, server_id, timeout=None):
        server = self.get_server(context, server_id, timeout=timeout)
        if server.status in ["SHUTOFF", "PAUSED", "SUSPENDED",
                             "SHELVED_OFFLOADED"]:
            LOG.warn(_LW("VM %(server_id)s is in %(state)s state, do not need"
                         "to thaw filesystem"), {'server_id': server_id,
                                                 'state': server.status})
            return
        try:
            return novaclient(context, timeout=timeout
                              ).servers.thaw_filesystem(server)
        except nova_exceptions.NotFound:
            raise exception.ServerNotFound(uuid=server_id)
        except request_exceptions.Timeout:
            raise exception.APITimeout(service='Nova')
        except nova_exceptions.Conflict as e:
            if isinstance(e, basestring):
                errmsg = e
            else:
                errmsg = e.message
            LOG.warn(_LW('vm %(server_id)s freeze fs meet error,'
                         'error message is %(message)s'),
                     {'server_id': server_id, 'message': e.message})
            if errmsg.find(QEMU_GA_NOT_ENABLE) != -1:
                raise exception.QemuGANotEnable()
            elif errmsg.find(QEMU_GA_NOT_AVAILABLE) != -1:
                raise exception.QemuGANotAvailable()
            elif errmsg.find(QEMU_GA_REPEAT_THAW) != -1:
                raise exception.QemuGARepeatThaw()
            else:
                raise

    # vm_state = backing_up
    def set_vm_state(self, context, server_id, vm_state, timeout=None):
        server = self.get_server(context, server_id, timeout=timeout)
        try:
            return novaclient(context, timeout=timeout
                              ).servers.reset_state(server, vm_state)
        except nova_exceptions.NotFound:
            raise exception.ServerNotFound(uuid=server_id)
        except request_exceptions.Timeout:
            raise exception.APITimeout(service='Nova')
        except Exception as e:
            if isinstance(e, basestring):
                errmsg = e
            else:
                errmsg = e.message
            if errmsg.find(NOT_PERMIT_VM_STATE) != -1:
                raise exception.NotPermitVmState()
            else:
                raise

    # vm_state = backing_up
    def get_vm_state(self, context, server_id, timeout=None):
        server = self.get_server(context, server_id, timeout=timeout)
        return server._info['OS-EXT-STS:vm_state']

    def exec_cmd(self, context, server_id, command,
                 run_as="root", timeout=None):
        server = self.get_server(context, server_id, timeout=timeout)
        try:
            return novaclient(context, timeout=timeout)\
                .servers.exec_cmd_by_qga(server, command, run_as)
        except nova_exceptions.NotFound:
            raise exception.ServerNotFound(uuid=server_id)
        except request_exceptions.Timeout:
            raise exception.APITimeout(service='Nova')
        except Exception as e:
            LOG.error(_LE("Run cmd error, %s"), e)
            raise exception.ExecCmdError()

    def detach_volume(self, context, server_id, volume_id,
                      privileged_user=False, timeout=None):
        try:
            return novaclient(context, privileged_user=privileged_user,
                              timeout=timeout)\
                .volumes.delete_server_volume(server_id, volume_id)
        except Exception as e:
            if isinstance(e, basestring):
                errmsg = e
            else:
                errmsg = e.message
            raise exception.DetachVolumeError(reason=errmsg)

    def attach_volume(self, context, server_id, volume_id, device_name,
                      privileged_user=False, timeout=None):
        try:
            return novaclient(
                context,
                privileged_user=privileged_user,
                timeout=timeout) .volumes.create_server_volume(
                server_id,
                volume_id,
                device_name)
        except Exception as e:
            if isinstance(e, basestring):
                errmsg = e
            else:
                errmsg = e.message
            raise exception.AttachVolumeError(reason=errmsg)

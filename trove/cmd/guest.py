# Copyright 2011 OpenStack Foundation
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
import os
import re
import sys
import subprocess

from oslo_config import cfg as openstack_cfg
from oslo_log import log as logging
from oslo_service import service as openstack_service

from trove.common import cfg
from trove.common import debug_utils
from trove.common.i18n import _
from trove.guestagent import api as guest_api
from trove.guestagent.common import operating_system
from trove.guestagent import volume

CONF = cfg.CONF
# The guest_id opt definition must match the one in common/cfg.py
CONF.register_opts([openstack_cfg.StrOpt('guest_id', default=None,
                                         help="ID of the Guest Instance."),
                    openstack_cfg.StrOpt('instance_rpc_encr_key',
                                         help=('Key (OpenSSL aes_cbc) for '
                                               'instance RPC encryption.'))])
LOG = logging.getLogger(__name__)


def main():
    log_levels = [
        'docker=WARN',
    ]
    default_log_levels = logging.get_default_log_levels()
    default_log_levels.extend(log_levels)
    logging.set_defaults(default_log_levels=default_log_levels)
    logging.register_options(CONF)

    cfg.parse_args(sys.argv)
    logging.setup(CONF, None)
    debug_utils.setup()

    get_docker_ca()

    from trove.guestagent import dbaas
    manager = dbaas.datastore_registry().get(CONF.datastore_manager)
    if not manager:
        msg = (_("Manager class not registered for datastore manager %s") %
               CONF.datastore_manager)
        raise RuntimeError(msg)

    if not CONF.guest_id:
        msg = (_("The guest_id parameter is not set. guest_info.conf "
                 "was not injected into the guest or not read by guestagent"))
        raise RuntimeError(msg)

    # Create user and group for running docker container.
    LOG.info('Creating user and group for database service')
    uid = cfg.get_configuration_property('database_service_uid')
    operating_system.create_user('database', uid)

    # Mount device if needed.
    # When doing rebuild, the device should be already formatted but not
    # mounted.
    device_path = CONF.get(CONF.datastore_manager).device_path
    mount_point = CONF.get(CONF.datastore_manager).mount_point
    device = volume.VolumeDevice(device_path)
    if not device.mount_points(device_path):
        LOG.info('Preparing the storage for %s, mount path %s',
                 device_path, mount_point)
        device.format()
        device.mount(mount_point)
        operating_system.chown(mount_point, CONF.database_service_uid,
                               CONF.database_service_uid,
                               recursive=True, as_root=True)

    # rpc module must be loaded after decision about thread monkeypatching
    # because if thread module is not monkeypatched we can't use eventlet
    # executor from oslo_messaging library.
    from trove import rpc
    rpc.init(CONF)

    from trove.common.rpc import service as rpc_service
    server = rpc_service.RpcService(
        key=CONF.instance_rpc_encr_key,
        topic="guestagent.%s" % CONF.guest_id,
        manager=manager, host=CONF.guest_id,
        rpc_api_version=guest_api.API.API_LATEST_VERSION)

    launcher = openstack_service.launch(CONF, server, restart_method='mutate')
    launcher.wait()


def get_docker_ca():
    # 1. 获取docker仓库地址，然后下载证书
    container_registry = CONF.guest_agent.container_registry
    ip = re.findall(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b", container_registry)[0]
    files = ['%s.cert' % ip, '%s.key' % ip, 'ca.crt']
    path = '/etc/docker/certs.d/%s/' % ip
    if not os.path.exists(path):
        cmd = 'sudo mkdir -p %s' % path
        start_proc(cmd, shell=True)
    # 2. 获取文件
    for file in files:
        cmd = 'cd /etc/docker/certs.d/{ip}/; sudo bash -c "curl http://{ip}:10001/{file} > /etc/docker/certs.d/{ip}/{file}"'.format(
            ip=ip, file=file)
        start_proc(cmd, shell=True)
    start_proc('systemctl restart docker', shell=True)


def start_proc(cmd, shell=False):
    """Given a command, starts and returns a process."""
    env = os.environ.copy()
    proc = subprocess.Popen(
        cmd,
        shell=shell,
        stdin=subprocess.PIPE,
        bufsize=0,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env
    )
    return proc

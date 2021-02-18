#    Copyright 2012 OpenStack Foundation
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
from tempfile import NamedTemporaryFile
from unittest import mock
from unittest.mock import call
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch
from unittest.mock import PropertyMock

from cinderclient import exceptions as cinder_exceptions
from cinderclient.v2 import volumes as cinderclient_volumes
import cinderclient.v2.client as cinderclient
import neutronclient.v2_0.client as neutronclient
from novaclient import exceptions as nova_exceptions
import novaclient.v2.flavors
import novaclient.v2.servers
from oslo_config import cfg
from swiftclient.client import ClientException
from testtools.matchers import Equals
from testtools.matchers import Is

from trove import rpc
from trove.backup import models as backup_models
from trove.backup import state
import trove.backup.models
from trove.common import timeutils
from trove.common import utils
import trove.common.context
from trove.common import exception
from trove.common.exception import GuestError
from trove.common.exception import PollTimeOut
from trove.common.exception import TroveError
import trove.common.template as template
from trove.datastore import models as datastore_models
import trove.db.models
from trove.extensions.common import models as common_models
from trove.extensions.mysql import models as mysql_models
import trove.guestagent.api
from trove.instance.models import BaseInstance
from trove.instance.models import DBInstance
from trove.instance.models import InstanceServiceStatus
from trove.instance.models import InstanceStatus
from trove.instance.service_status import ServiceStatuses
from trove.instance.tasks import InstanceTasks
from trove.taskmanager import models as taskmanager_models
from trove.tests.unittests import trove_testtools
from trove.tests.unittests.util import util

INST_ID = 'dbinst-id-1'
VOLUME_ID = 'volume-id-1'


class FakeOptGroup(object):
    def __init__(self, tcp_ports=['3306', '3301-3307'],
                 udp_ports=[], icmp=False):
        self.tcp_ports = tcp_ports
        self.udp_ports = udp_ports
        self.icmp = icmp


class fake_Server(object):
    def __init__(self):
        self.id = None
        self.name = None
        self.image_id = None
        self.flavor_id = None
        self.files = None
        self.userdata = None
        self.meta = None
        self.block_device_mapping_v2 = None
        self.status = 'HEALTHY'
        self.key_name = None


class fake_ServerManager(object):

    def create(self, name, image_id, flavor_id, files, userdata,
               block_device_mapping_v2=None,
               availability_zone=None,
               nics=None, config_drive=False,
               scheduler_hints=None, key_name=None, meta=None):
        server = fake_Server()
        server.id = "server_id"
        server.name = name
        server.image_id = image_id
        server.flavor_id = flavor_id
        server.files = files
        server.userdata = userdata
        server.block_device_mapping_v2 = block_device_mapping_v2
        server.availability_zone = availability_zone
        server.nics = nics
        server.key_name = key_name
        server.meta = meta
        return server


class fake_nova_client(object):
    def __init__(self):
        self.servers = fake_ServerManager()


class fake_InstanceServiceStatus(object):

    _instance = None

    def __init__(self):
        self.deleted = False
        self.status = None
        pass

    def set_status(self, status):
        self.status = status
        pass

    def get_status(self):
        return self.status

    @classmethod
    def find_by(cls, **kwargs):
        if not cls._instance:
            cls._instance = fake_InstanceServiceStatus()
        return cls._instance

    def save(self):
        pass

    def delete(self):
        self.deleted = True
        pass

    def is_deleted(self):
        return self.deleted


class fake_DBInstance(object):

    _instance = None

    def __init__(self):
        self.deleted = False
        pass

    @classmethod
    def find_by(cls, **kwargs):
        if not cls._instance:
            cls._instance = fake_DBInstance()
        return cls._instance

    def set_task_status(self, status):
        self.status = status
        pass

    def get_task_status(self):
        return self.status

    def save(self):
        pass

    def delete(self):
        self.deleted = True
        pass

    def is_deleted(self):
        return self.deleted


class BaseFreshInstanceTasksTest(trove_testtools.TestCase):

    def setUp(self):
        super(BaseFreshInstanceTasksTest, self).setUp()
        mock_instance = patch('trove.instance.models.FreshInstance')
        mock_instance.start()
        self.addCleanup(mock_instance.stop)
        mock_instance.id = Mock(return_value='instance_id')
        mock_instance.tenant_id = Mock(return_value="tenant_id")
        mock_instance.hostname = Mock(return_value="hostname")
        mock_instance.name = Mock(return_value='name')
        mock_instance.nova_client = Mock(
            return_value=fake_nova_client())
        mock_datastore_v = patch(
            'trove.datastore.models.DatastoreVersion')
        mock_datastore_v.start()
        self.addCleanup(mock_datastore_v.stop)
        mock_datastore = patch(
            'trove.datastore.models.Datastore')
        mock_datastore.start()
        self.addCleanup(mock_datastore.stop)

        taskmanager_models.FreshInstanceTasks.nova_client = fake_nova_client()
        self.orig_ISS_find_by = InstanceServiceStatus.find_by
        self.orig_DBI_find_by = DBInstance.find_by
        self.userdata = "hello moto"
        self.guestconfig_content = "guest config"
        with NamedTemporaryFile(mode="w", suffix=".cloudinit",
                                delete=False) as f:
            self.cloudinit = f.name
            f.write(self.userdata)
        with NamedTemporaryFile(mode="w", delete=False) as f:
            self.guestconfig = f.name
            f.write(self.guestconfig_content)
        self.freshinstancetasks = taskmanager_models.FreshInstanceTasks(
            None, MagicMock(), None, None)
        self.freshinstancetasks.context = trove.common.context.TroveContext(
            user='test_user')

    def tearDown(self):
        super(BaseFreshInstanceTasksTest, self).tearDown()
        os.remove(self.cloudinit)
        os.remove(self.guestconfig)
        InstanceServiceStatus.find_by = self.orig_ISS_find_by
        DBInstance.find_by = self.orig_DBI_find_by


class FreshInstanceTasksTest(BaseFreshInstanceTasksTest):

    def test_create_instance_userdata(self):
        cloudinit_location = os.path.dirname(self.cloudinit)
        datastore_manager = os.path.splitext(os.path.basename(self.
                                                              cloudinit))[0]

        cfg.CONF.set_override('cloudinit_location', cloudinit_location)

        server = self.freshinstancetasks._create_server(
            None, None, datastore_manager, None, None, None)

        self.assertEqual(server.userdata, self.userdata)

    def test_create_instance_with_keypair(self):
        cfg.CONF.set_override('nova_keypair', 'fake_keypair')

        server = self.freshinstancetasks._create_server(
            None, None, None, None, None, None)

        self.assertEqual('fake_keypair', server.key_name)

    @patch.object(DBInstance, 'get_by')
    def test_create_instance_guestconfig(self, patch_get_by):
        cfg.CONF.set_override('guest_config', self.guestconfig)
        cfg.CONF.set_override('guest_info', 'guest_info.conf')
        cfg.CONF.set_override('injected_config_location', '/etc/trove/conf.d')

        # execute
        files = self.freshinstancetasks.get_injected_files("test", 'test')
        # verify
        self.assertTrue(
            '/etc/trove/conf.d/guest_info.conf' in files)
        self.assertTrue(
            '/etc/trove/conf.d/trove-guestagent.conf' in files)
        self.assertEqual(
            self.guestconfig_content,
            files['/etc/trove/conf.d/trove-guestagent.conf'])

    @patch.object(DBInstance, 'get_by')
    def test_create_instance_guestconfig_compat(self, patch_get_by):
        cfg.CONF.set_override('guest_config', self.guestconfig)
        cfg.CONF.set_override('guest_info', '/etc/guest_info')
        cfg.CONF.set_override('injected_config_location', '/etc')

        # execute
        files = self.freshinstancetasks.get_injected_files("test", 'test')
        # verify
        self.assertTrue(
            '/etc/guest_info' in files)
        self.assertTrue(
            '/etc/trove-guestagent.conf' in files)
        self.assertEqual(
            self.guestconfig_content,
            files['/etc/trove-guestagent.conf'])

    def test_create_instance_with_az_kwarg(self):
        # execute
        server = self.freshinstancetasks._create_server(
            None, None, None, None, availability_zone='nova', nics=None)
        # verify
        self.assertIsNotNone(server)

    def test_create_instance_with_az(self):
        # execute
        server = self.freshinstancetasks._create_server(
            None, None, None, None, 'nova', None)
        # verify
        self.assertIsNotNone(server)

    def test_create_instance_with_az_none(self):
        # execute
        server = self.freshinstancetasks._create_server(
            None, None, None, None, None, None)
        # verify
        self.assertIsNotNone(server)

    @patch.object(taskmanager_models.FreshInstanceTasks, 'hostname',
                  new_callable=PropertyMock,
                  return_value='fake-hostname')
    def test_servers_create_block_device_mapping_v2(self, mock_hostname):
        self.freshinstancetasks.prepare_userdata = Mock(return_value=None)
        mock_nova_client = self.freshinstancetasks.nova_client = Mock()
        mock_servers_create = mock_nova_client.servers.create
        self.freshinstancetasks._create_server('fake-flavor', 'fake-image',
                                               'mysql', None, None, None)
        meta = {'trove_project_id': self.freshinstancetasks.tenant_id,
                'trove_user_id': 'test_user',
                'trove_instance_id': self.freshinstancetasks.id}
        mock_servers_create.assert_called_with(
            'fake-hostname', 'fake-image',
            'fake-flavor', files={},
            userdata=None,
            block_device_mapping_v2=None,
            availability_zone=None,
            nics=None,
            config_drive=True,
            scheduler_hints=None,
            key_name=None,
            meta=meta,
        )

    @patch.object(InstanceServiceStatus, 'find_by',
                  return_value=fake_InstanceServiceStatus.find_by())
    @patch.object(DBInstance, 'find_by',
                  return_value=fake_DBInstance.find_by())
    @patch('trove.taskmanager.models.LOG')
    def test_update_status_of_instance_failure(
            self, mock_logging, dbi_find_by_mock, iss_find_by_mock):
        self.freshinstancetasks.update_statuses_on_time_out()
        self.assertEqual(ServiceStatuses.FAILED_TIMEOUT_GUESTAGENT,
                         fake_InstanceServiceStatus.find_by().get_status())
        self.assertEqual(InstanceTasks.BUILDING_ERROR_TIMEOUT_GA,
                         fake_DBInstance.find_by().get_task_status())

    @patch.object(BaseInstance, 'update_db')
    @patch.object(taskmanager_models.FreshInstanceTasks, '_create_dns_entry')
    @patch.object(taskmanager_models.FreshInstanceTasks, 'get_injected_files')
    @patch.object(taskmanager_models.FreshInstanceTasks, '_create_server')
    @patch.object(taskmanager_models.FreshInstanceTasks, '_create_secgroup')
    @patch.object(taskmanager_models.FreshInstanceTasks, '_build_volume_info')
    @patch.object(taskmanager_models.FreshInstanceTasks, '_guest_prepare')
    @patch.object(template, 'SingleInstanceConfigTemplate')
    @patch('trove.taskmanager.models.FreshInstanceTasks._create_port')
    def test_create_instance(self,
                             mock_create_port,
                             mock_single_instance_template,
                             mock_guest_prepare,
                             mock_build_volume_info,
                             mock_create_secgroup,
                             mock_create_server,
                             mock_get_injected_files,
                             *args):

        cfg.CONF.set_override('tcp_ports', ['3306', '3301-3307'],
                              group='mysql')

        mock_flavor = {'id': 8, 'ram': 768, 'name': 'bigger_flavor'}
        config_content = {'config_contents': 'some junk'}
        mock_single_instance_template.return_value.config_contents = (
            config_content)
        overrides = Mock()
        mock_create_secgroup.return_value = 'fake_security_group_id'
        mock_create_port.return_value = 'fake-port-id'

        self.freshinstancetasks.create_instance(
            mock_flavor, 'mysql-image-id', None,
            None, 'mysql', 'mysql-server',
            2, None, None,
            None, [{'net-id': 'fake-net-id'}], overrides,
            None, None, 'volume_type',
            None, {'group': 'sg-id'}
        )

        mock_create_secgroup.assert_called_with('mysql', [])
        mock_create_port.assert_called_once_with(
            {'net-id': 'fake-net-id'},
            ['fake_security_group_id'],
            is_mgmt=False,
            is_public=False
        )
        mock_build_volume_info.assert_called_with(
            'mysql', availability_zone=None, volume_size=2,
            volume_type='volume_type'
        )
        mock_guest_prepare.assert_called_with(
            768, mock_build_volume_info(), 'mysql-server', None, None, None,
            config_content, None, overrides, None, None, None, ds_version=None
        )
        mock_create_server.assert_called_with(
            8, 'mysql-image-id', 'mysql',
            mock_build_volume_info()['block_device'], None,
            [{'port-id': 'fake-port-id'}],
            mock_get_injected_files(),
            {'group': 'sg-id'}
        )

    @patch.object(BaseInstance, 'update_db')
    @patch.object(taskmanager_models.FreshInstanceTasks, '_create_dns_entry')
    @patch.object(taskmanager_models.FreshInstanceTasks, 'get_injected_files')
    @patch.object(taskmanager_models.FreshInstanceTasks, '_create_server')
    @patch.object(taskmanager_models.FreshInstanceTasks, '_build_volume_info')
    @patch.object(taskmanager_models.FreshInstanceTasks, '_guest_prepare')
    @patch.object(template, 'SingleInstanceConfigTemplate')
    @patch('trove.common.clients_admin.neutron_client_trove_admin')
    def test_create_instance_with_mgmt_port(self,
                                            mock_neutron_client,
                                            mock_single_instance_template,
                                            mock_guest_prepare,
                                            mock_build_volume_info,
                                            mock_create_server,
                                            mock_get_injected_files,
                                            *args):
        self.patch_conf_property('management_networks', ['fake-mgmt-uuid'])

        mock_client = MagicMock()
        mock_client.create_security_group.return_value = {
            'security_group': {'id': 'fake-sg-id'}
        }
        mock_client.create_port.side_effect = [
            {'port': {'id': 'fake-mgmt-port-id'}},
            {
                'port': {
                    'id': 'fake-user-port-id',
                    'fixed_ips': [{'subnet_id': 'fake-subnet-id'}]
                }
            }
        ]
        mock_client.list_networks.return_value = {
            'networks': [{'id': 'fake-public-net-id'}]
        }
        mock_client.list_ports.return_value = {
            'ports': [{'id': 'fake-port-id'}]
        }
        mock_neutron_client.return_value = mock_client

        mock_flavor = {'id': 8, 'ram': 768, 'name': 'bigger_flavor'}
        config_content = {'config_contents': 'some junk'}
        mock_single_instance_template.return_value.config_contents = (
            config_content)

        self.freshinstancetasks.create_instance(
            mock_flavor, 'mysql-image-id', None,
            None, 'mysql', 'mysql-server',
            2, None, None,
            None, [{'net-id': 'fake-net-uuid'}, {'net-id': 'fake-mgmt-uuid'}],
            mock.ANY,
            None, None, 'volume_type',
            None, {'group': 'sg-id'},
            access={'is_public': True, 'allowed_cidrs': ['192.168.0.1/24']}
        )

        mock_build_volume_info.assert_called_with(
            'mysql', availability_zone=None, volume_size=2,
            volume_type='volume_type'
        )
        mock_guest_prepare.assert_called_with(
            768, mock_build_volume_info(), 'mysql-server', None, None, None,
            config_content, None, mock.ANY, None, None, None, ds_version=None)
        mock_create_server.assert_called_with(
            8, 'mysql-image-id', 'mysql',
            mock_build_volume_info()['block_device'], None,
            [
                {'port-id': 'fake-user-port-id'},
                {'port-id': 'fake-mgmt-port-id'}
            ],
            mock_get_injected_files(), {'group': 'sg-id'}
        )
        create_floatingip_param = {
            "floatingip": {
                'floating_network_id': 'fake-public-net-id',
                'port_id': 'fake-user-port-id',
                'project_id': mock.ANY
            }
        }
        mock_client.create_floatingip.assert_called_once_with(
            create_floatingip_param
        )

    @patch.object(BaseInstance, 'update_db')
    @patch.object(taskmanager_models, 'create_cinder_client')
    @patch.object(taskmanager_models.FreshInstanceTasks, 'device_path',
                  new_callable=PropertyMock,
                  return_value='fake-device-path')
    @patch.object(taskmanager_models.FreshInstanceTasks, 'volume_support',
                  new_callable=PropertyMock,
                  return_value=True)
    def test_build_volume_info(self, mock_volume_support, mock_device_path,
                               mock_create_cinderclient, mock_update_db):
        cfg.CONF.set_override('block_device_mapping', 'fake-bdm')
        cfg.CONF.set_override('mount_point', 'fake-mount-point',
                              group='mysql')
        mock_cinderclient = mock_create_cinderclient.return_value
        mock_volume = Mock(name='fake-vol', id='fake-vol-id',
                           size=2, status='available')
        mock_cinderclient.volumes.create.return_value = mock_volume
        mock_cinderclient.volumes.get.return_value = mock_volume
        volume_info = self.freshinstancetasks._build_volume_info(
            'mysql', volume_size=2, volume_type='volume_type')
        expected = {
            'block_device': [{
                'uuid': 'fake-vol-id',
                'source_type': 'volume',
                'destination_type': 'volume',
                'device_name': 'fake-bdm',
                'volume_size': 2,
                'delete_on_termination': True}],
            'device_path': 'fake-device-path',
            'mount_point': 'fake-mount-point'
        }
        self.assertEqual(expected, volume_info)

    @patch.object(BaseInstance, 'update_db')
    @patch.object(taskmanager_models.FreshInstanceTasks, '_create_volume')
    @patch.object(taskmanager_models.FreshInstanceTasks, 'device_path',
                  new_callable=PropertyMock,
                  return_value='fake-device-path')
    @patch.object(taskmanager_models.FreshInstanceTasks, 'volume_support',
                  new_callable=PropertyMock,
                  return_value=False)
    def test_build_volume_info_without_volume(self, mock_volume_support,
                                              mock_device_path,
                                              mock_create_volume,
                                              mock_update_db):
        cfg.CONF.set_override('mount_point', 'fake-mount-point',
                              group='mysql')
        volume_info = self.freshinstancetasks._build_volume_info('mysql')
        self.assertFalse(mock_create_volume.called)
        expected = {
            'block_device': None,
            'device_path': 'fake-device-path',
            'mount_point': 'fake-mount-point'
        }
        self.assertEqual(expected, volume_info)

    @patch.object(trove.guestagent.api.API, 'attach_replication_slave')
    @patch.object(rpc, 'get_client')
    @patch.object(DBInstance, 'get_by')
    def test_attach_replication_slave(self, mock_get_by, mock_get_client,
                                      mock_attach_replication_slave):
        mock_flavor = {'id': 8, 'ram': 768, 'name': 'bigger_flavor'}
        snapshot = {'replication_strategy': 'MysqlGTIDReplication',
                    'master': {'id': 'master-id'}}
        config_content = {'config_contents': 'some junk'}
        replica_config = MagicMock()
        replica_config.config_contents = config_content
        with patch.object(taskmanager_models.FreshInstanceTasks,
                          '_render_replica_config',
                          return_value=replica_config):
            self.freshinstancetasks.attach_replication_slave(snapshot,
                                                             mock_flavor)
        mock_attach_replication_slave.assert_called_with(snapshot,
                                                         config_content)

    @patch.object(BaseInstance, 'update_db')
    @patch.object(rpc, 'get_client')
    @patch.object(taskmanager_models.FreshInstanceTasks,
                  '_render_replica_config')
    @patch.object(trove.guestagent.api.API, 'attach_replication_slave',
                  side_effect=GuestError)
    @patch('trove.taskmanager.models.LOG')
    @patch.object(DBInstance, 'get_by')
    def test_error_attach_replication_slave(self, *args):
        mock_flavor = {'id': 8, 'ram': 768, 'name': 'bigger_flavor'}
        snapshot = {'replication_strategy': 'MysqlGTIDReplication',
                    'master': {'id': 'master-id'}}
        self.assertRaisesRegex(
            TroveError, 'Error attaching instance',
            self.freshinstancetasks.attach_replication_slave,
            snapshot, mock_flavor)


class ResizeVolumeTest(trove_testtools.TestCase):

    def setUp(self):
        super(ResizeVolumeTest, self).setUp()
        self.utils_poll_until_patch = patch.object(utils, 'poll_until')
        self.utils_poll_until_mock = self.utils_poll_until_patch.start()
        self.addCleanup(self.utils_poll_until_patch.stop)
        self.timeutils_isotime_patch = patch.object(timeutils, 'isotime')
        self.timeutils_isotime_mock = self.timeutils_isotime_patch.start()
        self.addCleanup(self.timeutils_isotime_patch.stop)
        self.instance = Mock()
        self.old_vol_size = 1
        self.new_vol_size = 2
        self.action = taskmanager_models.ResizeVolumeAction(self.instance,
                                                            self.old_vol_size,
                                                            self.new_vol_size)

        class FakeGroup(object):
            def __init__(self):
                self.mount_point = 'var/lib/mysql'
                self.device_path = '/dev/vdb'

        self.taskmanager_models_CONF = patch.object(taskmanager_models, 'CONF')
        self.mock_conf = self.taskmanager_models_CONF.start()
        self.mock_conf.get = Mock(return_value=FakeGroup())
        self.addCleanup(self.taskmanager_models_CONF.stop)

    def tearDown(self):
        super(ResizeVolumeTest, self).tearDown()

    @patch('trove.taskmanager.models.LOG')
    def test_resize_volume_unmount_exception(self, mock_logging):
        self.instance.guest.unmount_volume = Mock(
            side_effect=GuestError("test exception"))
        self.assertRaises(GuestError,
                          self.action._unmount_volume,
                          recover_func=self.action._recover_restart)
        self.assertEqual(1, self.instance.restart.call_count)
        self.instance.guest.unmount_volume.side_effect = None
        self.instance.reset_mock()

    @patch('trove.taskmanager.models.LOG')
    def test_resize_volume_detach_exception(self, mock_logging):
        self.instance.nova_client.volumes.delete_server_volume = Mock(
            side_effect=nova_exceptions.ClientException("test exception"))
        self.assertRaises(nova_exceptions.ClientException,
                          self.action._detach_volume,
                          recover_func=self.action._recover_mount_restart)
        self.assertEqual(1, self.instance.guest.mount_volume.call_count)
        self.assertEqual(1, self.instance.restart.call_count)
        self.instance.nova_client.volumes.delete_server_volume.side_effect = (
            None)
        self.instance.reset_mock()

    @patch('trove.taskmanager.models.LOG')
    def test_resize_volume_extend_exception(self, mock_logging):
        self.instance.volume_client.volumes.extend = Mock(
            side_effect=cinder_exceptions.ClientException("test exception"))
        self.assertRaises(cinder_exceptions.ClientException,
                          self.action._extend,
                          recover_func=self.action._recover_full)
        attach_count = (
            self.instance.nova_client.volumes.create_server_volume.call_count)
        self.assertEqual(1, attach_count)
        self.assertEqual(1, self.instance.guest.mount_volume.call_count)
        self.assertEqual(1, self.instance.restart.call_count)
        self.instance.volume_client.volumes.extend.side_effect = None
        self.instance.reset_mock()

    def test_resize_volume_verify_extend_no_volume(self):
        self.instance.volume_client.volumes.get = Mock(
            return_value=None)
        self.assertRaises(exception.TroveError,
                          self.action._verify_extend)
        self.instance.reset_mock()

    @patch('trove.taskmanager.models.LOG')
    def test_resize_volume_poll_timeout(self, mock_logging):
        utils.poll_until = Mock(side_effect=PollTimeOut)
        self.assertRaises(PollTimeOut, self.action._verify_extend)
        self.assertEqual(2, self.instance.volume_client.volumes.get.call_count)
        utils.poll_until.side_effect = None
        self.instance.reset_mock()

    def test_resize_volume_active_server_succeeds(self, *args):
        server = Mock(status=InstanceStatus.ACTIVE)
        self.instance.attach_mock(server, 'server')

        self.action.execute()

        extend_count = self.instance.volume_client.volumes.extend.call_count
        self.assertEqual(1, extend_count)
        self.instance.reset_mock()

    def test_resize_volume_server_error_fails(self):
        server = Mock(status=InstanceStatus.ERROR)
        self.instance.attach_mock(server, 'server')

        self.assertRaises(TroveError, self.action.execute)
        self.instance.reset_mock()


class BuiltInstanceTasksTest(trove_testtools.TestCase):

    def get_inst_service_status(self, status_id, statuses):
        answers = []
        for i, status in enumerate(statuses):
            inst_svc_status = InstanceServiceStatus(status,
                                                    id="%s-%s" % (status_id,
                                                                  i))
            inst_svc_status.save = MagicMock(return_value=None)
            answers.append(inst_svc_status)
        return answers

    def _stub_volume_client(self):
        self.instance_task._volume_client = MagicMock(spec=cinderclient.Client)
        stub_volume_mgr = MagicMock(spec=cinderclient.volumes.VolumeManager)
        self.instance_task.volume_client.volumes = stub_volume_mgr
        stub_volume_mgr.extend = MagicMock(return_value=None)
        stub_new_volume = cinderclient.volumes.Volume(
            stub_volume_mgr, {'status': 'available', 'size': 2}, True)
        stub_volume_mgr.get = MagicMock(return_value=stub_new_volume)
        stub_volume_mgr.attach = MagicMock(return_value=None)

    def _stub_neutron_client(self):
        stub_neutron_client = self.instance_task._neutron_client = MagicMock(
            spec=neutronclient.Client)
        stub_neutron_client.list_floatingips = MagicMock(
            return_value={'floatingips': [{
                'floating_ip_address': '192.168.10.1',
                'id': 'fake-floatingip-id'}]})
        stub_neutron_client.list_ports = MagicMock(
            return_value={'ports': [{
                'fixed_ips': [{'ip_address': '10.0.0.1'},
                              {'ip_address': 'fd4a:7bef:d1ed:1::1'}],
                'id': 'fake-port-id'}]})

    def setUp(self):
        super(BuiltInstanceTasksTest, self).setUp()
        self.new_flavor = {'id': 8, 'ram': 768, 'name': 'bigger_flavor'}
        stub_nova_server = MagicMock(id='fake_id')
        self.rpc_patches = patch.multiple(
            rpc, get_notifier=MagicMock(), get_client=MagicMock())
        self.rpc_mocks = self.rpc_patches.start()
        self.addCleanup(self.rpc_patches.stop)
        db_instance = DBInstance(InstanceTasks.NONE,
                                 id=INST_ID,
                                 name='resize-inst-name',
                                 datastore_version_id='1',
                                 datastore_id='id-1',
                                 flavor_id='6',
                                 manager='mysql',
                                 created=timeutils.utcnow(),
                                 updated=timeutils.utcnow(),
                                 compute_instance_id='computeinst-id-1',
                                 tenant_id='testresize-tenant-id',
                                 volume_size='1',
                                 volume_id=VOLUME_ID)

        # this is used during the final check of whether the resize successful
        db_instance.server_status = 'HEALTHY'
        self.db_instance = db_instance
        self.dm_dv_load_by_uuid_patch = patch.object(
            datastore_models.DatastoreVersion, 'load_by_uuid', MagicMock(
                return_value=datastore_models.DatastoreVersion(db_instance)))
        self.dm_dv_load_by_uuid_mock = self.dm_dv_load_by_uuid_patch.start()
        self.addCleanup(self.dm_dv_load_by_uuid_patch.stop)
        self.dm_ds_load_patch = patch.object(
            datastore_models.Datastore, 'load', MagicMock(
                return_value=datastore_models.Datastore(db_instance)))
        self.dm_ds_load_mock = self.dm_ds_load_patch.start()
        self.addCleanup(self.dm_ds_load_patch.stop)

        self.instance_task = taskmanager_models.BuiltInstanceTasks(
            trove.common.context.TroveContext(),
            db_instance,
            stub_nova_server,
            InstanceServiceStatus(ServiceStatuses.RUNNING,
                                  id='inst-stat-id-0'))

        self.instance_task._guest = MagicMock(spec=trove.guestagent.api.API)
        self.instance_task._nova_client = MagicMock(
            spec=novaclient.client)
        self.stub_server_mgr = MagicMock(
            spec=novaclient.v2.servers.ServerManager)
        self.stub_running_server = MagicMock(
            spec=novaclient.v2.servers.Server)
        self.stub_running_server.status = 'HEALTHY'
        self.stub_running_server.flavor = {'id': 6, 'ram': 512}
        self.stub_verifying_server = MagicMock(
            spec=novaclient.v2.servers.Server)
        self.stub_verifying_server.id = 'fake_id'
        self.stub_verifying_server.status = 'VERIFY_RESIZE'
        self.stub_verifying_server.flavor = {'id': 8, 'ram': 768}
        self.stub_server_mgr.get = MagicMock(
            return_value=self.stub_verifying_server)
        self.instance_task._nova_client.servers = self.stub_server_mgr
        stub_flavor_manager = MagicMock(
            spec=novaclient.v2.flavors.FlavorManager)
        self.instance_task._nova_client.flavors = stub_flavor_manager

        nova_flavor = novaclient.v2.flavors.Flavor(stub_flavor_manager,
                                                   self.new_flavor,
                                                   True)
        stub_flavor_manager.get = MagicMock(return_value=nova_flavor)

        self.instance_task._volume_client = MagicMock(spec=cinderclient)
        self.instance_task._volume_client.volumes = Mock(
            spec=cinderclient_volumes.VolumeManager)

        answers = (status for status in
                   self.get_inst_service_status('inst_stat-id',
                                                [ServiceStatuses.SHUTDOWN,
                                                 ServiceStatuses.RUNNING,
                                                 ServiceStatuses.RUNNING,
                                                 ServiceStatuses.RUNNING]))

        def side_effect_func(*args, **kwargs):
            if 'instance_id' in kwargs:
                return next(answers)
            elif ('id' in kwargs and 'deleted' in kwargs
                  and not kwargs['deleted']):
                return db_instance
            else:
                return MagicMock()

        self.dbm_dbmb_patch = patch.object(
            trove.db.models.DatabaseModelBase, 'find_by',
            MagicMock(side_effect=side_effect_func))
        self.dbm_dbmb_mock = self.dbm_dbmb_patch.start()
        self.addCleanup(self.dbm_dbmb_patch.stop)

        self.template_patch = patch.object(
            template, 'SingleInstanceConfigTemplate',
            MagicMock(spec=template.SingleInstanceConfigTemplate))
        self.template_mock = self.template_patch.start()
        self.addCleanup(self.template_patch.stop)
        db_instance.save = MagicMock(return_value=None)
        self.tbmb_running_patch = patch.object(
            trove.backup.models.Backup, 'running',
            MagicMock(return_value=None))
        self.tbmb_running_mock = self.tbmb_running_patch.start()
        self.addCleanup(self.tbmb_running_patch.stop)

        if 'volume' in self._testMethodName:
            self._stub_volume_client()

        if ('floating_ips' in self._testMethodName or
                'public_ips' in self._testMethodName):
            self._stub_neutron_client()

    def tearDown(self):
        super(BuiltInstanceTasksTest, self).tearDown()

    def test_resize_flavor(self):
        orig_server = self.instance_task.server
        self.instance_task.resize_flavor({'id': 1, 'ram': 512},
                                         self.new_flavor)
        # verify
        self.assertIsNot(self.instance_task.server, orig_server)
        self.assertEqual(1, self.instance_task._guest.stop_db.call_count)
        orig_server.resize.assert_any_call(self.new_flavor['id'])
        self.assertThat(self.db_instance.task_status, Is(InstanceTasks.NONE))
        self.assertEqual(2, self.stub_server_mgr.get.call_count)
        self.assertThat(self.db_instance.flavor_id, Is(self.new_flavor['id']))

    @patch('trove.taskmanager.models.LOG')
    def test_resize_flavor_resize_failure(self, mock_logging):
        orig_server = self.instance_task.server
        self.stub_verifying_server.status = 'ERROR'
        with patch.object(self.instance_task._nova_client.servers, 'get',
                          return_value=self.stub_verifying_server):
            # execute
            self.assertRaises(TroveError, self.instance_task.resize_flavor,
                              {'id': 1, 'ram': 512}, self.new_flavor)
            # verify
            self.assertTrue(self.stub_server_mgr.get.called)
            self.assertIs(self.instance_task.server,
                          self.stub_verifying_server)
            self.assertEqual(1, self.instance_task._guest.stop_db.call_count)
            orig_server.resize.assert_any_call(self.new_flavor['id'])
            self.assertThat(self.db_instance.task_status,
                            Is(InstanceTasks.NONE))
            self.assertThat(self.db_instance.flavor_id, Is('6'))

    @patch.object(utils, 'poll_until')
    def test_reboot(self, mock_poll):
        self.instance_task.server.reboot = Mock()

        self.instance_task.reboot()

        self.instance_task._guest.stop_db.assert_any_call()
        self.instance_task.server.reboot.assert_any_call()
        self.instance_task._guest.restart.assert_any_call()

    @patch.object(BaseInstance, 'update_db')
    def test_detach_replica(self, mock_update_db):
        with patch.object(self.instance_task, 'reset_task_status') as tr_mock:
            self.instance_task.detach_replica(Mock(), True)
            self.instance_task._guest.detach_replica.assert_called_with(True)
            mock_update_db.assert_called_with(slave_of_id=None)
            tr_mock.assert_not_called()

        with patch.object(self.instance_task, 'reset_task_status') as tr_mock:
            self.instance_task.detach_replica(Mock(), False)
            self.instance_task._guest.detach_replica.assert_called_with(False)
            mock_update_db.assert_called_with(slave_of_id=None)
            tr_mock.assert_called_once_with()

    @patch.object(BaseInstance, 'update_db')
    @patch('trove.taskmanager.models.LOG')
    def test_error_detach_replica(self, mock_logging, mock_update_db):
        with patch.object(self.instance_task, 'reset_task_status') as tr_mock:
            with patch.object(self.instance_task._guest, 'detach_replica',
                              side_effect=GuestError):
                self.assertRaises(
                    GuestError, self.instance_task.detach_replica,
                    Mock(), True)
                mock_update_db.assert_not_called()
                tr_mock.assert_not_called()

        with patch.object(self.instance_task, 'reset_task_status') as tr_mock:
            with patch.object(self.instance_task._guest, 'detach_replica',
                              side_effect=GuestError):
                self.assertRaises(
                    GuestError, self.instance_task.detach_replica,
                    Mock(), False)
                mock_update_db.assert_not_called()
                tr_mock.assert_called_once_with()

    @patch.object(BaseInstance, 'update_db')
    def test_make_read_only(self, mock_update_db):
        read_only = MagicMock()
        self.instance_task.make_read_only(read_only)
        self.instance_task._guest.make_read_only.assert_called_with(read_only)

    @patch.object(BaseInstance, 'update_db')
    def test_attach_replica(self, mock_update_db):
        master = MagicMock()
        replica_context = trove_testtools.TroveTestContext(self)
        mock_guest = MagicMock()
        mock_guest.get_replica_context = Mock(return_value=replica_context)
        type(master).guest = PropertyMock(return_value=mock_guest)

        config_content = {'config_contents': 'some junk'}
        replica_config = MagicMock()
        replica_config.config_contents = config_content

        with patch.object(taskmanager_models.BuiltInstanceTasks,
                          '_render_replica_config',
                          return_value=replica_config):
            self.instance_task.attach_replica(master)
        self.instance_task._guest.attach_replica.assert_called_with(
            replica_context, config_content, restart=False)
        mock_update_db.assert_called_with(slave_of_id=master.id)

    @patch('trove.taskmanager.models.LOG')
    def test_error_attach_replica(self, mock_logging):
        with patch.object(self.instance_task._guest, 'attach_replica',
                          side_effect=GuestError):
            self.assertRaises(GuestError, self.instance_task.attach_replica,
                              Mock())

    @patch.object(BaseInstance, 'update_db')
    def test_enable_as_master(self, mock_update_db):
        test_func = self.instance_task._guest.enable_as_master
        config_content = {'config_contents': 'some junk'}
        replica_source_config = MagicMock()
        replica_source_config.config_contents = config_content
        with patch.object(self.instance_task, '_render_replica_source_config',
                          return_value=replica_source_config):
            self.instance_task.enable_as_master()
        mock_update_db.assert_called_with(slave_of_id=None)
        test_func.assert_called_with(config_content)

    def test_get_last_txn(self):
        self.instance_task.get_last_txn()
        self.instance_task._guest.get_last_txn.assert_any_call()

    def test_get_latest_txn_id(self):
        self.instance_task.get_latest_txn_id()
        self.instance_task._guest.get_latest_txn_id.assert_any_call()

    def test_wait_for_txn(self):
        self.instance_task.wait_for_txn(None)
        self.instance_task._guest.wait_for_txn.assert_not_called()
        txn = Mock()
        self.instance_task.wait_for_txn(txn)
        self.instance_task._guest.wait_for_txn.assert_called_with(txn)

    def test_cleanup_source_on_replica_detach(self):
        test_func = self.instance_task._guest.cleanup_source_on_replica_detach
        replica_info = Mock()
        self.instance_task.cleanup_source_on_replica_detach(replica_info)
        test_func.assert_called_with(replica_info)

    def test_demote_replication_master(self):
        self.instance_task.demote_replication_master()
        self.instance_task._guest.demote_replication_master.assert_any_call()

    @patch('trove.taskmanager.models.BuiltInstanceTasks.set_service_status')
    @patch('trove.taskmanager.models.BuiltInstanceTasks.is_service_healthy')
    @patch('trove.taskmanager.models.BuiltInstanceTasks.reset_task_status')
    def test_upgrade(self, mock_resetstatus, mock_check, mock_setstatus):
        dsv = MagicMock()
        attrs = {'name': 'new_version'}
        dsv.configure_mock(**attrs)
        mock_check.return_value = True
        self.instance_task._guest.pre_upgrade.return_value = {}

        self.instance_task.upgrade(dsv)

        self.instance_task._guest.upgrade.assert_called_once_with(
            {'datastore_version': 'new_version'})

    def test_fix_device_path(self):
        self.assertEqual("/dev/vdb", self.instance_task.
                         _fix_device_path("vdb"))
        self.assertEqual("/dev/dev", self.instance_task.
                         _fix_device_path("dev"))
        self.assertEqual("/dev/vdb/dev", self.instance_task.
                         _fix_device_path("vdb/dev"))


class BackupTasksTest(trove_testtools.TestCase):

    def setUp(self):
        super(BackupTasksTest, self).setUp()
        self.backup = backup_models.DBBackup()
        self.backup.id = 'backup_id'
        self.backup.name = 'backup_test',
        self.backup.description = 'test desc'
        self.backup.location = 'http://xxx/z_CLOUD/container/12e48.xbstream.gz'
        self.backup.instance_id = 'instance id'
        self.backup.created = 'yesterday'
        self.backup.updated = 'today'
        self.backup.size = 2.0
        self.backup.state = state.BackupState.NEW
        self.bm_backup_patches = patch.multiple(
            backup_models.Backup,
            delete=MagicMock(return_value=None),
            get_by_id=MagicMock(return_value=self.backup))
        self.bm_backup_mocks = self.bm_backup_patches.start()
        self.addCleanup(self.bm_backup_patches.stop)
        self.bm_DBBackup_patch = patch.object(
            backup_models.DBBackup, 'save',
            MagicMock(return_value=self.backup))
        self.bm_DBBackup_mock = self.bm_DBBackup_patch.start()
        self.addCleanup(self.bm_DBBackup_patch.stop)
        self.backup.delete = MagicMock(return_value=None)

    def tearDown(self):
        super(BackupTasksTest, self).tearDown()

    def test_delete_backup_nolocation(self):
        self.backup.location = ''
        taskmanager_models.BackupTasks.delete_backup('dummy context',
                                                     self.backup.id)

        self.assertTrue(self.backup.deleted)

    @patch('trove.common.clients.create_swift_client')
    def test_delete_backup_fail_delete_manifest(self, mock_swift_client):
        client_mock = MagicMock()
        client_mock.head_object.return_value = {}
        client_mock.delete_object.side_effect = ClientException("foo")
        mock_swift_client.return_value = client_mock

        self.assertRaises(
            TroveError,
            taskmanager_models.BackupTasks.delete_backup,
            'dummy context', self.backup.id
        )
        self.assertFalse(backup_models.Backup.delete.called)
        self.assertEqual(
            state.BackupState.DELETE_FAILED,
            self.backup.state,
            "backup should be in DELETE_FAILED status"
        )

    @patch('trove.common.clients.create_swift_client')
    def test_delete_backup_delete_swift(self, mock_swift_client):
        client_mock = MagicMock()
        mock_swift_client.return_value = client_mock

        taskmanager_models.BackupTasks.delete_backup(mock.ANY, self.backup.id)

        client_mock.head_object.assert_called_once_with('container',
                                                        '12e48.xbstream.gz')
        client_mock.delete_object.assert_called_once_with('container',
                                                          '12e48.xbstream.gz')

    def test_delete_backup_restored(self):
        self.backup.state = state.BackupState.RESTORED
        taskmanager_models.BackupTasks.delete_backup(mock.ANY, self.backup.id)

        self.assertTrue(self.backup.deleted)

    def test_parse_manifest(self):
        manifest = 'container/prefix'
        cont, prefix = taskmanager_models.BackupTasks._parse_manifest(manifest)
        self.assertEqual('container', cont)
        self.assertEqual('prefix', prefix)

    def test_parse_manifest_bad(self):
        manifest = 'bad_prefix'
        cont, prefix = taskmanager_models.BackupTasks._parse_manifest(manifest)
        self.assertIsNone(cont)
        self.assertIsNone(prefix)

    def test_parse_manifest_long(self):
        manifest = 'container/long/path/to/prefix'
        cont, prefix = taskmanager_models.BackupTasks._parse_manifest(manifest)
        self.assertEqual('container', cont)
        self.assertEqual('long/path/to/prefix', prefix)

    def test_parse_manifest_short(self):
        manifest = 'container/'
        cont, prefix = taskmanager_models.BackupTasks._parse_manifest(manifest)
        self.assertEqual('container', cont)
        self.assertEqual('', prefix)


class NotifyMixinTest(trove_testtools.TestCase):
    def test_get_service_id(self):
        id_map = {
            'mysql': '123',
            'percona': 'abc'
        }
        mixin = taskmanager_models.NotifyMixin()
        self.assertThat(mixin._get_service_id('mysql', id_map), Equals('123'))

    @patch('trove.taskmanager.models.LOG')
    def test_get_service_id_unknown(self, mock_logging):
        id_map = {
            'mysql': '123',
            'percona': 'abc'
        }
        transformer = taskmanager_models.NotifyMixin()
        self.assertThat(transformer._get_service_id('m0ng0', id_map),
                        Equals('unknown-service-id-error'))


class RootReportTest(trove_testtools.TestCase):

    def setUp(self):
        super(RootReportTest, self).setUp()
        util.init_db()

    def tearDown(self):
        super(RootReportTest, self).tearDown()

    def test_report_root_first_time(self):
        context = Mock()
        context.user = utils.generate_uuid()
        report = mysql_models.RootHistory.create(
            context, utils.generate_uuid())
        self.assertIsNotNone(report)

    def test_report_root_double_create(self):
        context = Mock()
        context.user = utils.generate_uuid()
        id = utils.generate_uuid()
        history = mysql_models.RootHistory(id, context.user).save()
        with patch.object(mysql_models.RootHistory, 'load',
                          Mock(return_value=history)):
            report = mysql_models.RootHistory.create(context, id)
            self.assertTrue(mysql_models.RootHistory.load.called)
            self.assertEqual(history.user, report.user)
            self.assertEqual(history.id, report.id)


class ClusterRootTest(trove_testtools.TestCase):

    @patch.object(common_models.RootHistory, "create")
    @patch.object(common_models.Root, "create")
    def test_cluster_root_create(self, root_create, root_history_create):
        context = Mock()
        context.user = utils.generate_uuid()
        id = utils.generate_uuid()
        password = "rootpassword"
        cluster_instances = [utils.generate_uuid(), utils.generate_uuid()]
        common_models.ClusterRoot.create(context, id, password,
                                         cluster_instances)
        root_create.assert_called_with(context, id, password,
                                       cluster_instances_list=None)
        self.assertEqual(2, root_history_create.call_count)
        calls = [
            call(context, cluster_instances[0]),
            call(context, cluster_instances[1])
        ]
        root_history_create.assert_has_calls(calls)

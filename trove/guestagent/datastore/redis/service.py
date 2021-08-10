# -*- coding: utf-8 -*-
# Copyright (c) 2013 Rackspace
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
import redis
from redis.exceptions import BusyLoadingError, ConnectionError

from oslo_log import log as logging

from trove.common import cfg
from trove.common.db.redis.models import RedisRootUser
from trove.common import exception
from trove.common.i18n import _
from trove.common.stream_codecs import PropertiesCodec, StringConverter
from trove.common import utils
from trove.guestagent.common.configuration import ConfigurationManager
from trove.guestagent.common.configuration import OneFileOverrideStrategy
from trove.guestagent.common import guestagent_utils
from trove.guestagent.common import operating_system
from trove.guestagent.datastore.redis import system
from trove.guestagent.datastore import service
from trove.guestagent import pkg
from trove.guestagent.utils import docker as docker_util
from trove.instance import service_status

LOG = logging.getLogger(__name__)
TIME_OUT = 1200
CONF = cfg.CONF
CLUSTER_CFG = 'clustering'
SYS_OVERRIDES_AUTH = 'auth_password'
packager = pkg.Package()


class RedisAppStatus(service.BaseDbStatus):
    """
    Handles all of the status updating for the redis guest agent.
    """
    CLIENT = None

    def __init__(self, docker_client):
        super(RedisAppStatus, self).__init__(docker_client)

    @classmethod
    def set_client(cls, client):
        cls.CLIENT = client

    def get_actual_db_status(self):
        status = docker_util.get_container_status(self.docker_client)
        if status == "running":
            return self._check_redis_ping()
        elif status == "not running":
            return service_status.ServiceStatuses.SHUTDOWN
        elif status == "paused":
            return service_status.ServiceStatuses.PAUSED
        elif status == "exited":
            return service_status.ServiceStatuses.SHUTDOWN
        elif status == "dead":
            return service_status.ServiceStatuses.CRASHED
        else:
            return service_status.ServiceStatuses.UNKNOWN

    def _check_redis_ping(self):
        try:
            if self.CLIENT.ping():
                return service_status.ServiceStatuses.RUNNING
        except BusyLoadingError:
            return service_status.ServiceStatuses.BLOCKED
        except ConnectionError:
            return service_status.ServiceStatuses.SHUTDOWN
        except Exception:
            LOG.exception("Error getting Redis status.")
            container_log = docker_util.get_container_logs(
                self.docker_client, tail='all')
            LOG.debug('container log: \n%s', '\n'.join(container_log))
            return service_status.ServiceStatuses.RUNNING

    def cleanup_stalled_db_services(self):
        utils.execute_with_timeout('pkill', '-9',
                                   'redis-server',
                                   run_as_root=True,
                                   root_helper='sudo')


class RedisApp(service.BaseDbApp):
    """
    Handles installation and configuration of redis
    on a trove instance.
    """

    def __init__(self, status, docker_client=None):
        """
        Sets default status and state_change_wait_time
        """
        super().__init__(status, docker_client)

        revision_dir = guestagent_utils.build_file_path(
            os.path.dirname(system.REDIS_CONFIG),
            ConfigurationManager.DEFAULT_STRATEGY_OVERRIDES_SUB_DIR)
        config_value_mappings = {'yes': True, 'no': False, "''": None}
        self._value_converter = StringConverter(config_value_mappings)
        self.configuration_manager = ConfigurationManager(
            system.REDIS_CONFIG,
            system.REDIS_OWNER, system.REDIS_OWNER,
            PropertiesCodec(
                unpack_singletons=False,
                string_mappings=config_value_mappings
            ), requires_root=True,
            override_strategy=OneFileOverrideStrategy(revision_dir))

        self.admin = self.build_admin_client()

    def restart(self):
        LOG.info("Restarting database")

        # Ensure folders permission for database.
        for folder in ['/etc/redis', '/var/run/redis']:
            operating_system.ensure_directory(
                folder, user=CONF.database_service_uid,
                group=CONF.database_service_uid, force=True,
                as_root=True)
        try:
            docker_util.restart_container(self.docker_client)
        except Exception:
            LOG.exception("Failed to restart database")
            raise exception.TroveError("Failed to restart database")

        if not self.status.wait_for_status(
                service_status.ServiceStatuses.HEALTHY,
                CONF.state_change_wait_time, update_db=True
        ):
            raise exception.TroveError("Failed to start database")

        LOG.info("Finished restarting database")

    def build_admin_client(self):
        password = self.get_configuration_property('requirepass')
        socket = self.get_configuration_property('unixsocket')
        cmd = self.get_config_command_name()

        return RedisAdmin(password=password, unix_socket_path=socket,
                          config_cmd=cmd)

    def _refresh_admin_client(self):
        self.admin = self.build_admin_client()
        return self.admin

    def install_if_needed(self, packages):
        """
        Install redis if needed do nothing if it is already installed.
        """
        LOG.info('Preparing Guest as Redis Server.')
        if not packager.pkg_is_installed(packages):
            LOG.info('Installing Redis.')
            self._install_redis(packages)
        LOG.info('Redis installed completely.')

    def _install_redis(self, packages):
        """
        Install the redis server.
        """
        LOG.debug('Installing redis server.')
        LOG.debug("Creating %s.", system.REDIS_CONF_DIR)
        operating_system.create_directory(system.REDIS_CONF_DIR, as_root=True)
        pkg_opts = {}
        packager.pkg_install(packages, pkg_opts, TIME_OUT)
        self.start_db()
        LOG.debug('Finished installing redis server.')

    def stop_db(self, update_db=False, do_not_start_on_reboot=False):
        self.status.stop_db_service(
            system.SERVICE_CANDIDATES, CONF.state_change_wait_time,
            disable_on_boot=do_not_start_on_reboot, update_db=update_db)

    def restart(self):
        self.status.restart_db_service(
            system.SERVICE_CANDIDATES, CONF.state_change_wait_time)

    def update_overrides(self, context, overrides, remove=False):
        if overrides:
            self.configuration_manager.apply_user_override(overrides)
            # apply requirepass at runtime
            # TODO(zhaochao): updating 'requirepass' here will be removed
            # in the future releases, Redis only use enable_root/disable_root
            # to set this parameter.
            if 'requirepass' in overrides:
                self.admin.config_set('requirepass', overrides['requirepass'])
                self._refresh_admin_client()

    def apply_overrides(self, client, overrides):
        """Use the 'CONFIG SET' command to apply configuration at runtime.
        Commands that appear multiple times have values separated by a
        white space. For instance, the following two 'save' directives from the
        configuration file...
            save 900 1
            save 300 10
        ... would be applied in a single command as:
            CONFIG SET save "900 1 300 10"
        Note that the 'CONFIG' command has been renamed to prevent
        users from using it to bypass configuration groups.
        """
        for prop_name, prop_args in overrides.items():
            args_string = self._join_lists(
                self._value_converter.to_strings(prop_args), ' ')
            client.config_set(prop_name, args_string)
            # NOTE(zhaochao): requirepass applied in update_overrides is
            # only kept for back compatibility. Now requirepass is set
            # via enable_root/disable_root, Redis admin client should be
            # refreshed here.
            if prop_name == "requirepass":
                client = self._refresh_admin_client()

    def _join_lists(self, items, sep):
        """Join list items (including items from sub-lists) into a string.
        Non-list inputs are returned unchanged.
        _join_lists('1234', ' ') = "1234"
        _join_lists(['1','2','3','4'], ' ') = "1 2 3 4"
        _join_lists([['1','2'], ['3','4']], ' ') = "1 2 3 4"
        """
        if isinstance(items, list):
            return sep.join([sep.join(e) if isinstance(e, list) else e
                             for e in items])
        return items

    def remove_overrides(self):
        self.configuration_manager.remove_user_override()

    def make_read_only(self, read_only):
        # Redis has no mechanism to make an instance read-only at present
        pass

    def start_db_with_conf_changes(self, config_contents, **kwargs):
        LOG.info('Starting redis with conf changes.')
        if self.status.is_running:
            format = 'Cannot start_db_with_conf_changes because status is %s.'
            LOG.debug(format, self.status)
            raise RuntimeError(format % self.status)
        LOG.info("Initiating config.")
        self.configuration_manager.reset_configuration(config_contents)
        # The configuration template has to be updated with
        # guestagent-controlled settings.
        self.apply_initial_guestagent_configuration()
        self.start_db(True)

    def start_db(self, update_db=False, ds_version=None, command=None, extra_volumes=None):
        """Start and wait for database service."""
        docker_image = CONF.get(CONF.datastore_manager).docker_image
        image = (f'{docker_image}:latest' if not ds_version else f'{docker_image}:{ds_version}')
        command = command if command else ''

        try:
            redis_pass = self.get_auth_password()
        except exception.UnprocessableEntity:
            redis_pass = utils.generate_random_password()
        # Get uid and gid
        user = "%s:%s" % (system.REDIS_OWNER, system.REDIS_OWNER)
        # Create folders for redis on localhost
        for folder in ['/etc/redis', '/var/run/redis']:
            operating_system.ensure_directory(
                folder, user=CONF.database_service_uid,
                group=CONF.database_service_uid, force=True,
                as_root=True)
        volumes = {
            "/etc/redis": {"bind": "/etc/redis", "mode": "rw"},
            "/var/run/redis": {"bind": "/var/run/redis", "mode": "rw"},
            "/var/lib/redis": {"bind": "/var/lib/redis", "mode": "rw"},
            "/var/lib/redis/data": {"bind": "/var/lib/redis/data", "mode": "rw"},
        }
        if extra_volumes:
            volumes.update(extra_volumes)

        # Expose ports
        ports = {}
        tcp_ports = cfg.get_configuration_property('tcp_ports')
        for port_range in tcp_ports:
            for port in port_range:
                ports[f'{port}/tcp'] = port

        try:
            docker_util.start_container(
                self.docker_client,
                image,
                volumes=volumes,
                network_mode="bridge",
                ports=ports,
                user=user,
                environment={
                    "REDIS_PASSWORD": redis_pass,
                    "DATA": system.REDIS_DATA_DIR,
                },
                command=command
            )

            # Save root password
            LOG.debug("Saving root credentials to local host.")
            self.save_password('redis', redis_pass)
        except Exception:
            LOG.exception("Failed to start database service")
            raise exception.TroveError("Failed to start database service")

        if not self.status.wait_for_status(
                service_status.ServiceStatuses.HEALTHY,
                CONF.state_change_wait_time, update_db
        ):
            raise exception.TroveError("Failed to start database service")

    def apply_initial_guestagent_configuration(self):
        """Update guestagent-controlled configuration properties.
        """

        # Hide the 'CONFIG' command from end users by mangling its name.
        self.admin.set_config_command_name(self._mangle_config_command_name())

        self.configuration_manager.apply_system_override(
            {'daemonize': 'yes',
             'protected-mode': 'no',
             'supervised': 'systemd',
             'pidfile': system.REDIS_PID_FILE,
             'logfile': system.REDIS_LOG_FILE,
             'dir': system.REDIS_DATA_DIR})

    def get_config_command_name(self):
        """Get current name of the 'CONFIG' command.
        """
        renamed_cmds = self.configuration_manager.get_value('rename-command')
        if renamed_cmds:
            for name_pair in renamed_cmds:
                if name_pair[0] == 'CONFIG':
                    return name_pair[1]

        return None

    def _mangle_config_command_name(self):
        """Hide the 'CONFIG' command from the clients by renaming it to a
        random string known only to the guestagent.
        Return the mangled name.
        """
        mangled = utils.generate_random_password()
        self._rename_command('CONFIG', mangled)
        return mangled

    def _rename_command(self, old_name, new_name):
        """It is possible to completely disable a command by renaming it
        to an empty string.
        """
        self.configuration_manager.apply_system_override(
            {'rename-command': [old_name, new_name]})

    def get_logfile(self):
        """Specify the log file name. Also the empty string can be used to
        force Redis to log on the standard output.
        Note that if you use standard output for logging but daemonize,
        logs will be sent to /dev/null
        """
        return self.get_configuration_property('logfile')

    def get_db_filename(self):
        """The filename where to dump the DB.
        """
        return self.get_configuration_property('dbfilename')

    def get_working_dir(self):
        """The DB will be written inside this directory,
        with the filename specified the 'dbfilename' configuration directive.
        The Append Only File will also be created inside this directory.
        """
        return self.get_configuration_property('dir')

    def get_persistence_filepath(self):
        """Returns the full path to the persistence file."""
        return guestagent_utils.build_file_path(
            self.get_working_dir(), self.get_db_filename())

    def get_port(self):
        """Port for this instance or default if not set."""
        return self.get_configuration_property('port', system.REDIS_PORT)

    def get_auth_password(self, **kwargs):
        """Client authentication password for this instance or None if not set.
        """
        return self.get_configuration_property('requirepass')

    def is_appendonly_enabled(self):
        """True if the Append Only File (AOF) persistence mode is enabled.
        """
        return self.get_configuration_property('appendonly', False)

    def get_append_file_name(self):
        """The name of the append only file (AOF).
        """
        return self.get_configuration_property('appendfilename')

    def is_cluster_enabled(self):
        """Only nodes that are started as cluster nodes can be part of a
        Redis Cluster.
        """
        return self.get_configuration_property('cluster-enabled', False)

    def enable_cluster(self):
        """In order to start a Redis instance as a cluster node enable the
        cluster support
        """
        self.configuration_manager.apply_system_override(
            {'cluster-enabled': 'yes'}, CLUSTER_CFG)

    def get_cluster_config_filename(self):
        """Cluster node configuration file.
        """
        return self.get_configuration_property('cluster-config-file')

    def set_cluster_config_filename(self, name):
        """Make sure that instances running in the same system do not have
        overlapping cluster configuration file names.
        """
        self.configuration_manager.apply_system_override(
            {'cluster-config-file': name}, CLUSTER_CFG)

    def get_cluster_node_timeout(self):
        """Cluster node timeout is the amount of milliseconds a node must be
        unreachable for it to be considered in failure state.
        """
        return self.get_configuration_property('cluster-node-timeout')

    def get_configuration_property(self, name, default=None):
        """Return the value of a Redis configuration property.
        Returns a single value for single-argument properties or
        a list otherwise.
        """
        return utils.unpack_singleton(
            self.configuration_manager.get_value(name, default))

    def cluster_meet(self, ip, port):
        try:
            utils.execute_with_timeout('redis-cli', 'cluster', 'meet',
                                       ip, port)
        except exception.ProcessExecutionError:
            LOG.exception('Error joining node to cluster at %s.', ip)
            raise

    def cluster_addslots(self, first_slot, last_slot):
        try:
            group_size = 200
            # Create list of slots represented in strings
            # eg. ['10', '11', '12', '13']
            slots = list(map(str, range(first_slot, last_slot + 1)))
            while slots:
                cmd = (['redis-cli', 'cluster', 'addslots']
                       + slots[0:group_size])
                out, err = utils.execute_with_timeout(*cmd, run_as_root=True,
                                                      root_helper='sudo')
                if 'OK' not in out:
                    raise RuntimeError(_('Error executing addslots: %s')
                                       % out)
                del slots[0:group_size]
        except exception.ProcessExecutionError:
            LOG.exception('Error adding slots %(first_slot)s-%(last_slot)s'
                          ' to cluster.',
                          {'first_slot': first_slot, 'last_slot': last_slot})
            raise

    def _get_node_info(self):
        try:
            out, _ = utils.execute_with_timeout('redis-cli', '--csv',
                                                'cluster', 'nodes')
            return [line.split(' ') for line in out.splitlines()]
        except exception.ProcessExecutionError:
            LOG.exception('Error getting node info.')
            raise

    def _get_node_details(self):
        for node_details in self._get_node_info():
            if 'myself' in node_details[2]:
                return node_details
        raise exception.TroveError(_("Unable to determine node details"))

    def get_node_ip(self):
        """Returns [ip, port] where both values are strings"""
        return self._get_node_details()[1].split(':')

    def get_node_id_for_removal(self):
        node_details = self._get_node_details()
        node_id = node_details[0]
        my_ip = node_details[1].split(':')[0]
        try:
            slots, _ = utils.execute_with_timeout('redis-cli', '--csv',
                                                  'cluster', 'slots')
            return node_id if my_ip not in slots else None
        except exception.ProcessExecutionError:
            LOG.exception('Error validating node to for removal.')
            raise

    def remove_nodes(self, node_ids):
        try:
            for node_id in node_ids:
                utils.execute_with_timeout('redis-cli', 'cluster',
                                           'forget', node_id)
        except exception.ProcessExecutionError:
            LOG.exception('Error removing node from cluster.')
            raise

    def enable_root(self, password=None):
        if not password:
            password = utils.generate_random_password()
        redis_password = RedisRootUser(password=password)
        try:
            self.configuration_manager.apply_system_override(
                {'requirepass': password, 'masterauth': password},
                change_id=SYS_OVERRIDES_AUTH)
            self.apply_overrides(
                self.admin, {'requirepass': password, 'masterauth': password})
        except exception.TroveError:
            LOG.exception('Error enabling authentication for instance.')
            raise
        return redis_password.serialize()

    def disable_root(self):
        try:
            self.configuration_manager.remove_system_override(
                change_id=SYS_OVERRIDES_AUTH)
            self.apply_overrides(self.admin,
                                 {'requirepass': '', 'masterauth': ''})
        except exception.TroveError:
            LOG.exception('Error disabling authentication for instance.')
            raise


class RedisAdmin(object):
    """Handles administrative tasks on the Redis database.
    """

    DEFAULT_CONFIG_CMD = 'CONFIG'

    def __init__(self, password=None, unix_socket_path=None, config_cmd=None):
        self.__client = redis.StrictRedis(
            password=password, unix_socket_path=unix_socket_path)
        self.__config_cmd_name = config_cmd or self.DEFAULT_CONFIG_CMD

    def set_config_command_name(self, name):
        """Set name of the 'CONFIG' command or None for default.
        """
        self.__config_cmd_name = name or self.DEFAULT_CONFIG_CMD

    def ping(self):
        """Ping the Redis server and return True if a response is received.
        """
        return self.__client.ping()

    def get_info(self, section=None):
        return self.__client.info(section=section)

    def persist_data(self):
        save_cmd = 'SAVE'
        last_save = self.__client.lastsave()
        LOG.debug("Starting Redis data persist")
        save_ok = True
        try:
            save_ok = self.__client.bgsave()
        except redis.exceptions.ResponseError as re:
            # If an auto-save is in progress just use it, since it must have
            # just happened
            if "Background save already in progress" in str(re):
                LOG.info("Waiting for existing background save to finish")
            else:
                raise
        if save_ok:
            save_cmd = 'BGSAVE'

            def _timestamp_changed():
                return last_save != self.__client.lastsave()

            try:
                utils.poll_until(_timestamp_changed, sleep_time=2,
                                 time_out=TIME_OUT)
            except exception.PollTimeOut:
                raise RuntimeError(_("Timeout occurred waiting for Redis "
                                     "persist (%s) to complete.") % save_cmd)

        # If the background save fails for any reason, try doing a foreground
        # one.  This blocks client connections, so we don't want it to be
        # the default.
        elif not self.__client.save():
            raise exception.BackupCreationError(_("Could not persist "
                                                  "Redis data (%s)") % save_cmd)
        LOG.debug("Redis data persist (%s) completed", save_cmd)

    def set_master(self, host=None, port=None):
        self.__client.slaveof(host, port)

    def config_set(self, name, value):
        response = self.execute(
            '%s %s' % (self.__config_cmd_name, 'SET'), name, value)
        if not self._is_ok_response(response):
            raise exception.UnprocessableEntity(
                _("Could not set configuration property '%(name)s' to "
                  "'%(value)s'.") % {'name': name, 'value': value})

    def _is_ok_response(self, response):
        """Return True if a given Redis response is 'OK'.
        """
        return response and redis.client.bool_ok(response)

    def execute(self, cmd_name, *cmd_args, **options):
        """Execute a command and return a parsed response.
        """
        try:
            return self.__client.execute_command(cmd_name, *cmd_args,
                                                 **options)
        except Exception as e:
            LOG.exception(e)
            raise exception.TroveError(
                _("Redis command '%(cmd_name)s %(cmd_args)s' failed.")
                % {'cmd_name': cmd_name, 'cmd_args': ' '.join(cmd_args)})

    def wait_until(self, key, wait_value, section=None, timeout=None):
        """Polls redis until the specified 'key' changes to 'wait_value'."""
        timeout = timeout or CONF.usage_timeout
        LOG.debug("Waiting for Redis '%(key)s' to be: %(value)s.",
                  {'key': key, 'value': wait_value})

        def _check_info():
            redis_info = self.get_info(section)
            if key in redis_info:
                current_value = redis_info[key]
                LOG.debug("Found '%(value)s' for field %(key)s.",
                          {'value': current_value, 'key': key})
            else:
                LOG.error('Output from Redis command: %s', redis_info)
                raise RuntimeError(_("Field %(field)s not found "
                                     "(Section: '%(sec)s').") %
                                   ({'field': key, 'sec': section}))
            return current_value == wait_value

        try:
            utils.poll_until(_check_info, time_out=timeout)
        except exception.PollTimeOut:
            raise RuntimeError(_("Timeout occurred waiting for Redis field "
                                 "'%(field)s' to change to '%(val)s'.") %
                               {'field': key, 'val': wait_value})

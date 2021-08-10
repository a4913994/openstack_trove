# -*- coding: utf-8 -*-
import os

from oslo_log import log as logging

from trove.common import cfg
from trove.guestagent.common import operating_system
from trove.guestagent.datastore import manager
from trove.guestagent.datastore.redis import service, system
from trove.guestagent import guest_log

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class RedisManager(manager.Manager):
    """
    This is the Redis manager class. It is dynamically loaded
    based off of the service_type of the trove instance
    """
    GUEST_LOG_DEFS_REDIS_LABEL = 'redis'

    def __init__(self):
        super(RedisManager, self).__init__('redis')
        self.status = service.RedisAppStatus(self.docker_client)
        self.app = service.RedisApp(self.status, self.docker_client)

    def _refresh_admin_client(self):
        self.admin = self.app.build_admin_client()
        self.status.set_client(self.admin)
        return self.admin

    @property
    def configuration_manager(self):
        return self.app.configuration_manager

    @property
    def datastore_log_defs(self):
        logfile = self.app.get_logfile()
        if not logfile:
            return {}
        return {
            self.GUEST_LOG_DEFS_REDIS_LABEL: {
                self.GUEST_LOG_TYPE_LABEL: guest_log.LogType.SYS,
                self.GUEST_LOG_USER_LABEL: CONF.database_service_uid,
                self.GUEST_LOG_FILE_LABEL: logfile
            }
        }

    def do_prepare(self, context, packages, databases, memory_mb, users, device_path, mount_point, backup_info,
                   config_contents, root_password, overrides, cluster_config, snapshot, ds_version=None):
        """This is called from prepare in the base class."""
        operating_system.ensure_directory(system.REDIS_LOG_DIR,
                                          user=CONF.database_service_uid,
                                          group=CONF.database_service_uid,
                                          as_root=True)
        operating_system.ensure_directory(system.REDIS_DATA_DIR,
                                          user=CONF.database_service_uid,
                                          group=CONF.database_service_uid,
                                          as_root=True)
        operating_system.ensure_directory('/etc/redis',
                                          user=CONF.database_service_uid,
                                          group=CONF.database_service_uid,
                                          as_root=True)

        LOG.info('Writing redis configuration.')
        if cluster_config:
            config_contents = (config_contents + "\n"
                               + "cluster-enabled yes\n"
                               + "cluster-config-file cluster.conf\n")
        self.app.configuration_manager.reset_configuration(config_contents)
        self.app.apply_initial_guestagent_configuration()
        self.adm = self.app.build_admin_client()
        self.status.set_client(self.adm)

        if backup_info:
            self.perform_restore(context, system.REDIS_DATA_DIR, backup_info)
            if not snapshot:
                signal_file = f"{system.REDIS_DATA_DIR}/recovery.signal"
                operating_system.execute_shell_cmd(
                    f"touch {signal_file}", [], shell=True, as_root=True)
                operating_system.chown(signal_file, CONF.database_service_uid, CONF.database_service_uid, force=True,
                                       as_root=True)

        if snapshot:
            self.attach_replica(context, snapshot, snapshot['config'])

        # config_file can only be set on the postgres command line
        command = f"redis-server {system.REDIS_CONFIG}"
        self.app.start_db(ds_version=ds_version, command=command)
        self.app.admin = self.app.build_admin_client()

    def pre_upgrade(self, context):
        mount_point = system.REDIS_DATA_DIR
        save_etc_dir = "%s/etc" % mount_point
        home_save = "%s/trove_user" % mount_point

        self.app.status.begin_restart()
        self.app.stop_db()

        operating_system.copy("%s/." % system.REDIS_CONF_DIR, save_etc_dir,
                              preserve=True, as_root=True)

        operating_system.copy("%s/." % os.path.expanduser('~'), home_save,
                              preserve=True, as_root=True)

        self.unmount_volume(context, mount_point=mount_point)

        return {
            'mount_point': mount_point,
            'save_etc_dir': save_etc_dir,
            'home_save': home_save
        }

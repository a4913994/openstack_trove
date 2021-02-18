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

import copy
import time
import traceback

from eventlet import greenthread
from eventlet.timeout import Timeout
from oslo_log import log as logging
from swiftclient.client import ClientException

from trove import rpc
from trove.backup import models as bkup_models
from trove.backup.models import Backup
from trove.backup.models import DBBackup
from trove.backup.state import BackupState
from trove.cluster import tasks
from trove.cluster.models import Cluster
from trove.cluster.models import DBCluster
from trove.common import cfg
from trove.common import clients
from trove.common import exception
from trove.common import neutron
from trove.common import template
from trove.common import timeutils
from trove.common import utils
from trove.common.clients import create_cinder_client
from trove.common.clients import create_dns_client
from trove.common.clients import create_guest_client
from trove.common.exception import BackupCreationError
from trove.common.exception import GuestError
from trove.common.exception import GuestTimeout
from trove.common.exception import InvalidModelError
from trove.common.exception import PollTimeOut
from trove.common.exception import TroveError
from trove.common.exception import VolumeCreationFailure
from trove.common.i18n import _
from trove.common.notification import DBaaSInstanceRestart
from trove.common.notification import DBaaSInstanceUpgrade
from trove.common.notification import EndNotification
from trove.common.notification import StartNotification
from trove.common.notification import TroveInstanceCreate
from trove.common.notification import TroveInstanceModifyFlavor
from trove.common.strategies.cluster import strategy
from trove.common.utils import try_recover
from trove.extensions.mysql import models as mysql_models
from trove.instance import models as inst_models
from trove.instance import service_status as srvstatus
from trove.instance.models import BuiltInstance
from trove.instance.models import DBInstance
from trove.instance.models import FreshInstance
from trove.instance.models import Instance
from trove.instance.models import InstanceServiceStatus
from trove.instance.models import InstanceStatus
from trove.instance.tasks import InstanceTasks
from trove.module import models as module_models
from trove.module import views as module_views
from trove.quota.quota import run_with_quotas

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class NotifyMixin(object):
    """Notification Mixin

    This adds the ability to send usage events to an Instance object.
    """

    def _get_service_id(self, datastore_manager, id_map):
        if datastore_manager in id_map:
            datastore_manager_id = id_map[datastore_manager]
        else:
            datastore_manager_id = cfg.UNKNOWN_SERVICE_ID
            LOG.error("Datastore ID for Manager (%s) is not configured",
                      datastore_manager)
        return datastore_manager_id

    def send_usage_event(self, event_type, **kwargs):
        event_type = 'trove.instance.%s' % event_type
        publisher_id = CONF.host
        # Grab the instance size from the kwargs or from the nova client
        instance_size = kwargs.pop('instance_size', None)
        flavor = self.nova_client.flavors.get(self.flavor_id)
        server = kwargs.pop('server', None)
        if server is None:
            server = self.nova_client.servers.get(self.server_id)
        az = getattr(server, 'OS-EXT-AZ:availability_zone', None)

        # Default payload
        created_time = timeutils.isotime(self.db_info.created)
        payload = {
            'availability_zone': az,
            'created_at': created_time,
            'name': self.name,
            'instance_id': self.id,
            'instance_name': self.name,
            'instance_size': instance_size or flavor.ram,
            'instance_type': flavor.name,
            'instance_type_id': flavor.id,
            'launched_at': created_time,
            'nova_instance_id': self.server_id,
            'region': CONF.region,
            'state_description': self.status,
            'state': self.status,
            'tenant_id': self.tenant_id,
            'user_id': self.context.user,
        }

        if CONF.get(self.datastore_version.manager).volume_support:
            payload.update({
                'volume_size': self.volume_size,
                'nova_volume_id': self.volume_id
            })

        payload['service_id'] = self._get_service_id(
            self.datastore_version.manager, CONF.notification_service_id)

        # Update payload with all other kwargs
        payload.update(kwargs)
        LOG.debug('Sending event: %(event_type)s, %(payload)s',
                  {'event_type': event_type, 'payload': payload})

        notifier = rpc.get_notifier(
            service="taskmanager", publisher_id=publisher_id)

        notifier.info(self.context, event_type, payload)


class ConfigurationMixin(object):
    """Configuration Mixin

    Configuration related tasks for instances and resizes.
    """

    def _render_config(self, flavor):
        config = template.SingleInstanceConfigTemplate(
            self.datastore_version, flavor, self.id)
        config.render()
        return config

    def _render_replica_source_config(self, flavor):
        config = template.ReplicaSourceConfigTemplate(
            self.datastore_version, flavor, self.id)
        config.render()
        return config

    def _render_replica_config(self, flavor):
        config = template.ReplicaConfigTemplate(
            self.datastore_version, flavor, self.id)
        config.render()
        return config

    def _render_config_dict(self, flavor):
        config = template.SingleInstanceConfigTemplate(
            self.datastore_version, flavor, self.id)
        ret = config.render_dict()
        LOG.debug("the default template dict of mysqld section: %s", ret)
        return ret


class ClusterTasks(Cluster):

    def update_statuses_on_failure(self, cluster_id, shard_id=None,
                                   status=None):

        if CONF.update_status_on_fail:
            if shard_id:
                db_instances = DBInstance.find_all(cluster_id=cluster_id,
                                                   shard_id=shard_id).all()
            else:
                db_instances = DBInstance.find_all(
                    cluster_id=cluster_id).all()

            for db_instance in db_instances:
                db_instance.set_task_status(
                    status or InstanceTasks.BUILDING_ERROR_SERVER)
                db_instance.save()

    @classmethod
    def get_ip(cls, instance):
        return instance.get_visible_ip_addresses()[0].get('address')

    def _all_instances_ready(self, instance_ids, cluster_id,
                             shard_id=None):
        """Wait for all instances to get READY."""
        return self._all_instances_acquire_status(
            instance_ids, cluster_id, shard_id,
            srvstatus.ServiceStatuses.INSTANCE_READY,
            fast_fail_statuses=[
                srvstatus.ServiceStatuses.FAILED,
                srvstatus.ServiceStatuses.FAILED_TIMEOUT_GUESTAGENT
            ]
        )

    def _all_instances_shutdown(self, instance_ids, cluster_id,
                                shard_id=None):
        """Wait for all instances to go SHUTDOWN."""
        return self._all_instances_acquire_status(
            instance_ids, cluster_id, shard_id,
            srvstatus.ServiceStatuses.SHUTDOWN,
            fast_fail_statuses=[
                srvstatus.ServiceStatuses.FAILED,
                srvstatus.ServiceStatuses.FAILED_TIMEOUT_GUESTAGENT
            ]
        )

    def _all_instances_running(self, instance_ids, cluster_id, shard_id=None):
        """Wait for all instances to become ACTIVE."""
        return self._all_instances_acquire_status(
            instance_ids, cluster_id, shard_id,
            srvstatus.ServiceStatuses.RUNNING,
            fast_fail_statuses=[
                srvstatus.ServiceStatuses.FAILED,
                srvstatus.ServiceStatuses.FAILED_TIMEOUT_GUESTAGENT
            ]
        )

    def _all_instances_acquire_status(
        self, instance_ids, cluster_id, shard_id, expected_status,
        fast_fail_statuses=None):

        def _is_fast_fail_status(status):
            return ((fast_fail_statuses is not None) and
                    ((status == fast_fail_statuses) or
                     (status in fast_fail_statuses)))

        def _all_have_status(ids):
            for instance_id in ids:
                status = InstanceServiceStatus.find_by(
                    instance_id=instance_id).get_status()
                task_status = DBInstance.find_by(
                    id=instance_id).get_task_status()
                if (_is_fast_fail_status(status) or
                    (task_status == InstanceTasks.BUILDING_ERROR_SERVER)):
                    # if one has failed, no need to continue polling
                    LOG.debug("Instance %(id)s has acquired a fast-fail "
                              "status %(status)s and"
                              " task_status %(task_status)s.",
                              {'id': instance_id, 'status': status,
                               'task_status': task_status})
                    return True
                if status != expected_status:
                    # if one is not in the expected state, continue polling
                    LOG.debug("Instance %(id)s was %(status)s.",
                              {'id': instance_id, 'status': status})
                    return False

            return True

        def _instance_ids_with_failures(ids):
            LOG.debug("Checking for service failures on instances: %s", ids)
            failed_instance_ids = []
            for instance_id in ids:
                status = InstanceServiceStatus.find_by(
                    instance_id=instance_id).get_status()
                task_status = DBInstance.find_by(
                    id=instance_id).get_task_status()
                if (_is_fast_fail_status(status) or
                    (task_status == InstanceTasks.BUILDING_ERROR_SERVER)):
                    failed_instance_ids.append(instance_id)
            return failed_instance_ids

        LOG.debug("Polling until all instances acquire %(expected)s "
                  "status: %(ids)s",
                  {'expected': expected_status, 'ids': instance_ids})
        try:
            utils.poll_until(lambda: instance_ids,
                             lambda ids: _all_have_status(ids),
                             sleep_time=CONF.usage_sleep_time,
                             time_out=CONF.usage_timeout)
        except PollTimeOut:
            LOG.exception("Timed out while waiting for all instances "
                          "to become %s.", expected_status)
            self.update_statuses_on_failure(cluster_id, shard_id)
            return False

        failed_ids = _instance_ids_with_failures(instance_ids)
        if failed_ids:
            LOG.error("Some instances failed: %s", failed_ids)
            self.update_statuses_on_failure(cluster_id, shard_id)
            return False

        LOG.debug("All instances have acquired the expected status %s.",
                  expected_status)

        return True

    def delete_cluster(self, context, cluster_id):

        LOG.debug("begin delete_cluster for id: %s", cluster_id)

        def all_instances_marked_deleted():
            db_instances = DBInstance.find_all(cluster_id=cluster_id,
                                               deleted=False).all()
            return len(db_instances) == 0

        try:
            utils.poll_until(all_instances_marked_deleted,
                             sleep_time=2,
                             time_out=CONF.cluster_delete_time_out)
        except PollTimeOut:
            LOG.error("timeout for instances to be marked as deleted.")
            return

        LOG.debug("setting cluster %s as deleted.", cluster_id)
        cluster = DBCluster.find_by(id=cluster_id)
        cluster.deleted = True
        cluster.deleted_at = timeutils.utcnow()
        cluster.task_status = tasks.ClusterTasks.NONE
        cluster.save()
        LOG.debug("end delete_cluster for id: %s", cluster_id)

    def rolling_restart_cluster(self, context, cluster_id, delay_sec=0):
        LOG.debug("Begin rolling cluster restart for id: %s", cluster_id)

        def _restart_cluster_instance(instance):
            LOG.debug("Restarting instance with id: %s", instance.id)
            context.notification = (
                DBaaSInstanceRestart(context, **request_info))
            with StartNotification(context, instance_id=instance.id):
                with EndNotification(context):
                    instance.update_db(task_status=InstanceTasks.REBOOTING)
                    instance.restart()

        timeout = Timeout(CONF.cluster_usage_timeout)
        cluster_notification = context.notification
        request_info = cluster_notification.serialize(context)
        try:
            node_db_inst = DBInstance.find_all(cluster_id=cluster_id,
                                               deleted=False).all()
            for index, db_inst in enumerate(node_db_inst):
                if index > 0:
                    LOG.debug(
                        "Waiting (%ds) for restarted nodes to rejoin the "
                        "cluster before proceeding.", delay_sec)
                    time.sleep(delay_sec)
                instance = BuiltInstanceTasks.load(context, db_inst.id)
                _restart_cluster_instance(instance)
        except Timeout as t:
            if t is not timeout:
                raise  # not my timeout
            LOG.exception("Timeout for restarting cluster.")
            raise
        except Exception:
            LOG.exception("Error restarting cluster.", cluster_id)
            raise
        finally:
            context.notification = cluster_notification
            timeout.cancel()
            self.reset_task()

        LOG.debug("End rolling restart for id: %s.", cluster_id)

    def rolling_upgrade_cluster(self, context, cluster_id,
                                datastore_version, ordering_function=None):
        LOG.debug("Begin rolling cluster upgrade for id: %s.", cluster_id)

        def _upgrade_cluster_instance(instance):
            LOG.debug("Upgrading instance with id: %s.", instance.id)
            context.notification = (
                DBaaSInstanceUpgrade(context, **request_info))
            with StartNotification(
                context, instance_id=instance.id,
                datastore_version_id=datastore_version.id):
                with EndNotification(context):
                    instance.update_db(
                        datastore_version_id=datastore_version.id,
                        task_status=InstanceTasks.UPGRADING)
                    instance.upgrade(datastore_version)

        timeout = Timeout(CONF.cluster_usage_timeout)
        cluster_notification = context.notification
        request_info = cluster_notification.serialize(context)
        try:
            instances = []
            for db_inst in DBInstance.find_all(cluster_id=cluster_id,
                                               deleted=False).all():
                instance = BuiltInstanceTasks.load(
                    context, db_inst.id)
                instances.append(instance)

            if ordering_function is not None:
                instances.sort(key=ordering_function)

            for instance in instances:
                _upgrade_cluster_instance(instance)

            self.reset_task()
        except Timeout as t:
            if t is not timeout:
                raise  # not my timeout
            LOG.exception("Timeout for upgrading cluster.")
            self.update_statuses_on_failure(
                cluster_id, status=InstanceTasks.UPGRADING_ERROR)
        except Exception:
            LOG.exception("Error upgrading cluster %s.", cluster_id)
            self.update_statuses_on_failure(
                cluster_id, status=InstanceTasks.UPGRADING_ERROR)
        finally:
            context.notification = cluster_notification
            timeout.cancel()

        LOG.debug("End upgrade_cluster for id: %s.", cluster_id)


class FreshInstanceTasks(FreshInstance, NotifyMixin, ConfigurationMixin):
    """
    FreshInstanceTasks contains the tasks related an instance that not
    associated with a compute server.
    """

    def wait_for_instance(self, timeout, flavor):
        # Make sure the service becomes active before sending a usage
        # record to avoid over billing a customer for an instance that
        # fails to build properly.
        error_message = ''
        error_details = ''
        try:
            LOG.info("Waiting for instance %s up and running with "
                     "timeout %ss", self.id, timeout)
            utils.poll_until(self._service_is_active,
                             sleep_time=CONF.usage_sleep_time,
                             time_out=timeout)

            LOG.info("Created instance %s successfully.", self.id)
            if not self.db_info.task_status.is_error:
                self.reset_task_status()
            TroveInstanceCreate(instance=self,
                                instance_size=flavor['ram']).notify()
        except exception.ComputeInstanceNotFound:
            # Check if the instance has been deleted by another request.
            instance = DBInstance.find_by(id=self.id)
            if (instance.deleted or
                    instance.task_status == InstanceTasks.DELETING):
                LOG.warning(f"Instance {self.id} has been deleted during "
                            f"waiting for creation")
                return
        except (TroveError, PollTimeOut) as ex:
            LOG.error("Failed to create instance %s, error: %s.",
                      self.id, str(ex))
            self.update_statuses_on_time_out()
            error_message = "%s" % ex
            error_details = traceback.format_exc()
        except Exception as ex:
            LOG.error("Failed to send usage create-event for instance %s, "
                      "error: %s", self.id, str(ex))
            error_message = "%s" % ex
            error_details = traceback.format_exc()
        finally:
            if error_message:
                inst_models.save_instance_fault(
                    self.id, error_message, error_details,
                    skip_delta=CONF.usage_sleep_time + 1
                )

    def _create_port(self, network_info, security_groups, is_mgmt=False,
                     is_public=False):
        name = 'trove-%s' % self.id
        type = 'Management' if is_mgmt else 'User'
        description = '%s port for trove instance %s' % (type, self.id)

        try:
            port_id = neutron.create_port(
                self.neutron_client, name,
                description, network_info.get('network_id'),
                security_groups,
                is_public=is_public,
                subnet_id=network_info.get('subnet_id'),
                ip=network_info.get('ip_address'),
                is_mgmt=is_mgmt,
                project_id=self.tenant_id
            )
        except Exception as e:
            self.update_db(
                task_status=inst_models.InstanceTasks.BUILDING_ERROR_PORT
            )
            error = (f"Failed to create {type} port for instance {self.id}: "
                     f"{str(e)}")
            raise TroveError(message=error)

        return port_id

    def _prepare_networks_for_instance(self, datastore_manager, nics,
                                       access=None):
        """Prepare the networks for the trove instance.

        'nics' contains the networks that management network always comes at
        last.

        returns a list of dicts which only contains port-id.
        """
        LOG.info("Preparing networks for the instance %s", self.id)
        security_group = None
        networks = copy.deepcopy(nics)
        access = access or {}

        if CONF.trove_security_groups_support:
            security_group = self._create_secgroup(
                datastore_manager,
                access.get('allowed_cidrs', [])
            )
            LOG.info(
                "Security group %s created for instance %s",
                security_group, self.id
            )

        # Create management port
        if CONF.management_networks:
            port_sgs = [security_group] if security_group else []
            if len(CONF.management_security_groups) > 0:
                port_sgs = CONF.management_security_groups
            # The management network is always the last one
            networks.pop(-1)
            port_id = self._create_port(
                {'network_id': CONF.management_networks[-1]},
                port_sgs,
                is_mgmt=True,
            )
            LOG.info("Management port %s created for instance: %s", port_id,
                     self.id)
            networks.append({"port-id": port_id})

        if not CONF.management_networks and not networks:
            return None

        # Create port in the user defined network, associate floating IP if
        # needed
        if len(networks) > 1 or not CONF.management_networks:
            network_info = networks.pop(0)
            port_sgs = [security_group] if security_group else []
            port_id = self._create_port(
                network_info,
                port_sgs,
                is_mgmt=False,
                is_public=access.get('is_public', False),
            )
            LOG.info("User port %s created for instance %s", port_id,
                     self.id)
            networks.insert(0, {"port-id": port_id})

        LOG.info(
            "Finished to prepare networks for the instance %s, networks: %s",
            self.id, networks
        )
        return networks

    def create_instance(self, flavor, image_id, databases, users,
                        datastore_manager, packages, volume_size,
                        backup_id, availability_zone, root_password, nics,
                        overrides, cluster_config, snapshot, volume_type,
                        modules, scheduler_hints, access=None,
                        ds_version=None):
        """Create trove instance.

        It is the caller's responsibility to ensure that
        FreshInstanceTasks.wait_for_instance is called after
        create_instance to ensure that the proper usage event gets sent
        """
        LOG.info(
            "Creating instance %s, nics: %s, access: %s",
            self.id, nics, access
        )

        networks = self._prepare_networks_for_instance(
            datastore_manager, nics, access=access
        )
        files = self.get_injected_files(datastore_manager, ds_version)
        cinder_volume_type = volume_type or CONF.cinder_volume_type
        volume_info = self._create_server_volume(
            flavor['id'], image_id,
            datastore_manager, volume_size,
            availability_zone, networks,
            files, cinder_volume_type,
            scheduler_hints
        )

        config = self._render_config(flavor)

        backup_info = None
        if backup_id is not None:
            backup = bkup_models.Backup.get_by_id(self.context, backup_id)
            backup_info = {'id': backup_id,
                           'instance_id': backup.instance_id,
                           'location': backup.location,
                           'type': backup.backup_type,
                           'checksum': backup.checksum,
                           }
        self._guest_prepare(flavor['ram'], volume_info,
                            packages, databases, users, backup_info,
                            config.config_contents, root_password,
                            overrides,
                            cluster_config, snapshot, modules,
                            ds_version=ds_version)

        if root_password:
            self.report_root_enabled()

        # when DNS is supported, we attempt to add this after the
        # instance is prepared.  Otherwise, if DNS fails, instances
        # end up in a poorer state and there's no tooling around
        # re-sending the prepare call; retrying DNS is much easier.
        try:
            self._create_dns_entry()
        except Exception as e:
            log_fmt = "Error creating DNS entry for instance: %s"
            exc_fmt = _("Error creating DNS entry for instance: %s")
            err = inst_models.InstanceTasks.BUILDING_ERROR_DNS
            self._log_and_raise(e, log_fmt, exc_fmt, self.id, err)

    def attach_replication_slave(self, snapshot, flavor):
        LOG.debug("Calling attach_replication_slave for %s.", self.id)
        try:
            replica_config = self._render_replica_config(flavor)
            self.guest.attach_replication_slave(snapshot,
                                                replica_config.config_contents)
        except GuestError as e:
            log_fmt = "Error attaching instance %s as replica."
            exc_fmt = _("Error attaching instance %s as replica.")
            err = inst_models.InstanceTasks.BUILDING_ERROR_REPLICA
            self._log_and_raise(e, log_fmt, exc_fmt, self.id, err)

    def get_replication_master_snapshot(self, context, slave_of_id, flavor,
                                        parent_backup_id=None):
        # First check to see if we need to take a backup
        master = BuiltInstanceTasks.load(context, slave_of_id)
        backup_required = master.backup_required_for_replication()
        if backup_required:
            # if we aren't passed in a backup id, look it up to possibly do
            # an incremental backup, thus saving time
            if not parent_backup_id:
                backup = Backup.get_last_completed(
                    context, slave_of_id, include_incremental=True)
                if backup:
                    parent_backup_id = backup.id
        else:
            LOG.debug('Skip creating replication master backup')

        snapshot_info = {
            'name': "Replication snapshot for %s" % self.id,
            'description': "Backup image used to initialize replication slave",
            'instance_id': slave_of_id,
            'parent_id': parent_backup_id,
            'tenant_id': self.tenant_id,
            'state': BackupState.NEW,
            'datastore_version_id': self.datastore_version.id,
            'deleted': False,
            'replica_number': 1,
        }

        replica_backup_id = None
        if backup_required:
            try:
                db_info = DBBackup.create(**snapshot_info)
                replica_backup_id = db_info.id
            except InvalidModelError:
                log_fmt = ("Unable to create replication snapshot record "
                           "for instance: %s")
                exc_fmt = _("Unable to create replication snapshot record "
                            "for instance: %s")
                LOG.exception(log_fmt, self.id)
                raise BackupCreationError(exc_fmt % self.id)
            if parent_backup_id:
                # Look up the parent backup  info or fail early if not
                #  found or if the user does not have access to the parent.
                _parent = Backup.get_by_id(context, parent_backup_id)
                parent = {
                    'location': _parent.location,
                    'checksum': _parent.checksum,
                }
                snapshot_info.update({
                    'parent': parent,
                })

        try:
            snapshot_info.update({
                'id': replica_backup_id,
                'datastore': master.datastore.name,
                'datastore_version': master.datastore_version.name,
            })
            snapshot = master.get_replication_snapshot(
                snapshot_info, flavor=master.flavor_id)
            snapshot.update({
                'config': self._render_replica_config(flavor).config_contents
            })
            return snapshot
        except Exception as e_create:
            create_log_fmt = (
                "Error creating replication snapshot from "
                "instance %(source)s for new replica %(replica)s.")
            create_exc_fmt = (
                "Error creating replication snapshot from "
                "instance %(source)s for new replica %(replica)s.")
            create_fmt_content = {
                'source': slave_of_id,
                'replica': self.id
            }
            err = inst_models.InstanceTasks.BUILDING_ERROR_REPLICA
            e_create_fault = create_log_fmt % create_fmt_content
            e_create_stack = traceback.format_exc()
            # we persist fault details to source instance
            inst_models.save_instance_fault(slave_of_id, e_create_fault,
                                            e_create_stack)

            # if the delete of the 'bad' backup fails, it'll mask the
            # create exception, so we trap it here
            try:
                if backup_required:
                    Backup.delete(context, replica_backup_id)
            except Exception as e_delete:
                LOG.error(create_log_fmt, create_fmt_content)
                # Make sure we log any unexpected errors from the create
                if not isinstance(e_create, TroveError):
                    LOG.exception(e_create)
                delete_log_fmt = (
                    "An error occurred while deleting a bad "
                    "replication snapshot from instance %(source)s.")
                delete_exc_fmt = _(
                    "An error occurred while deleting a bad "
                    "replication snapshot from instance %(source)s.")
                # we've already logged the create exception, so we'll raise
                # the delete (otherwise the create will be logged twice)
                self._log_and_raise(e_delete, delete_log_fmt, delete_exc_fmt,
                                    {'source': slave_of_id}, err)

            # the delete worked, so just log the original problem with create
            self._log_and_raise(e_create, create_log_fmt, create_exc_fmt,
                                create_fmt_content, err)

    def report_root_enabled(self):
        mysql_models.RootHistory.create(self.context, self.id)

    def update_statuses_on_time_out(self):
        if CONF.update_status_on_fail:
            # Updating service status
            service = InstanceServiceStatus.find_by(instance_id=self.id)
            service.set_status(
                srvstatus.ServiceStatuses.FAILED_TIMEOUT_GUESTAGENT)
            service.save()
            LOG.error(
                "Service status: %s, service error description: %s",
                srvstatus.ServiceStatuses.FAILED_TIMEOUT_GUESTAGENT.api_status,
                srvstatus.ServiceStatuses.FAILED_TIMEOUT_GUESTAGENT.description
            )

            # Updating instance status
            db_info = DBInstance.find_by(id=self.id, deleted=False)
            db_info.set_task_status(InstanceTasks.BUILDING_ERROR_TIMEOUT_GA)
            db_info.save()
            LOG.error(
                "Trove instance status: %s, Trove instance status "
                "description: %s",
                InstanceTasks.BUILDING_ERROR_TIMEOUT_GA.action,
                InstanceTasks.BUILDING_ERROR_TIMEOUT_GA.db_text
            )

    def _service_is_active(self):
        """
        Check that the database guest is active.

        This function is meant to be called with poll_until to check that
        the guest is alive before sending a 'create' message. This prevents
        over billing a customer for an instance that they can never use.

        Returns: boolean if the service is active.
        Raises: TroveError if the service is in a failure state.
        """
        service = InstanceServiceStatus.find_by(instance_id=self.id)
        status = service.get_status()

        if (status == srvstatus.ServiceStatuses.RUNNING or
            status == srvstatus.ServiceStatuses.INSTANCE_READY or
            status == srvstatus.ServiceStatuses.HEALTHY):
            return True
        elif status not in [srvstatus.ServiceStatuses.NEW,
                            srvstatus.ServiceStatuses.BUILDING,
                            srvstatus.ServiceStatuses.UNKNOWN,
                            srvstatus.ServiceStatuses.DELETED]:
            raise TroveError(_("Service not active, status: %s") % status)

        c_id = self.db_info.compute_instance_id
        try:
            server = self.nova_client.servers.get(c_id)
        except Exception as e:
            if getattr(e, 'message', '') == 'Not found':
                raise exception.ComputeInstanceNotFound(instance_id=self.id,
                                                        server_id=c_id)
            else:
                raise TroveError(
                    _("Failed to get server %(server)s for instance "
                      "%(instance)s, error: %(error)s"),
                    server=c_id, instance=self.id, error=str(e)
                )

        server_status = server.status
        if server_status in [InstanceStatus.ERROR,
                             InstanceStatus.FAILED]:
            server_fault_message = 'No fault found'
            try:
                server_fault_message = server.fault.get('message', 'Unknown')
            except AttributeError:
                pass
            raise TroveError(
                _("Server not active, status: %(status)s, fault message: "
                  "%(srv_msg)s") %
                {'status': server_status, 'srv_msg': server_fault_message}
            )
        return False

    def _build_sg_rules_mapping(self, rule_ports):
        final = []
        cidr = CONF.trove_security_group_rule_cidr
        for port_or_range in set(rule_ports):
            from_, to_ = port_or_range[0], port_or_range[-1]
            final.append({'cidr': cidr,
                          'from_': str(from_),
                          'to_': str(to_)})
        return final

    def _create_server_volume(self, flavor_id, image_id, datastore_manager,
                              volume_size, availability_zone, nics, files,
                              volume_type, scheduler_hints):
        LOG.debug("Begin _create_server_volume for id: %s", self.id)
        server = None
        volume_info = self._build_volume_info(
            datastore_manager,
            volume_size=volume_size,
            volume_type=volume_type,
            availability_zone=availability_zone)
        block_device_mapping_v2 = volume_info['block_device']
        try:
            server = self._create_server(
                flavor_id, image_id,
                datastore_manager,
                block_device_mapping_v2,
                availability_zone, nics, files,
                scheduler_hints
            )
            server_id = server.id
            # Save server ID.
            self.update_db(compute_instance_id=server_id)
        except Exception as e:
            log_fmt = "Failed to create server for instance %s"
            exc_fmt = _("Failed to create server for instance %s")
            err = inst_models.InstanceTasks.BUILDING_ERROR_SERVER
            self._log_and_raise(e, log_fmt, exc_fmt, self.id, err)
        LOG.debug("End _create_server_volume for id: %s", self.id)
        return volume_info

    def _build_volume_info(self, datastore_manager, volume_size=None,
                           volume_type=None, availability_zone=None):
        volume_info = None
        volume_support = self.volume_support
        device_path = self.device_path
        mount_point = CONF.get(datastore_manager).mount_point
        LOG.debug("trove volume support = %s", volume_support)
        if volume_support:
            try:
                volume_info = self._create_volume(
                    volume_size, volume_type, datastore_manager,
                    availability_zone)
            except Exception as e:
                log_fmt = "Failed to create volume for instance %s"
                exc_fmt = _("Failed to create volume for instance %s")
                err = inst_models.InstanceTasks.BUILDING_ERROR_VOLUME
                self._log_and_raise(e, log_fmt, exc_fmt, self.id, err)
        else:
            LOG.debug("device_path = %(path)s\n"
                      "mount_point = %(point)s",
                      {
                          "path": device_path,
                          "point": mount_point
                      })
            volume_info = {
                'block_device': None,
                'device_path': device_path,
                'mount_point': mount_point,
            }
        return volume_info

    # We remove all translations for messages logging execpet those for
    # exception raising. And we cannot use _(xxxx) instead of _("xxxx")
    # because of H701 PEP8 checking. So we pass log format , exception
    # format, and format content in and do translations only if needed.
    def _log_and_raise(self, exc, log_fmt, exc_fmt,
                       fmt_content, task_status):
        LOG.error("%(message)s\n%(exc)s\n%(trace)s",
                  {"message": log_fmt % fmt_content,
                   "exc": exc,
                   "trace": traceback.format_exc()})
        self.update_db(task_status=task_status)
        exc_message = '\n%s' % exc if exc else ''
        full_message = "%s%s" % (exc_fmt % fmt_content, exc_message)
        raise TroveError(message=full_message)

    def _create_volume(self, volume_size, volume_type, datastore_manager,
                       availability_zone):
        LOG.debug("Begin _create_volume for id: %s", self.id)
        volume_client = create_cinder_client(self.context, self.region_name)
        volume_desc = ("datastore volume for %s" % self.id)
        volume_kwargs = {
            'size': volume_size,
            'name': "trove-%s" % self.id,
            'description': volume_desc,
            'volume_type': volume_type}
        if CONF.enable_volume_az:
            volume_kwargs['availability_zone'] = availability_zone
        volume_ref = volume_client.volumes.create(**volume_kwargs)

        # Record the volume ID in case something goes wrong.
        self.update_db(volume_id=volume_ref.id)

        utils.poll_until(
            lambda: volume_client.volumes.get(volume_ref.id),
            lambda v_ref: v_ref.status in ['available', 'error'],
            sleep_time=2,
            time_out=CONF.volume_time_out)

        v_ref = volume_client.volumes.get(volume_ref.id)
        if v_ref.status in ['error']:
            raise VolumeCreationFailure()
        LOG.debug("End _create_volume for id: %s", self.id)
        return self._build_volume(v_ref, datastore_manager)

    def _build_volume(self, v_ref, datastore_manager):
        LOG.debug("Created volume %s", v_ref)
        # TODO(zhaochao): from Liberty, Nova libvirt driver does not honor
        # user-supplied device name anymore, so we may need find a new
        # method to make sure the volume is correctly mounted inside the
        # guest, please refer to the 'intermezzo-problem-with-device-names'
        # section of Nova user referrence at:
        # https://docs.openstack.org/nova/latest/user/block-device-mapping.html
        bdm = CONF.block_device_mapping

        # use Nova block_device_mapping_v2, referrence:
        # https://docs.openstack.org/api-ref/compute/#create-server
        # setting the delete_on_terminate instance to true=1
        block_device_v2 = [{
            "uuid": v_ref.id,
            "source_type": "volume",
            "destination_type": "volume",
            "device_name": bdm,
            "volume_size": v_ref.size,
            "delete_on_termination": True
        }]
        created_volumes = [{'id': v_ref.id,
                            'size': v_ref.size}]

        device_path = self.device_path
        mount_point = CONF.get(datastore_manager).mount_point

        LOG.debug("block_device = %(device)s\n"
                  "volume = %(volume)s\n"
                  "device_path = %(path)s\n"
                  "mount_point = %(point)s",
                  {"device": block_device_v2,
                   "volume": created_volumes,
                   "path": device_path,
                   "point": mount_point})

        volume_info = {'block_device': block_device_v2,
                       'device_path': device_path,
                       'mount_point': mount_point}
        return volume_info

    def _create_server(self, flavor_id, image_id, datastore_manager,
                       block_device_mapping_v2, availability_zone,
                       nics, files={}, scheduler_hints=None):
        userdata = self.prepare_userdata(datastore_manager)
        metadata = {'trove_project_id': self.tenant_id,
                    'trove_user_id': self.context.user,
                    'trove_instance_id': self.id}
        name = self.hostname or self.name
        bdmap_v2 = block_device_mapping_v2
        config_drive = CONF.use_nova_server_config_drive
        key_name = CONF.nova_keypair

        server = self.nova_client.servers.create(
            name, image_id, flavor_id, key_name=key_name, nics=nics,
            block_device_mapping_v2=bdmap_v2,
            files=files, userdata=userdata,
            availability_zone=availability_zone,
            config_drive=config_drive, scheduler_hints=scheduler_hints,
            meta=metadata,
        )
        LOG.debug("Created new compute instance %(server_id)s "
                  "for database instance %(id)s",
                  {'server_id': server.id, 'id': self.id})
        return server

    def _guest_prepare(self, flavor_ram, volume_info,
                       packages, databases, users, backup_info=None,
                       config_contents=None, root_password=None,
                       overrides=None, cluster_config=None, snapshot=None,
                       modules=None, ds_version=None):
        LOG.debug("Entering guest_prepare")
        # Now wait for the response from the create to do additional work
        self.guest.prepare(flavor_ram, packages, databases, users,
                           device_path=volume_info['device_path'],
                           mount_point=volume_info['mount_point'],
                           backup_info=backup_info,
                           config_contents=config_contents,
                           root_password=root_password,
                           overrides=overrides,
                           cluster_config=cluster_config,
                           snapshot=snapshot, modules=modules,
                           ds_version=ds_version)

    def _create_dns_entry(self):
        dns_support = CONF.trove_dns_support
        LOG.debug("trove dns support = %s", dns_support)

        if dns_support:
            LOG.debug("%(gt)s: Creating dns entry for instance: %(id)s",
                      {'gt': greenthread.getcurrent(), 'id': self.id})
            dns_client = create_dns_client(self.context)

            def get_server():
                c_id = self.db_info.compute_instance_id
                return self.nova_client.servers.get(c_id)

            def ip_is_available(server):
                LOG.debug("Polling for ip addresses: $%s ", server.addresses)
                if server.addresses != {}:
                    return True
                elif (server.addresses == {} and
                      server.status != InstanceStatus.ERROR):
                    return False
                elif (server.addresses == {} and
                      server.status == InstanceStatus.ERROR):
                    LOG.error("Failed to create DNS entry for instance "
                              "%(instance)s. Server status was "
                              "%(status)s).",
                              {'instance': self.id, 'status': server.status})
                    raise TroveError(status=server.status)

            utils.poll_until(get_server, ip_is_available,
                             sleep_time=1, time_out=CONF.dns_time_out)
            server = self.nova_client.servers.get(
                self.db_info.compute_instance_id)
            self.db_info.addresses = server.addresses
            LOG.debug("Creating dns entry...")
            ip = self.dns_ip_address
            if not ip:
                raise TroveError(_("Failed to create DNS entry for instance "
                                   "%s. No IP available.") % self.id)
            dns_client.create_instance_entry(self.id, ip)
            LOG.debug("Successfully created DNS entry for instance: %s",
                      self.id)
        else:
            LOG.debug("%(gt)s: DNS not enabled for instance: %(id)s",
                      {'gt': greenthread.getcurrent(), 'id': self.id})

    def _create_secgroup(self, datastore_manager, allowed_cidrs):
        name = "%s-%s" % (CONF.trove_security_group_name_prefix, self.id)

        try:
            sg_id = neutron.create_security_group(
                self.neutron_client, name, self.id
            )

            if not allowed_cidrs:
                allowed_cidrs = [CONF.trove_security_group_rule_cidr]
            tcp_ports = CONF.get(datastore_manager).tcp_ports
            udp_ports = CONF.get(datastore_manager).udp_ports

            neutron.create_security_group_rule(
                self.neutron_client, sg_id, 'tcp', tcp_ports, allowed_cidrs
            )
            neutron.create_security_group_rule(
                self.neutron_client, sg_id, 'udp', udp_ports, allowed_cidrs
            )
        except Exception:
            message = ("Failed to create security group for instance %s"
                       % self.id)
            LOG.exception(message)
            self.update_db(
                task_status=inst_models.InstanceTasks.BUILDING_ERROR_SEC_GROUP
            )
            raise TroveError(message=message)

        return sg_id


class BuiltInstanceTasks(BuiltInstance, NotifyMixin, ConfigurationMixin):
    """
    BuiltInstanceTasks contains the tasks related an instance that already
    associated with a compute server.
    """

    def is_service_healthy(self):
        """Wait for the db service up and running.

        This method is supposed to be called with poll_until against an
        existing db instance.
        """
        service = InstanceServiceStatus.find_by(instance_id=self.id)
        status = service.get_status()

        if service.is_uptodate():
            if status in [srvstatus.ServiceStatuses.HEALTHY]:
                return True
            elif status in [
                srvstatus.ServiceStatuses.FAILED,
                srvstatus.ServiceStatuses.UNKNOWN,
                srvstatus.ServiceStatuses.FAILED_TIMEOUT_GUESTAGENT
            ]:
                raise TroveError('Database service error, status: %s' % status)

        return False

    def resize_volume(self, new_size):
        LOG.info("Resizing volume for instance %(instance_id)s from "
                 "%(old_size)s GB to %(new_size)s GB.",
                 {'instance_id': self.id, 'old_size': self.volume_size,
                  'new_size': new_size})
        action = ResizeVolumeAction(self, self.volume_size, new_size)
        action.execute()
        LOG.info("Resized volume for instance %s successfully.", self.id)

    def resize_flavor(self, old_flavor, new_flavor):
        LOG.info("Resizing instance %(instance_id)s from flavor "
                 "%(old_flavor)s to %(new_flavor)s.",
                 {'instance_id': self.id, 'old_flavor': old_flavor['id'],
                  'new_flavor': new_flavor['id']})
        action = ResizeAction(self, old_flavor, new_flavor)
        action.execute()
        LOG.info("Resized instance %s successfully.", self.id)

    def migrate(self, host):
        LOG.info("Initiating migration to host %s.", host)
        action = MigrateAction(self, host)
        action.execute()

    def rebuild(self, image_id):
        LOG.info(f"Rebuilding instance {self.id}, new image {image_id}")
        action = RebuildAction(self, image_id)
        action.execute()

    def create_backup(self, backup_info):
        LOG.info("Initiating backup for instance %s, backup_info: %s", self.id,
                 backup_info)
        self.guest.create_backup(backup_info)

    def backup_required_for_replication(self):
        LOG.debug("Check if replication backup is required for instance %s.",
                  self.id)
        return self.guest.backup_required_for_replication()

    def get_replication_snapshot(self, snapshot_info, flavor):
        def _get_replication_snapshot():
            LOG.info("Getting replication snapshot for instance %s.", self.id)
            try:
                rep_source_config = self._render_replica_source_config(flavor)
                result = self.guest.get_replication_snapshot(
                    snapshot_info, rep_source_config.config_contents)

                LOG.info("Finnished getting replication snapshot for "
                         "instance %s", self.id)
                return result
            except Exception as err:
                LOG.error("Failed to get replication snapshot from %s, "
                          "error: %s", self.id, str(err))
                raise

        return run_with_quotas(self.context.project_id, {'backups': 1},
                               _get_replication_snapshot)

    def detach_replica(self, master, for_failover=False):
        LOG.info("Detaching replica %s from %s", self.id, master.id)
        try:
            self.guest.detach_replica(for_failover)
            self.update_db(slave_of_id=None)
            self.slave_list = None
            LOG.info('Replica %s detached', self.id)
        except (GuestError, GuestTimeout):
            LOG.error("Failed to detach replica %s from %s", self.id,
                      master.id)
            raise
        finally:
            if not for_failover:
                self.reset_task_status()

    def attach_replica(self, master, restart=False):
        LOG.info("Attaching replica %s to master %s", self.id, master.id)
        try:
            replica_info = master.guest.get_replica_context()
            flavor = self.nova_client.flavors.get(self.flavor_id)
            slave_config = self._render_replica_config(flavor).config_contents
            self.guest.attach_replica(replica_info, slave_config,
                                      restart=restart)
            self.update_db(slave_of_id=master.id)
            self.slave_list = None
        except (GuestError, GuestTimeout):
            LOG.exception("Failed to attach replica %s.", self.id)
            raise

    def make_read_only(self, read_only):
        LOG.debug("Calling make_read_only on %s", self.id)
        self.guest.make_read_only(read_only)

    def get_public_ip(self):
        """Get public IP (IP, ID) for the trove instance."""
        for item in self.get_visible_ip_addresses():
            if item['type'] == 'public':
                fips = self.neutron_client.list_floatingips(
                    floating_ip_address=item['address'])['floatingips']
                if fips:
                    fip_id = fips[0]['id']
                    return item['address'], fip_id

        return None, None

    def enable_as_master(self):
        LOG.info("Enable %s as master", self.id)

        flavor = self.nova_client.flavors.get(self.flavor_id)
        replica_source_config = self._render_replica_source_config(flavor)
        self.update_db(slave_of_id=None)
        self.slave_list = None

        self.guest.enable_as_master(replica_source_config.config_contents)

    def get_last_txn(self):
        LOG.info("Getting master UUID and last txn for replica %s", self.id)
        return self.guest.get_last_txn()

    def get_latest_txn_id(self):
        LOG.info("Getting latest txn id on %s", self.id)
        return self.guest.get_latest_txn_id()

    def wait_for_txn(self, txn):
        LOG.info("Waiting for txn sync on %s, txn: %s", self.id, txn)
        if txn:
            self.guest.wait_for_txn(txn)

    def cleanup_source_on_replica_detach(self, replica_info):
        LOG.debug("Calling cleanup_source_on_replica_detach on %s", self.id)
        self.guest.cleanup_source_on_replica_detach(replica_info)

    def demote_replication_master(self):
        LOG.info("Demoting old replication master %s", self.id)
        self.guest.demote_replication_master()

    def reboot(self):
        try:
            LOG.debug("Stopping database on instance %s.", self.id)
            try:
                self.guest.stop_db()
            except (exception.GuestError, exception.GuestTimeout) as e:
                # Acceptable to be here if db was already in crashed state
                # Also we check guest state before issuing reboot
                LOG.warning(str(e))

            LOG.info("Rebooting instance %s.", self.id)
            self.server.reboot()

            def update_server_info():
                self.refresh_compute_server_info()
                return self.server_status_matches(['ACTIVE'])

            utils.poll_until(update_server_info, sleep_time=3,
                             time_out=CONF.reboot_time_out, initial_delay=5)

            LOG.info("Starting database on instance %s.", self.id)
            self.guest.restart()

            # Wait for database service up and running
            utils.poll_until(self.is_service_healthy,
                             time_out=CONF.report_interval * 2)

            LOG.info("Rebooted instance %s successfully.", self.id)
        except Exception as e:
            LOG.error("Failed to reboot instance %(id)s: %(e)s",
                      {'id': self.id, 'e': str(e)})
        finally:
            self.reset_task_status()

    def restart(self):
        LOG.info("Initiating datastore restart on instance %s.", self.id)
        try:
            self.guest.restart()
        except GuestError:
            LOG.error("Failed to initiate datastore restart on instance "
                      "%s.", self.id)
        finally:
            self.reset_task_status()

    def guest_log_list(self):
        LOG.info("Retrieving guest log list for instance %s.", self.id)
        try:
            return self.guest.guest_log_list()
        except GuestError:
            LOG.error("Failed to retrieve guest log list for instance "
                      "%s.", self.id)
        finally:
            self.reset_task_status()

    def guest_log_action(self, log_name, enable, disable, publish, discard):
        LOG.info("Processing guest log for instance %s.", self.id)
        try:
            return self.guest.guest_log_action(log_name, enable, disable,
                                               publish, discard)
        except GuestError:
            LOG.error("Failed to process guest log for instance %s.",
                      self.id)
        finally:
            self.reset_task_status()

    def refresh_compute_server_info(self):
        """Refreshes the compute server field."""
        server = self.nova_client.servers.get(self.server.id)
        self.server = server

    def _refresh_datastore_status(self):
        """
        Gets the latest instance service status from datastore and updates
        the reference on this BuiltInstanceTask reference
        """
        self.datastore_status = InstanceServiceStatus.find_by(
            instance_id=self.id)

    def set_datastore_status_to_paused(self):
        """
        Updates the InstanceServiceStatus for this BuiltInstance to PAUSED.
        This does not change the reference for this BuiltInstanceTask
        """
        datastore_status = InstanceServiceStatus.find_by(instance_id=self.id)
        datastore_status.status = srvstatus.ServiceStatuses.PAUSED
        datastore_status.save()

    def upgrade(self, datastore_version):
        LOG.info("Upgrading instance %s to new datastore version %s",
                 self.id, datastore_version)

        self.set_service_status(srvstatus.ServiceStatuses.UPGRADING)

        try:
            upgrade_info = self.guest.pre_upgrade()
            upgrade_info = upgrade_info if upgrade_info else {}
            upgrade_info.update({'datastore_version': datastore_version.name})
            self.guest.upgrade(upgrade_info)

            # Wait for db instance healthy
            LOG.info('Waiting for instance %s to be healthy after upgrading',
                     self.id)
            utils.poll_until(self.is_service_healthy, time_out=600,
                             sleep_time=5)

            self.reset_task_status()
            LOG.info("Finished upgrading instance %s to new datastore "
                     "version %s", self.id, datastore_version)
        except Exception as e:
            LOG.error('Failed to upgrade instance %s, error: %s', self.id, e)
            self.update_db(
                task_status=inst_models.InstanceTasks.BUILDING_ERROR_SERVER)

    def _fix_device_path(self, device):
        """Get correct device path.

        Some cinder drivers appear to return "vdb" instead of "/dev/vdb".
        """
        if device.startswith("/dev"):
            return device
        else:
            return "/dev/%s" % device

    def update_access(self, access):
        LOG.info(f"Updating access for instance {self.id}, access {access}")

        new_is_public = access.get('is_public', False)
        new_allowed_cidrs = access.get('allowed_cidrs', [])
        is_public = (self.access.get('is_public', False) if self.access
                     else None)
        allowed_cidrs = (self.access.get('allowed_cidrs', []) if self.access
                         else None)

        ports = self.neutron_client.list_ports(
            name='trove-%s' % self.id)['ports']

        if is_public != new_is_public:
            for port in ports:
                if port['network_id'] not in CONF.management_networks:
                    LOG.debug(f"Updating port {port['id']}, is_public: "
                              f"{new_is_public}")
                    neutron.ensure_port_access(self.neutron_client, port['id'],
                                               new_is_public, self.tenant_id)

        if CONF.trove_security_groups_support:
            if allowed_cidrs != new_allowed_cidrs:
                name = f"{CONF.trove_security_group_name_prefix}-{self.id}"
                sgs = self.neutron_client.list_security_groups(
                    name=name)['security_groups']

                LOG.debug(f"Updating security group rules for instance "
                          f"{self.id}")
                for sg in sgs:
                    neutron.clear_ingress_security_group_rules(
                        self.neutron_client,
                        sg['id'])

                    if new_allowed_cidrs:
                        tcp_ports = CONF.get(self.datastore.name).tcp_ports
                        udp_ports = CONF.get(self.datastore.name).udp_ports

                        neutron.create_security_group_rule(
                            self.neutron_client, sg['id'], 'tcp', tcp_ports,
                            new_allowed_cidrs)
                        neutron.create_security_group_rule(
                            self.neutron_client, sg['id'], 'udp', udp_ports,
                            new_allowed_cidrs)
        else:
            LOG.warning('Security group not supported.')

        LOG.info(f"Finished to update access for instance {self.id}")
        self.update_db(
            task_status=InstanceTasks.NONE,
            access={'is_public': new_is_public,
                    'allowed_cidrs': new_allowed_cidrs}
        )


class BackupTasks(object):
    @classmethod
    def _parse_manifest(cls, manifest):
        # manifest is in the format 'container/prefix'
        # where prefix can be 'path' or 'lots/of/paths'
        try:
            container_index = manifest.index('/')
            prefix_index = container_index + 1
        except ValueError:
            return None, None
        container = manifest[:container_index]
        prefix = manifest[prefix_index:]
        return container, prefix

    @classmethod
    def delete_files_from_swift(cls, context, container, filename):
        container = container or CONF.backup_swift_container
        client = clients.create_swift_client(context)
        obj = client.head_object(container, filename)
        if 'x-static-large-object' in obj:
            # Static large object
            LOG.debug("Deleting large object file: %(cont)s/%(filename)s",
                      {'cont': container, 'filename': filename})
            client.delete_object(container, filename,
                                 query_string='multipart-manifest=delete')
        else:
            # Single object
            LOG.debug("Deleting object file: %(cont)s/%(filename)s",
                      {'cont': container, 'filename': filename})
            client.delete_object(container, filename)

    @classmethod
    def delete_backup(cls, context, backup_id):
        """Delete backup from swift."""

        def _delete(backup):
            backup.deleted = True
            backup.deleted_at = timeutils.utcnow()
            # Set datastore_version_id to None to remove dependency.
            backup.datastore_version_id = None
            backup.save()

        LOG.info("Deleting backup %s.", backup_id)
        backup = bkup_models.Backup.get_by_id(context, backup_id)
        try:
            filename = backup.filename
            # Do not remove the object if the backup was restored from remote
            # location.
            if filename and backup.state != bkup_models.BackupState.RESTORED:
                BackupTasks.delete_files_from_swift(context,
                                                    backup.container_name,
                                                    filename)
        except ValueError:
            _delete(backup)
        except ClientException as e:
            if e.http_status == 404:
                # Backup already deleted in swift
                _delete(backup)
            else:
                LOG.exception("Error occurred when deleting from swift. "
                              "Details: %s", e)
                backup.state = bkup_models.BackupState.DELETE_FAILED
                backup.save()
                raise TroveError(_("Failed to delete swift object for backup "
                                   "%s.") % backup_id)
        else:
            _delete(backup)
        LOG.info("Deleted backup %s successfully.", backup_id)


class ModuleTasks(object):

    @classmethod
    def reapply_module(cls, context, module_id, md5, include_clustered,
                       batch_size, batch_delay, force):
        """Reapply module."""
        LOG.info("Reapplying module %s.", module_id)

        batch_size = batch_size or CONF.module_reapply_max_batch_size
        batch_delay = batch_delay or CONF.module_reapply_min_batch_delay
        # Don't let non-admin bypass the safeguards
        if not context.is_admin:
            batch_size = min(batch_size, CONF.module_reapply_max_batch_size)
            batch_delay = max(batch_delay, CONF.module_reapply_min_batch_delay)
        modules = module_models.Modules.load_by_ids(context, [module_id])
        current_md5 = modules[0].md5
        LOG.debug("MD5: %(md5)s  Force: %(f)s.", {'md5': md5, 'f': force})

        # Process all the instances
        instance_modules = module_models.InstanceModules.load_all(
            context, module_id=module_id, md5=md5)
        total_count = instance_modules.count()
        reapply_count = 0
        skipped_count = 0
        if instance_modules:
            module_list = module_views.convert_modules_to_list(modules)
            for instance_module in instance_modules:
                instance_id = instance_module.instance_id
                if (instance_module.md5 != current_md5 or force) and (
                    not md5 or md5 == instance_module.md5):
                    instance = BuiltInstanceTasks.load(context, instance_id,
                                                       needs_server=False)
                    if instance and (
                        include_clustered or not instance.cluster_id):
                        try:
                            module_models.Modules.validate(
                                modules, instance.datastore.id,
                                instance.datastore_version.id)
                            client = create_guest_client(context, instance_id)
                            client.module_apply(module_list)
                            Instance.add_instance_modules(
                                context, instance_id, modules)
                            reapply_count += 1
                        except exception.ModuleInvalid as ex:
                            LOG.info("Skipping: %s", ex)
                            skipped_count += 1

                        # Sleep if we've fired off too many in a row.
                        if (batch_size and
                            not reapply_count % batch_size and
                            (reapply_count + skipped_count) < total_count):
                            LOG.debug("Applied module to %(cnt)d of %(total)d "
                                      "instances - sleeping for %(batch)ds",
                                      {'cnt': reapply_count,
                                       'total': total_count,
                                       'batch': batch_delay})
                            time.sleep(batch_delay)
                    else:
                        LOG.debug("Instance '%s' not found or doesn't match "
                                  "criteria, skipping reapply.", instance_id)
                        skipped_count += 1
                else:
                    LOG.debug("Instance '%s' does not match "
                              "criteria, skipping reapply.", instance_id)
                    skipped_count += 1
        LOG.info("Reapplied module to %(num)d instances "
                 "(skipped %(skip)d).",
                 {'num': reapply_count, 'skip': skipped_count})


class ResizeVolumeAction(object):
    """Performs volume resize action."""

    def __init__(self, instance, old_size, new_size):
        self.instance = instance
        self.old_size = int(old_size)
        self.new_size = int(new_size)

    def get_mount_point(self):
        mount_point = CONF.get(
            self.instance.datastore_version.manager).mount_point
        return mount_point

    def get_device_path(self):
        return self.instance.device_path

    def _fail(self, orig_func):
        LOG.error("%(func)s encountered an error when "
                  "attempting to resize the volume for "
                  "instance %(id)s. Setting service "
                  "status to failed.", {'func': orig_func.__name__,
                                        'id': self.instance.id})
        service = InstanceServiceStatus.find_by(instance_id=self.instance.id)
        service.set_status(srvstatus.ServiceStatuses.FAILED)
        service.save()

    def _recover_restart(self, orig_func):
        LOG.exception("%(func)s encountered an error when attempting to "
                      "resize the volume for instance %(id)s. Trying to "
                      "recover by restarting the "
                      "guest.", {'func': orig_func.__name__,
                                 'id': self.instance.id})
        self.instance.restart()

    def _recover_mount_restart(self, orig_func):
        LOG.exception("%(func)s encountered an error when attempting to "
                      "resize the volume for instance %(id)s. Trying to "
                      "recover by mounting the volume and then restarting "
                      "the guest.", {'func': orig_func.__name__,
                                     'id': self.instance.id})
        self._mount_volume()
        self.instance.restart()

    def _recover_full(self, orig_func):
        LOG.error("%(func)s encountered an error when attempting to "
                  "resize the volume for instance %(id)s. Trying to "
                  "recover by attaching and"
                  " mounting the volume and then restarting the "
                  "guest.", {'func': orig_func.__name__,
                             'id': self.instance.id})
        self._attach_volume()
        self._mount_volume()
        self.instance.restart()

    def _stop_db(self):
        LOG.debug("Instance %s calling stop_db.", self.instance.id)
        self.instance.guest.stop_db()

    @try_recover
    def _unmount_volume(self):
        LOG.debug("Unmounting the volume on instance %(id)s", {
            'id': self.instance.id})
        mount_point = self.get_mount_point()
        device_path = self.get_device_path()
        self.instance.guest.unmount_volume(device_path=device_path,
                                           mount_point=mount_point)
        LOG.debug("Successfully unmounted the volume %(vol_id)s for "
                  "instance %(id)s", {'vol_id': self.instance.volume_id,
                                      'id': self.instance.id})

    @try_recover
    def _detach_volume(self):
        LOG.debug("Detach volume %(vol_id)s from instance %(id)s", {
            'vol_id': self.instance.volume_id,
            'id': self.instance.id})
        self.instance.nova_client.volumes.delete_server_volume(
            self.instance.server.id, self.instance.volume_id)

        def volume_available():
            volume = self.instance.volume_client.volumes.get(
                self.instance.volume_id)
            return volume.status == 'available'

        utils.poll_until(volume_available,
                         sleep_time=2,
                         time_out=CONF.volume_time_out)

        LOG.debug("Successfully detached volume %(vol_id)s from instance "
                  "%(id)s", {'vol_id': self.instance.volume_id,
                             'id': self.instance.id})

    @try_recover
    def _attach_volume(self):
        device_path = self.get_device_path()
        LOG.debug("Attach volume %(vol_id)s to instance %(id)s at "
                  "%(dev)s", {'vol_id': self.instance.volume_id,
                              'id': self.instance.id, 'dev': device_path})
        self.instance.nova_client.volumes.create_server_volume(
            self.instance.server.id, self.instance.volume_id, device_path)

        def volume_in_use():
            volume = self.instance.volume_client.volumes.get(
                self.instance.volume_id)
            return volume.status == 'in-use'

        utils.poll_until(volume_in_use,
                         sleep_time=2,
                         time_out=CONF.volume_time_out)

        LOG.debug("Successfully attached volume %(vol_id)s to instance "
                  "%(id)s", {'vol_id': self.instance.volume_id,
                             'id': self.instance.id})

    @try_recover
    def _resize_fs(self, online=False):
        LOG.info(f"Resizing the filesystem for instance {self.instance.id}, "
                 f"online: {online}")
        mount_point = self.get_mount_point()
        device_path = self.get_device_path()
        self.instance.guest.resize_fs(device_path=device_path,
                                      mount_point=mount_point,
                                      online=online)
        LOG.debug(f"Successfully resized volume {self.instance.volume_id} "
                  f"filesystem for instance {self.instance.id}")

    @try_recover
    def _mount_volume(self):
        LOG.debug("Mount the volume on instance %(id)s", {
            'id': self.instance.id})
        mount_point = self.get_mount_point()
        device_path = self.get_device_path()
        self.instance.guest.mount_volume(device_path=device_path,
                                         mount_point=mount_point)
        LOG.debug("Successfully mounted the volume %(vol_id)s on instance "
                  "%(id)s", {'vol_id': self.instance.volume_id,
                             'id': self.instance.id})

    @try_recover
    def _extend(self):
        LOG.info(f"Calling Cinder to extend volume {self.instance.volume_id} "
                 f"for instance {self.instance.id} to size {self.new_size}")
        self.instance.volume_client.volumes.extend(self.instance.volume_id,
                                                   self.new_size)
        LOG.debug("Successfully extended the volume %(vol_id)s for instance "
                  "%(id)s", {'vol_id': self.instance.volume_id,
                             'id': self.instance.id})

    def _verify_extend(self):
        try:
            volume = self.instance.volume_client.volumes.get(
                self.instance.volume_id)
            if not volume:
                msg = f'Failed to get volume {self.instance.volume_id}'
                raise exception.TroveError(msg)

            def volume_is_new_size():
                volume = self.instance.volume_client.volumes.get(
                    self.instance.volume_id)
                return volume.size == self.new_size

            utils.poll_until(volume_is_new_size,
                             sleep_time=5,
                             time_out=CONF.volume_time_out)

            self.instance.update_db(volume_size=self.new_size)
        except PollTimeOut:
            LOG.error("Timeout trying to extend the volume %(vol_id)s "
                      "for instance %(id)s",
                      {'vol_id': self.instance.volume_id,
                       'id': self.instance.id})
            volume = self.instance.volume_client.volumes.get(
                self.instance.volume_id)
            if volume.status == 'extending':
                self._fail(self._verify_extend)
            elif volume.size != self.new_size:
                self.instance.update_db(volume_size=volume.size)
                if not CONF.online_volume_resize:
                    self._recover_full(self._verify_extend)
            raise
        except Exception as e:
            LOG.error("Error encountered trying to verify extend for "
                      "the volume %(vol_id)s for instance %(id)s, "
                      "error: %(error)s",
                      {'vol_id': self.instance.volume_id,
                       'id': self.instance.id,
                       'error': str(e)})
            if not CONF.online_volume_resize:
                self._recover_full(self._verify_extend)
            raise

    def _resize_active_volume(self):
        if CONF.online_volume_resize:
            try:
                self._extend()
            except Exception as e:
                LOG.error(f'Failed to extend volume, error: {str(e)}')

            self._verify_extend()
            self._resize_fs(recover_func=self._fail, online=True)
            return

        self._stop_db()
        self._unmount_volume(recover_func=self._recover_restart)
        self._detach_volume(recover_func=self._recover_mount_restart)
        self._extend(recover_func=self._recover_full)
        self._verify_extend()
        # if anything fails after this point, recovery is futile
        self._attach_volume(recover_func=self._fail)
        self._resize_fs(recover_func=self._fail, online=False)
        self._mount_volume(recover_func=self._fail)
        self.instance.restart()

    def execute(self):
        LOG.debug("%(gt)s: Resizing instance %(id)s volume for server "
                  "%(server_id)s from %(old_volume_size)s to "
                  "%(new_size)r GB", {'gt': greenthread.getcurrent(),
                                      'id': self.instance.id,
                                      'server_id': self.instance.server.id,
                                      'old_volume_size': self.old_size,
                                      'new_size': self.new_size})

        if self.instance.server.status in [InstanceStatus.ACTIVE,
                                           InstanceStatus.HEALTHY]:
            try:
                self._resize_active_volume()
            finally:
                self.instance.reset_task_status()

        else:
            self.instance.reset_task_status()
            msg = (
                "Failed to resize instance %(id)s volume for server "
                "%(server_id)s. The instance must be in state %(state)s "
                "not %(inst_state)s." %
                {
                    'id': self.instance.id,
                    'server_id': self.instance.server.id,
                    'state': [InstanceStatus.ACTIVE, InstanceStatus.HEALTHY],
                    'inst_state': self.instance.server.status
                }
            )
            raise TroveError(msg)


class ResizeActionBase(object):
    """Base class for executing a resize action."""

    def __init__(self, instance):
        """
        Creates a new resize action for a given instance
        :param instance: reference to existing instance that will be resized
        :type instance: trove.taskmanager.models.BuiltInstanceTasks
        """
        self.instance = instance
        self.wait_status = ['VERIFY_RESIZE']
        self.ignore_stop_error = False

    def _assert_guest_is_ok(self):
        # The guest will never set the status to PAUSED.
        self.instance.set_datastore_status_to_paused()
        # Now we wait until it sets it to anything at all,
        # so we know it's alive.
        utils.poll_until(
            self._guest_is_awake,
            sleep_time=3,
            time_out=CONF.resize_time_out)

    def _assert_nova_status_is_ok(self):
        # Make sure Nova thinks things went well.
        if not self.instance.server_status_matches(["VERIFY_RESIZE"]):
            msg = "Migration failed! status=%(act_status)s and " \
                  "not %(exp_status)s" % {
                      "act_status": self.instance.server.status,
                      "exp_status": 'VERIFY_RESIZE'}
            raise TroveError(msg)

    def _assert_nova_action_was_successful(self):
        pass

    def _assert_datastore_is_ok(self):
        self._start_datastore()

    def _assert_processes_are_ok(self):
        """Checks the procs; if anything is wrong, reverts the operation."""
        # Tell the guest to turn back on, and make sure it can start.
        self._assert_guest_is_ok()
        LOG.debug("Nova guest is ok.")
        self._assert_datastore_is_ok()
        LOG.debug("Datastore is ok.")

    def _confirm_nova_action(self):
        LOG.debug("Instance %s calling Compute confirm resize...",
                  self.instance.id)
        self.instance.refresh_compute_server_info()
        if self.instance.server_status_matches(["VERIFY_RESIZE"]):
            self.instance.server.confirm_resize()

    def _datastore_is_online(self):
        self.instance._refresh_datastore_status()
        return self.instance.is_datastore_running

    def _datastore_is_offline(self):
        self.instance._refresh_datastore_status()
        return (self.instance.datastore_status_matches(
            srvstatus.ServiceStatuses.SHUTDOWN))

    def _revert_nova_action(self):
        LOG.debug("Instance %s calling Compute revert resize...",
                  self.instance.id)
        self.instance.server.revert_resize()

    def _record_action_success(self):
        pass

    def execute(self):
        """Initiates the action."""
        try:
            LOG.debug("Instance %s calling stop_db...", self.instance.id)
            self.instance.guest.stop_db(do_not_start_on_reboot=True)
        except Exception as e:
            if self.ignore_stop_error:
                LOG.warning(f"Failed to stop db {self.instance.id}, error: "
                            f"{str(e)}")
            else:
                raise

        try:
            self._perform_nova_action()
        finally:
            if self.instance.db_info.task_status != (
                inst_models.InstanceTasks.NONE):
                self.instance.reset_task_status()

    def _guest_is_awake(self):
        self.instance._refresh_datastore_status()
        return not self.instance.datastore_status_matches(
            srvstatus.ServiceStatuses.PAUSED)

    def _guest_is_healthy(self):
        self.instance._refresh_datastore_status()
        return self.instance.datastore_status_matches(
            srvstatus.ServiceStatuses.HEALTHY)

    def wait_for_healthy(self):
        utils.poll_until(
            self._guest_is_healthy,
            sleep_time=3,
            time_out=CONF.resize_time_out)

    def _perform_nova_action(self):
        """Calls Nova to resize or migrate an instance, and confirms."""
        LOG.debug("Begin resize method _perform_nova_action instance: %s",
                  self.instance.id)
        need_to_revert = False
        try:
            LOG.debug("Initiating nova action")
            self._initiate_nova_action()
            LOG.debug("Waiting for nova action completed")
            self._wait_for_nova_action()
            LOG.debug("Asserting nova status is ok")
            self._assert_nova_status_is_ok()
            need_to_revert = True
            LOG.debug("Asserting nova action success")
            self._assert_nova_action_was_successful()
            LOG.debug("Asserting processes are OK")
            self._assert_processes_are_ok()
            LOG.debug("Confirming nova action")
            self._confirm_nova_action()
        except Exception:
            LOG.exception("Exception during nova action.")
            if need_to_revert:
                LOG.error("Reverting action for instance %s",
                          self.instance.id)
                self._revert_nova_action()
                self._wait_for_revert_nova_action()

            if self.instance.server_status_matches(['ACTIVE']):
                LOG.error("Restarting datastore.")
                self.instance.guest.restart()
            else:
                LOG.error("Cannot restart datastore because "
                          "Nova server status is not ACTIVE")

            LOG.error("Error resizing instance %s.", self.instance.id)
            raise

        self._record_action_success()
        LOG.debug("End resize method _perform_nova_action instance: %s",
                  self.instance.id)

    def _wait_for_nova_action(self):
        LOG.info(f"Waiting for Nova server status changed to "
                 f"{self.wait_status}")

        def update_server_info():
            self.instance.refresh_compute_server_info()
            if self.instance.server.status.upper() == 'ERROR':
                raise TroveError("Nova server is in ERROR status")
            return self.instance.server_status_matches(self.wait_status)

        utils.poll_until(
            update_server_info,
            sleep_time=5,
            time_out=CONF.resize_time_out,
            initial_delay=10)

    def _wait_for_revert_nova_action(self):
        # Wait for the server to return to ACTIVE after revert.
        def update_server_info():
            self.instance.refresh_compute_server_info()
            return self.instance.server_status_matches(['ACTIVE'])

        utils.poll_until(
            update_server_info,
            sleep_time=2,
            time_out=CONF.revert_time_out)


class ResizeAction(ResizeActionBase):
    def __init__(self, instance, old_flavor, new_flavor):
        """
        :type instance: trove.taskmanager.models.BuiltInstanceTasks
        :type old_flavor: dict
        :type new_flavor: dict
        """
        super(ResizeAction, self).__init__(instance)
        self.old_flavor = old_flavor
        self.new_flavor = new_flavor
        self.new_flavor_id = new_flavor['id']

    def _assert_nova_action_was_successful(self):
        # Do check to make sure the status and flavor id are correct.
        if str(self.instance.server.flavor['id']) != str(self.new_flavor_id):
            msg = "Assertion failed! flavor_id=%s and not %s" \
                  % (self.instance.server.flavor['id'], self.new_flavor_id)
            raise TroveError(msg)

    def _initiate_nova_action(self):
        self.instance.server.resize(self.new_flavor_id)

    def _revert_nova_action(self):
        LOG.debug("Instance %s calling Compute revert resize... "
                  "Repairing config.", self.instance.id)
        try:
            config = self.instance._render_config(self.old_flavor)
            config = {'config_contents': config.config_contents}
            self.instance.guest.reset_configuration(config)
        except GuestTimeout:
            LOG.exception("Error sending reset_configuration call.")
        LOG.debug("Reverting resize.")
        super(ResizeAction, self)._revert_nova_action()

    def _record_action_success(self):
        LOG.debug("Updating instance %(id)s to flavor_id %(flavor_id)s.",
                  {'id': self.instance.id, 'flavor_id': self.new_flavor_id})
        self.instance.update_db(flavor_id=self.new_flavor_id,
                                task_status=inst_models.InstanceTasks.NONE)
        update_time = timeutils.isotime(self.instance.updated)
        TroveInstanceModifyFlavor(instance=self.instance,
                                  old_instance_size=self.old_flavor['ram'],
                                  instance_size=self.new_flavor['ram'],
                                  launched_at=update_time,
                                  modify_at=update_time,
                                  server=self.instance.server).notify()

    def _start_datastore(self):
        config = self.instance._render_config(self.new_flavor)
        self.instance.guest.start_db_with_conf_changes(
            config.config_contents,
            self.instance.datastore_version.name)


class MigrateAction(ResizeActionBase):
    def __init__(self, instance, host=None):
        super(MigrateAction, self).__init__(instance)
        self.instance = instance
        self.host = host

    def _assert_nova_action_was_successful(self):
        LOG.debug("Currently no assertions for a Migrate Action")

    def _initiate_nova_action(self):
        LOG.debug("Migrating instance %(instance)s without flavor change ...\n"
                  "Forcing migration to host(%(host)s)",
                  {"instance": self.instance.id,
                   "host": self.host})

        self.instance.server.migrate(force_host=self.host)

    def _record_action_success(self):
        LOG.debug("Successfully finished Migration to "
                  "%(hostname)s: %(id)s",
                  {'hostname': self.instance.hostname,
                   'id': self.instance.id})

    def _start_datastore(self):
        self.instance.guest.restart()


class RebuildAction(ResizeActionBase):
    def __init__(self, instance, image_id):
        super(RebuildAction, self).__init__(instance)
        self.image_id = image_id
        self.ignore_stop_error = True
        self.wait_status = ['ACTIVE']

    def _initiate_nova_action(self):
        files = self.instance.get_injected_files(
            self.instance.datastore.name,
            self.instance.datastore_version.name)

        LOG.debug(f"Rebuilding Nova server {self.instance.server.id}")
        # Before Nova version 2.57, userdata is not supported when doing
        # rebuild, have to use injected files instead.
        self.instance.server.rebuild(
            self.image_id,
            files=files,
        )

    def _assert_nova_status_is_ok(self):
        pass

    def _assert_nova_action_was_successful(self):
        if self.instance.server.image['id'] != self.image_id:
            msg = (f"Assertion failed! The service image ID is "
                   f"{self.instance.server.image['id']} not {self.image_id}")
            raise TroveError(msg)

    def _assert_processes_are_ok(self):
        """Send rebuild async request to the guest and wait."""
        flavor = self.instance.nova_client.flavors.get(self.instance.flavor_id)
        config = self.instance._render_config(flavor)
        config_contents = config.config_contents

        overrides = {}
        if self.instance.configuration:
            overrides = self.instance.configuration. \
                get_configuration_overrides()

        LOG.info(f"Sending rebuild request to the instance {self.instance.id}")
        self.instance.guest.rebuild(
            self.instance.datastore_version.version,
            config_contents=config_contents, config_overrides=overrides)

        LOG.info(f"Waiting for instance {self.instance.id} healthy")
        self._assert_guest_is_ok()
        self.wait_for_healthy()
        LOG.info(f"Finished to rebuild {self.instance.id}")

    def _revert_nova_action(self):
        pass

    def _wait_for_revert_nova_action(self):
        pass

    def _confirm_nova_action(self):
        pass


def load_cluster_tasks(context, cluster_id):
    manager = Cluster.manager_from_cluster_id(context, cluster_id)
    strat = strategy.load_taskmanager_strategy(manager)
    task_manager_cluster_tasks_class = strat.task_manager_cluster_tasks_class
    return ClusterTasks.load(context, cluster_id,
                             task_manager_cluster_tasks_class)

#!/usr/bin/python3
# -*- coding: utf-8 -*-

import json
import cm_client
from cm_client.rest import ApiException
from cm_client import (ApiConfig, ApiConfigList, ApiHost, ApiHostList, ApiCluster, ApiClusterList,
                       ApiServiceList, ApiService, ApiRoleList, ApiRole, ApiRolesToInclude, ApiRestartClusterArgs,
                       ApiRollingRestartClusterArgs, ApiCommand)
from ansible.module_utils.basic import AnsibleModule
from functools import wraps
import time


########################################
ANSIBLE_METADATA = {
    "metadata_version": "1.0",
    "supported_by": "community",
    "status": ["preview"],
    "version": "1.1.0"
}


# Build module with params
def build_module():
    fields = {
        # Connection params
        "cm_login": {"required": True, "type": "str", "no_log": True},
        "cm_password": {"required": True, "type": "str", "no_log": True},
        "cm_host": {"required": True, "type": "str"},
        "cm_port": {"required": False, "type": "str", "default": "7180"},
        "cm_proto": {"required": False, "type": "str", "default": "http"},
        "api_version": {"required": False, "type": "str", "default": "18"},  # 18+
        "config_view": {"required": False, "type": "str"},  # Config size on "info": summary/full
        # Application params
        "cluster": {"required": False, "type": "str"},  # Clusters. If empty - operate with the only cluster or fail.
        "services": {"required": False, "type": "str"},  # Services to operate. If empty - operating with clusters.
        "only_stale_services": {"required": False, "type": "bool", "default": False},
        "redeploy_client_configuration": {"required": False, "type": "bool", "default": False},
        "un_upgraded_only": {"required": False, "type": "bool", "default": False},
        "rolling_restart_roles_type": {
            '''
            all_roles - Only the slave roles of the selected rolling-restartable services.
            slaves_only - Only the non-slave roles of the selected rolling-restartable services.
            non_slaves_only - All roles of the selected rolling-restartable services.
            '''
            "required": False,
            "default": "slaves_only",
            "choices": ["slaves_only", "non_slaves_only", "all_roles"],
            "type": "str",
        },
        "action": {
            '''
            info - Just print cluster/service "config_view"-type info
            stop - Stop cluster/service
            start - Start cluster/service
            restart - Restart cluster/service straight
            rolling-restart - Restart cluster/service rolling
            '''
            "default": "info",
            "choices": ["stop", "start", "restart", "rolling-restart", "info"],
            "type": "str"
        }
    }
    module = AnsibleModule(
        argument_spec=fields,
        mutually_exclusive=[],
        supports_check_mode=True
    )

    return module


########################################
class CM(object):
    def __init__(self, module):
        # Init client params to connect to CM
        self.module = module
        params = self.module.params
        api_url = f"{params['cm_proto']}://{params['cm_host']}:{params['cm_port']}/api/v{params['api_version']}"
        cm_client.configuration.username = params['cm_login']
        cm_client.configuration.password = params['cm_password']
        cm_client.configuration.host = api_url
        api_client = cm_client.ApiClient()
        self.name = params['cm_host']  # Initial "cluster name" is, kinda, it's hostname

        # API clients to make requests through them
        self.cm_resource_api_client = cm_client.ClouderaManagerResourceApi(api_client)
        self.cm_cluster_api_client = cm_client.ClustersResourceApi(api_client)
        self.cm_mgmt_roles_api_client = cm_client.MgmtRolesResourceApi(api_client)
        self.cm_roles_api_client = cm_client.RolesResourceApi(api_client)
        self.cm_hosts_api_client = cm_client.HostsResourceApi(api_client)
        self.cm_service_api_client = cm_client.ServicesResourceApi(api_client)
        self.command_resource_api_client = cm_client.CommandsResourceApi(api_client)

        # Parameters and actions
        self.action = params["action"]
        self.cluster_name = params["cluster"].upper() if params["cluster"] is not None else params["cluster"]
        if params["services"] is not None:
            self.services_names = [service.strip() for service in params["services"].split(",")]
        else:
            self.services_names = list()
        self.config_view = params["config_view"]
        self.only_stale_services = params["only_stale_services"]
        self.redeploy_client_configuration = params["redeploy_client_configuration"]
        self.rolling_restart_roles_type = params["rolling_restart_roles_type"].upper()
        self.un_upgraded_only = params["un_upgraded_only"]

        # By default a module status is "not changed"
        self.changed = False

        # Entities set and update
        self.config = ApiConfigList([ApiConfig()])
        self.read_config()
        self.clusters = ApiClusterList([ApiCluster()])
        self.read_clusters()
        # Cluster name validation
        if self.cluster_name is not None and self.cluster_name not in [cluster.name for cluster in self.clusters.items]:
            self.module.fail_json(msg=f"Cluster {self.cluster_name} is absent on server {self.name}")
        if self.cluster_name is None and len(self.clusters.items) > 1:
            available_clusters = ",".join([cluster.name for cluster in self.clusters.items])
            self.module.fail_json(msg=f"Which cluster should I manage? Available clusters: {available_clusters}")
        self.hosts = ApiHostList([ApiHost()])
        self.read_hosts()
        self.services = ApiServiceList([ApiService()])
        self.read_services()
        self.roles = ApiRoleList([ApiRole])
        self.read_roles()
        self.cluster_stop_command = ApiCommand()
        self.cluster_start_command = ApiCommand()
        self.cluster_restart_command = ApiCommand()
        self.cluster_rolling_restart_command = ApiCommand()

        # Variables
        self.command_wait_interval = 3

        # Debug
        self.debug_command = list()

    # try-except for all the API calls to the CM
    class Decorators(object):
        @classmethod
        def try_cm_api(cls, func):
            @wraps(func)
            def wrapper(*args):
                try:
                    return func(*args)
                except ApiException as e:
                    args[0].module.fail_json(msg=f"Cluster Manager error : {e}")
            return wrapper

    # FUNCTIONS
    # Get ApiCommand by it's id
    @Decorators.try_cm_api
    def _read_command(self, command_id):
        return self.command_resource_api_client.read_command(command_id=command_id)

    # Waits for a command to be done and returns an ApiCommand object.
    @Decorators.try_cm_api
    def _wait_for_command_exec(self, command):
        while self._read_command(int(command.id)).active:
            time.sleep(self.command_wait_interval)
        return self._read_command(int(command.id))

    # Here we go for the cluster config
    @Decorators.try_cm_api
    def read_config(self, config_view="full"):
        self.config = self.cm_resource_api_client.get_config(view=config_view)

    # Get clusters list
    @Decorators.try_cm_api
    def read_clusters(self, config_view="full", cluster_type="any"):
        self.clusters = self.cm_cluster_api_client.read_clusters(cluster_type=cluster_type, view=config_view)

    # Get hosts list
    @Decorators.try_cm_api
    def read_hosts(self, config_view="full"):
        self.hosts = self.cm_hosts_api_client.read_hosts(view=config_view)

    # Get services list for all the clusters
    @Decorators.try_cm_api
    def read_services(self, config_view="full"):
        services_list = list()
        for cluster in self.clusters.items:
            for serv in self.cm_service_api_client.read_services(cluster_name=cluster.name, view=config_view).items:
                services_list.append(serv)
        self.services = ApiServiceList(services_list)

    # Get roles list
    @Decorators.try_cm_api
    def read_roles(self, config_view="full"):
        roles = []
        # Get roles by cluster and service
        for cluster in self.clusters.items:
            for service in self.services.items:
                for role in self.cm_roles_api_client.read_roles(
                        cluster_name=cluster.name,
                        service_name=service.name,
                        filter="",  # Default
                        view=config_view
                        ).items:
                    roles.append(role)
        self.roles = ApiRoleList(roles)

    # Return info according to request parameters
    def meta(self):
        # Clusters and Hosts
        if self.cluster_name is not None:
            # Clusters filter by cluster
            clusters_content = [item.to_dict() for item in self.clusters.items
                                if item.name == self.cluster_name]
            # Hosts filter by cluster
            hosts_content = [item.to_dict() for item in self.hosts.items
                             if item.cluster_ref.cluster_name == self.cluster_name]
        else:
            clusters_content = self.clusters.to_dict().get("items")
            hosts_content = self.hosts.to_dict().get("items")
        # Services and Roles
        if self.services_names:
            # Services filter by cluster
            services_content = []
            for serv in self.services.items:
                if serv.cluster_ref is not None:
                    if serv.cluster_ref.cluster_name in [cluster.get("name") for cluster in clusters_content]\
                            and serv.name in self.services_names:
                        services_content.append(serv.to_dict())
            # Roles filter by service
            roles_content = []
            for role in self.roles.items:
                if role.service_ref.cluster_name in [cluster.get("name") for cluster in clusters_content]\
                        and role.service_ref.service_name in [serv.get("name") for serv in services_content]:
                    roles_content.append(role.to_dict())
                    break
        else:
            services_content = self.services.to_dict().get("items")
            roles_content = self.roles.to_dict().get("items")
        return {
            "cluster_name": self.name,
            "config": self.config.to_dict().get("items"),
            "clusters": clusters_content,
            "hosts": hosts_content,
            "services": services_content,
            "roles": roles_content,
        }

    def __repr__(self):
        return f'CM(name={self.name})'

    def __str__(self):
        return f"name: {self.name}"


########################################
class CMClusterManage(CM):
    # Stop all services in the cluster.
    @CM.Decorators.try_cm_api
    def stop_cluster(self, cluster_name):
        # If all services already stopped - do nothing
        if len([serv for serv in self.services.items if serv.service_state != "STOPPED"]) > 0:
            command = self.cm_cluster_api_client.stop_command(cluster_name=cluster_name)
            self.cluster_stop_command = self._wait_for_command_exec(command)
            self.changed = True
        else:
            self.cluster_stop_command = ApiCommand(
                name="Stop",
                active=False,
                success=True,
                result_message="All services already stopped."
            )

    # Start all services in the cluster.
    @CM.Decorators.try_cm_api
    def start_cluster(self, cluster_name):
        # If all services already started - do nothing
        if len([serv for serv in self.services.items if serv.service_state != "STARTED"]) > 0:
            command = self.cm_cluster_api_client.start_command(cluster_name=cluster_name)
            self.cluster_start_command = self._wait_for_command_exec(command)
            self.changed = True
        else:
            self.cluster_start_command = ApiCommand(
                name="Start",
                active=False,
                success=True,
                result_message="All services already started."
            )

    # Restart cluster
    @CM.Decorators.try_cm_api
    def restart_cluster(self, cluster_name):
        stale_services_count = len([serv for serv in self.services.items if serv.config_staleness_status == "STALE"])
        if self.only_stale_services and stale_services_count == 0:
            self.cluster_restart_command = ApiCommand(
                name="Restart",
                active=False,
                success=True,
                result_message="No stale configurations found."
            )
        else:
            restart_config = ApiRestartClusterArgs(
                restart_only_stale_services=self.only_stale_services,  # Restart services with stale configs only.
                redeploy_client_configuration=self.redeploy_client_configuration,  # Re-deploy client configuration.
                restart_service_names=self.services_names  # List of services to restart.
            )
            self.debug_command = [
                self.only_stale_services,  # Restart services with stale configs only.
                self.redeploy_client_configuration,  # Re-deploy client configuration.
                self.services_names  # List of services to restart.
            ]
            command = self.cm_cluster_api_client.restart_command(cluster_name=cluster_name, body=restart_config)
            self.cluster_restart_command = self._wait_for_command_exec(command)
            self.changed = True

    # Restart cluster in a rolling manner
    # !!!Enterprise license only!!!
    @CM.Decorators.try_cm_api
    def rolling_restart_cluster(self, cluster_name):
        # If all services is stopped - do nothing
        services_to_restart = list()
        if self.services_names:
            for serv in self.services.items:
                if serv.name in self.services_names and serv.service_state == "STARTED":
                    services_to_restart.append(serv)
        else:
            services_to_restart = [serv.name for serv in self.services.items]

        if len(services_to_restart) == 0:
            self.cluster_rolling_restart_command = ApiCommand(
                name="Rolling-restart",
                active=False,
                success=True,
                result_message="No services pointed or they are stopped."
            )
        else:
            restart_config = ApiRollingRestartClusterArgs(
                slave_batch_size=1,  # Default value
                sleep_seconds=0,  # Default value
                slave_fail_count_threshold=0,  # Default value
                # Restart roles with stale configs only.
                stale_configs_only=self.only_stale_services,
                # Restart roles that haven't been upgraded yet.
                un_upgraded_only=self.un_upgraded_only,
                # Re-deploy client configuration.
                redeploy_client_configuration=self.redeploy_client_configuration,
                # Role types to restart. Default is slave roles only.
                roles_to_include=getattr(ApiRolesToInclude(), f"{self.rolling_restart_roles_type}"),
                # List of services to restart.
                restart_service_names=self.services_names or [serv.name for serv in self.services.items]
            )
            command = self.cm_cluster_api_client.rolling_restart(cluster_name=cluster_name, body=restart_config)
            self.cluster_rolling_restart_command = self._wait_for_command_exec(command)
            self.changed = True


def main():
    # Get CM object. All entities configs and initial validations must be prepared inside of it.
    module = build_module()
    cm_instance = CMClusterManage(module)

    # Updates config in the short view. If you want full configs - set the request parameter "config_view" to the "full"
    def update_configs(cm):
        cm.read_config(cm.config_view or "summary")
        cm.read_clusters(cm.config_view or "summary")
        cm.read_hosts(cm.config_view or "summary")
        cm.read_services(cm.config_view or "summary")
        cm.read_roles(cm.config_view or "summary")

    # Just get info
    if cm_instance.action == "info":
        # Just get config and go home
        update_configs(cm_instance)
        cm_instance.module.exit_json(
            changed=cm_instance.changed,
            msg="Cluster information gathered",
            meta=cm_instance.meta(),
        )

    # Stop/start the entire cluster (one for a time)
    elif cm_instance.action in ["start", "stop", "restart", "rolling-restart"]:
        if cm_instance.cluster_name is not None:
            cluster_name = cm_instance.cluster_name  # If the cluster is set by a playbook - operating it
        else:
            cluster_name = cm_instance.clusters.items[0].name  # We can serve only one cluster at a time
        # Execute command
        getattr(cm_instance, f"{cm_instance.action}_cluster".replace("-", "_"))(cluster_name)
        update_configs(cm_instance)
        # Get command object and evaluating it
        executed_command = getattr(cm_instance, f"cluster_{cm_instance.action}_command".replace("-", "_"))
        if executed_command.success:
            cm_instance.module.exit_json(
                changed=cm_instance.changed,
                msg="Changes performed successfully" if cm_instance.changed else "Cluster already in the state",
                meta=cm_instance.meta(),
                command=executed_command.to_dict(),
            )
        else:
            cm_instance.module.fail_json(
                changed=cm_instance.changed,
                msg=f"ERROR! Command '{executed_command.name}' is unsuccessful: '{executed_command.result_message}'",
                meta=cm_instance.meta(),
                command=executed_command.to_dict(),
            )

    else:
        cm_instance.module.fail_json(
            msg=f"WARNING! {cm_instance.action} is not implemented"
        )


if __name__ == "__main__":
    main()

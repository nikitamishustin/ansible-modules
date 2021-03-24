#!/usr/bin/python3
# -*- coding: utf-8 -*-

from cm_client.rest import ApiException
from cm_client import ApiConfig, ApiConfigList
import cm_client
from ansible.module_utils.basic import AnsibleModule
import time
from functools import wraps

ANSIBLE_METADATA = {
    "metadata_version": "1.0",
    "supported_by": "community",
    "status": ["preview"],
    "version": "1.1.0"
}


def build_module():
    fields = {
        "cm_login": {"required": True, "type": "str", "no_log": True},
        "cm_password": {"required": True, "type": "str", "no_log": True},
        "cm_host": {"required": True, "type": "str"},
        "cm_port": {"required": False, "type": "str", "default": "7180"},
        "api_version": {"required": False, "type": "str", "default": "18"},
        "name": {"required": False, "type": "str"},
        "value": {"required": False, "type": "str"},
        "action": {
            "default": "infos",
            "choices": ['set', 'append', 'absent', 'infos'],
            "type": 'str'
        }
    }
    module = AnsibleModule(
        argument_spec=fields,
        mutually_exclusive=[],
        supports_check_mode=True
    )

    return module


class ClusterManager(object):
    def __init__(self, name, api_client, module):
        self.name = name
        self.module = module
        self.cm_resource_api_client = cm_client.ClouderaManagerResourceApi(api_client)
        self.cm_cluster_api_client = cm_client.ClustersResourceApi(api_client)
        self.command_resource_api_client = cm_client.CommandsResourceApi(api_client)
        self.config = self._get_config()
        self.clusters = self._get_clusters()
        self.changed = False
        self.parcels_refresh_command = None
        self.clusters_refresh_commands = None

    # Try-except decorator to log errors on the cluster management API requests.
    class Decorators(object):
        @classmethod
        def try_cm_api(cls, func, *args):
            @wraps(func)
            def wrapper(*args):
                try:
                    return func(*args)
                except ApiException as e:
                    args[0].module.fail_json(msg=f"Cluster Manager error : {e}")
            return wrapper

    @Decorators.try_cm_api
    def _get_config_content(self):
        return self.cm_resource_api_client.get_config().items

    @Decorators.try_cm_api
    def _get_clusters(self):
        return self.cm_cluster_api_client.read_clusters(cluster_type="any", view="summary").items

    @Decorators.try_cm_api
    def _update_config(self):
        self.cm_resource_api_client.update_config(body=self.body)

    @Decorators.try_cm_api
    def _refresh_parcel_repos(self):
        return self.cm_resource_api_client.refresh_parcel_repos()

    @Decorators.try_cm_api
    def _refresh_clusters_config(self):
        return_commands = []
        for cluster in self.clusters:
            return_commands.append(self.cm_cluster_api_client.refresh(cluster.name))
        return(return_commands)

    @Decorators.try_cm_api
    def _read_command(self, id):
        return self.command_resource_api_client.read_command(id)

    def _get_config(self):
        prop_dict = {}
        for prop in self._get_config_content():
            if ',' in prop.value:
                prop_dict[prop.name] = prop.value.split(',')
            else:
                prop_dict[prop.name] = [prop.value]
        return prop_dict

    def set_prop(self, name, state, value=None, override=False):
        value = str(value)
        if state == 'set':
            if self.config.get(name, []) != [value]:
                self.config[name] = [value]
                self.changed = True
        elif state == 'append':
            if value not in self.config.get(name, []):
                self.config.setdefault(name, []).append(value)
                self.changed = True
        elif state == 'absent':
            if value in self.config.get(name, []):
                for num, item in enumerate(self.config.get(name)):
                    if item == value:
                        self.config.get(name).pop(num)
                        self.changed = True
                        break
        self._put_state(name)

    def _put_state(self, new_prop):
        config_body = []
        for name, value in self.config.items():
            prop_dict = ApiConfig(
                name=name,
                value=','.join(value)
            )
            config_body.append(prop_dict)
        self.body = ApiConfigList(config_body)
        self._update_config()
        # Refreshing is not momentary, we need to wait until parcel repo refresh command will be inactive.
        if new_prop.lower() in ('parcel_repo_path', 'remote_parcel_repo_urls'):
            # Refresh parcels repo
            command = self._refresh_parcel_repos()
            while self._read_command(int(command.id)).active:
                time.sleep(3)
            # Adding ApiCommand object to self
            self.parcels_refresh_command = self._read_command(int(command.id))
        else:
            # Refresh all clusters configs
            commands = self._refresh_clusters_config()
            active_commands_count = len(commands)
            while active_commands_count > 0:
                time.sleep(3)
                active_commands_count = 0
                for command in commands:
                    active_commands_count += 1 if self._read_command(int(command.id)).active else 0
            # Adding ApiCommand object to self
            self.clusters_refresh_commands = [self._read_command(int(command.id)) for command in commands]

    def meta(self):
        if self.parcels_refresh_command is not None:
            parcel_refresh_command_meta = self.parcels_refresh_command.to_dict()
        else:
            parcel_refresh_command_meta = dict()

        if self.clusters_refresh_commands is not None:
            clusters_refresh_commands_meta = [command.to_dict() for command in self.clusters_refresh_commands]
        else:
            clusters_refresh_commands_meta = []

        meta = {
            "cluster_name": self.name,
            "config": f"{self.config}",
            "parcel_refresh": parcel_refresh_command_meta,
            "config_refresh": clusters_refresh_commands_meta,
        }
        return meta

    def __repr__(self):
        return f'ClusterManager(name={self.name})'

    def __str__(self):
        return f"name: {self.name}"


def main():
    module = build_module()
    params = module.params

    api_url = f"http://{params['cm_host']}:{params['cm_port']}/api/v{params['api_version']}"
    cm_client.configuration.username = params['cm_login']
    cm_client.configuration.password = params['cm_password']
    cm_client.configuration.host = api_url
    api_client = cm_client.ApiClient()
    cm_config = ClusterManager(name=params["cm_host"], api_client=api_client, module=module)

    if params["action"] == "infos":
        # Just get all info
        module.exit_json(changed=cm_config.changed, msg="Parameters information gathered", meta=cm_config.meta())
    else:
        # Execute command
        try:
            cm_config.set_prop(name=params["name"], state=params["action"], value=params["value"])
        except ApiException as e:
            module.fail_json(msg=f"Cluster error : {e}")
        module.exit_json(changed=cm_config.changed, msg=f'{params["name"]} is in desired state', meta=cm_config.meta())


if __name__ == "__main__":
    main()

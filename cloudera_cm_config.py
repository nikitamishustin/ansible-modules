#!/usr/bin/python3
# -*- coding: utf-8 -*-

from cm_client.rest import ApiException
from cm_client import ApiConfig, ApiConfigList
import cm_client
from ansible.module_utils.basic import AnsibleModule
import time

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


class CM:
    def __init__(self, name, api_client):
        self.name = name
        self.cm_resource_api_client = cm_client.ClouderaManagerResourceApi(api_client)
        self.command_resource_api_client = cm_client.CommandsResourceApi(api_client)
        self.config = self._get_config()
        self.changed = False
        self.parcels_refresh_command = None

    def _get_config(self):
        prop_dict = {}
        for prop in self.cm_resource_api_client.get_config().items:
            if ',' in prop.value:
                prop_dict[prop.name] = prop.value.split(',')
            else:
                prop_dict[prop.name] = [prop.value]
        return prop_dict

    def set_prop(self, name, state, value=None, override=False):
        value = str(value)
        if state == 'set':
            if self.config[name] != [value]:
                self.config[name] = [value]
                self.changed = True
        elif state == 'append':
            if value not in self.config[name]:
                self.config[name].append(value)
                self.changed = True
        elif state == 'absent':
            if value in self.config[name]:
                for num, item in enumerate(self.config[name]):
                    if item == value:
                        self.config[name].pop(num)
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
        body = ApiConfigList(config_body)
        self.cm_resource_api_client.update_config(body=body)
        # Refreshing is not momentary, we need to wait until refresh command will be inactive.
        if 'parcel' in new_prop.lower():
            parcel_refresh_command = self.cm_resource_api_client.refresh_parcel_repos()
            while self.command_resource_api_client.read_command(int(parcel_refresh_command.id)).active:
                time.sleep(3)
            # Adding ApiCommand object to self
            self.parcels_refresh_command = self.command_resource_api_client.read_command(int(parcel_refresh_command.id))

    def meta(self):
        meta = {
            "cluster_name": self.name,
            "config": f"{self.config}",
            "parcel_refresh": self.parcels_refresh_command.to_dict() if self.parcels_refresh_command is not None else dict()
        }
        return meta

    def __repr__(self):
        return f'CM(name={self.name})'

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
    cm_config = CM(name=params["cm_host"], api_client=api_client)

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

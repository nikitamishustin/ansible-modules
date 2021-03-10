#!/usr/bin/python3
# -*- coding: utf-8 -*-

import time
from natsort import natsorted
from cm_client.rest import ApiException
import cm_client
from ansible.module_utils.basic import AnsibleModule

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
        "cluster_name": {"required": True, "type": "str"},
        "api_version": {"required": False, "type": "str", "default": "18"},
        "product": {"required": False, "type": "str"},
        "version": {"required": False, "type": "str"},
        "state": {
            "default": "present",
            "choices": ['present', 'distributed', 'activated', 'absent', 'infos'],
            "type": 'str'
        }
    }

    mutually_exclusive = []
    module = AnsibleModule(
        argument_spec=fields,
        mutually_exclusive=mutually_exclusive,
        supports_check_mode=True
    )

    return module


class Parcel:
    def __init__(self, name, version, cluster_name, api_client, no_wait=False, **kwargs):
        self.name = name
        self.cluster_name = cluster_name
        self.api_client = api_client
        self.parcel_api_client_instance = cm_client.ParcelResourceApi(self.api_client)
        self.parcels_api_client_instance = cm_client.ParcelsResourceApi(self.api_client)
        self.version = self._get_versions(version)
        self.no_wait = no_wait
        self.changed = False
        self.update()

    def update(self):
        self.stage = self._get_stage()
        self.status = self._get_status()

    def _get_status(self):
        return self.parcel_api_client_instance.read_parcel(self.cluster_name, self.name, self.version).state

    def _get_stage(self):
        return self.parcel_api_client_instance.read_parcel(self.cluster_name, self.name, self.version).stage.lower()

    def _get_versions(self, version):
        if version == "latest":
            versions = []
            for parcel in self.parcels_api_client_instance.read_parcels(self.cluster_name).items:
                if parcel.product == self.name:
                    versions.append(parcel.version)
            version = natsorted(versions)[-1]
        return version

    def check_transition(self):
        self.update()
        if self.no_wait:
            return True
        trans_states = ["downloading", "distributing", "undistributing", "activating"]
        while ((self.status.total_count > 0) and
               (self.status.total_count != self.status.count) or
               (self.stage in trans_states)):
            time.sleep(1)
            self.update()

    def downloaded(self):
        if self.stage != "downloaded":
            if self.stage == "activated":
                self.deactivate()
            if self.stage == "distributed":
                self.undistribute()
            self.parcel_api_client_instance.start_download_command(self.cluster_name, self.name, self.version)
            self.check_transition()
            self.changed = True

    def distributed(self):
        if self.stage != "distributed":
            if self.stage != "downloaded":
                if self.stage == "available_remotely":
                    self.downloaded()
                elif self.stage == "activated":
                    self.deactivate()
            self.parcel_api_client_instance.start_distribution_command(self.cluster_name, self.name, self.version)
            self.check_transition()
            self.changed = True

    def activated(self):
        if self.stage != "activated":
            if self.stage != "distributed":
                self.distributed()
            self.parcel_api_client_instance.activate_command(self.cluster_name, self.name, self.version)
            self.check_transition()
            self.changed = True

    def deactivate(self):
        self.parcel_api_client_instance.deactivate_command(self.cluster_name, self.name, self.version)
        self.check_transition()

    def undistribute(self):
        self.parcel_api_client_instance.start_removal_of_distribution_command(
            self.cluster_name, self.name, self.version)
        self.check_transition()

    def available_remotely(self):
        if self.stage != "available_remotely":
            if self.stage != "downloaded":
                if self.stage == "activated":
                    self.deactivate()
                if self.stage == "distributed":
                    self.undistribute()
            self.parcel_api_client_instance.remove_download_command(self.cluster_name, self.name, self.version)
            self.check_transition()
            self.changed = True

    def meta(self):
        meta = {
            "product": self.name,
            "version": self.version,
            "stage": self.stage
        }
        return meta

    def __repr__(self):
        return f'Parcel(name="{self.name}", version="{self.version}", cluster_name="{self.cluster_name}", \
            api_client={self.api_client}, stage="{self.stage}", status={self.status})'

    def __str__(self):
        return f"name: {self.name}, version: {self.version}, state: {self.stage}"


def main():
    module = build_module()
    choice_map = {
        'present': 'downloaded',
        'distributed': 'distributed',
        'activated': 'activated',
        'absent': 'available_remotely',
        'infos': 'infos'
    }
    params = module.params

    api_url = f"http://{params['cm_host']}:{params['cm_port']}/api/v{params['api_version']}"
    cm_client.configuration.username = params['cm_login']
    cm_client.configuration.password = params['cm_password']
    cm_client.configuration.host = api_url
    api_client = cm_client.ApiClient()

    if params["product"] and params["version"]:
        parcel = Parcel(params["product"], params["version"], params["cluster_name"], api_client)
        try:
            getattr(parcel, choice_map.get(params["state"]))()
        except ApiException as e:
            module.fail_json(msg=f"Cluster error : {e}")
        module.exit_json(changed=parcel.changed, msg="Parcel informations gathered", meta=parcel.meta())
    else:
        if params["state"] == "infos":
            api_client_instance = cm_client.ParcelsResourceApi(api_client)
            parcels = []
            try:
                for parcel in api_client_instance.read_parcels(params["cluster_name"]).items:
                    if params["product"]:
                        if params["product"] != parcel.product:
                            continue
                    parcels.append(
                        Parcel(
                            name=parcel.product,
                            version=parcel.version,
                            cluster_name=parcel.cluster_ref.cluster_name,
                            api_client=api_client
                        ).meta()
                    )
            except ApiException as e:
                module.fail_json(msg=f"Cluster error : {e}")
        module.fail_json(changed=False, msg="No valid parameters combination was used, exiting", meta=parcels)


if __name__ == "__main__":
    main()

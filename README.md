# ansible-modules

test-playbook:

```
---
- name: Example to activate the parcel
  hosts: localhost
  become: false
  tasks:
    - name: Add parcel repo
      cloudera_cm_config:
        cm_login: admin
        cm_password: admin
        cm_host: dcher01-vm0
        name: REMOTE_PARCEL_REPO_URLS
        value: "http://fusion-repo.wandisco.com/parcels/"
        action: append

    - name: Activate parcels
      cloudera_parcel:
        cm_login: admin
        cm_password: admin
        cm_host: dcher01-vm0
        cluster_name: DCHER-01
        product: "{{ item }}"
        version: latest
        state: activated
      loop:
        - FUSION
        - FUSION_CLIENT
```
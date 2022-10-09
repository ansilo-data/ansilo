---
sidebar_position: 7
---

# Service Users

In certain scenarios, your build scripts or scheduled jobs will need to authenticate with other nodes.
You can implement service users in this case.

### Step 1: Add the service user to your `ansilo.yml`

The node will execute the referenced shell script to retrieve any password or token used to authenticate
as the service user.

```yaml
auth:
  service_users:
    - username: example_user
      shell: ${dir}/scripts/retrieve-password.sh
```

:::tip
Within the script, you can use `curl` or other cli tools to retrieve passwords or tokens from external systems.
:::

### Step 2a: Specify the service user in the job

To run scheduled jobs using the service user, specify the `service_user` field:

```yaml
jobs:
  - id: my_authenticated_job
    service_user: example_service_user
```

### Step 2b: Specify the service user in your build stage

To run build scripts using the service user, specify the `service_user` field:

```yaml
build:
  stages:
    - sql: ${dir}/sql/*.sql
      service_user: example_service_user
```

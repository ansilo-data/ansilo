---
sidebar_position: 1
---

# Optimisation

You can modify memory capacity and concurrency limits using these parameters in your `ansilo.yml`.

```yaml
resources:
    # Memory capacity in megabytes
    # Default: 512
    memory: 1024
    # Maximum number of concurrent connections to postgres
    # Default: 10
    connections: 15
```

:::info
The memory capacity is distributed across the postgres, JVM and other processes.

Per-connection memory is calculated as `memory / 3 / connections`.
:::

:::tip
For most analytical cases a higher for `memory` to `connections` ratio will be preferable.

For most transactional cases a lower `memory` to `connections` ratio will be preferred.
:::

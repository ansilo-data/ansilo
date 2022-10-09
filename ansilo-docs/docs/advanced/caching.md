---
sidebar_position: 2
---

# Caching

In some situations you may want to reduce the load on you data source.
A good technique is to cache the data in postgres using 
[materalised views](https://www.postgresql.org/docs/current/rules-materializedviews.html)
and refresh the data regularly.

### Step 1: Configure runtime SQL scripts in `ansilo.yml`

We do not want to persist any data when the container is building,
only at runtime when it is running on your infrastructure.

```yaml
        
build:
  stages:
    # Scripts that execute at build-time
    - sql: ${dir}/sql/*.sql
    # Scripts that execute at runtime
    - sql: ${dir}/sql/runtime/*.sql
      type: runtime
```

### Step 2: Create materialized views

In the example we cache customer data using a [materalised view](https://www.postgresql.org/docs/current/rules-materializedviews.html).

```sql
CREATE MATERIALIZED VIEW customers_mat AS
  SELECT * FROM sources.customers;
```

:::caution
This will run every time the container starts up. If you restart or redeploy the container
it will query the data source to retrieve the data.
:::

### Step 3: Schedule a job to refresh the cache

Define a job in `ansilo.yml` which refreshes the materialised view on a schedule.

<div className="combined-code-blocks">

```yaml
jobs:
  - id: refresh_customer_data
    description: Refreshes the cached customer data 
    triggers:
      - cron: "0 0 * * * *"
    sql: |
```

```sql
      REFRESH MATERIALIZED VIEW customers_mat;
```

</div>

See [scheduling jobs](/docs/guides/scheduling-jobs/) for more details.
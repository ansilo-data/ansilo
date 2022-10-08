---
sidebar_position: 4
---

# Scheduling automated jobs

To regularly ingest, move, analyse, transform or cache data a _job_ can be defined to perform this task.
A _job_ is one or many SQL queries which get executed on a regular basis.

### Step 1: configure the job in `ansilo.yml`

<div className="combined-code-blocks">

```yaml
jobs:
  - id: load_into_warehouse
    description: Copies the customers data into the data warehouse
    triggers:
      # Runs the job at the schedule defined by the cron extension
      - cron: "0 0 * * * *"
    sql: |
```

```sql
      BEGIN;

      -- Clear the existing customer data from the warehouse
      DELETE FROM warehouse.customers;
      -- Load the warehouse table from the source table
      INSERT INTO warehouse.customers
        SELECT * FROM source.customers;

      COMMIT;
```

</div>


### Cron scheduling

The scheduling format is as follows:

```
sec   min   hour   day of month   month   day of week   year
*     *     *      *              *       *             *
```

:::caution
Time is specified for UTC and not your local timezone. Note that the year may be omitted.
:::


### Authenticated jobs

Some jobs require authentication in order to access the data they need.
When required, specify the service user which the job executes as.

```yaml
jobs:
  - id: my_authenticated_job
    service_user: example_service_user
```

See [service users](/docs/advanced/service-users) for how to define service users.

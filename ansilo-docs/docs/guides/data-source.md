---
sidebar_position: 1
---

# Connecting to your data source

Data sources are configured in the `sources` section in the `ansilo.yml` configuration file.

### Step 1: review the support matrix

Check that your data store is supported on the [support matrix](/connectors/overview/).
Each connector page has examples of the configuration options specific to that data store.

### Step 2: Add the source to `ansilo.yml`

```yaml
# In this example, we are using an Oracle database but you can use any 
# from the supported connectors.
sources:
  - id: oracle
    type: jdbc.oracle
    options:
      jdbc_url: jdbc:oracle:thin:@my.oracle.host/db
      properties:
        oracle.jdbc.user: oracleuser
        oracle.jdbc.password: oraclepass
```

See [connectors](/connectors/overview/) for all supported data sources.

### Step 3: Validate the connection

In development mode, the instance will restart automatically and try to connect to the data source.
If the connection fails, the error will be logged to stdout/stderr.

### Step 4: Import foreign schemas

Once connected, the schemas from the data store can be imported using [`IMPORT FOREIGN SCHEMA`](https://www.postgresql.org/current/sql-importforeignschema.html).

```sql
-- Create a schema for our data source
CREATE SCHEMA sources;

-- Import foreign tables into the sources schema
IMPORT FOREIGN SCHEMA "ORACLEUSER.%" 
FROM SERVER oracle INTO sources;
```

:::tip
The foreign server will be created automatically using the `sources.*.id` value from `ansilo.yml`.
In this example a sever is created with the name `oracle`.
You do not need to manually issue the `CREATE SERVER` command.
:::
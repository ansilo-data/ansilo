---
sidebar_position: 2
---

import Diagram from './diagrams/configuration.svg'

# Configuration

Every node is defined by its configuration, the configuration files can be considered
the _source code_ when working with Ansilo.

<center>
    <Diagram width="70%" height="auto" className="auto-invert" />
</center>

### Configuration root

The entrypoint for configuration is the `ansilo.yml` file.
A typical example looks like the following:

```yaml
name: Customers

networking:
  port: 443

auth:
  users:
    - username: demouser
      password: mysupersecret!

sources:
  - id: mysql
    type: jdbc.mysql
    options:
      jdbc_url: jdbc:mysql://my-customers-data-store:3306/db
      properties:
        user: ${env:MYSQL_USERNAME}
        password: ${env:MYSQL_PASSWORD}

build:
  stages:
    - sql: ${dir}/sql/*.sql
```

:::info
You can import external values or secrets into your configuration
using [directives](/fundamentals/configuration/#directives).
:::

### Sections

| Section      | Purpose                                                |
| ------------ | ------------------------------------------------------ |
| `networking` | The listening port and interfaces                      |
| `auth`       | How to authenticate users, service users and tokens    |
| `sources`    | Data sources to be interfaced with                     |
| `build`      | SQL scripts used to initialise the PostgreSQL database |
| `jobs`       | Queries to execute on a schedule                       |
| `resources`  | Memory and concurrency limits                          |

### Directives

Directives enable you to import configuration values from external sources.

| Directive                       | Replacement                                                                                                           |
| ------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `${env:ENV_VAR:default}`        | Environment variable `ENV_VAR` or `default` if the variable is not set                                                |
| `${dir}`                        | The directory of the configuration file                                                                               |
| `${arg:ARG_NAME}`               | The value passed to the CLI argument `-D ARG_NAME=value` when running ansilo                                          |
| `${embed:example.yml}`          | Yaml from the file `example.yml`. Useful for configuration splitting.                                                 |
| `${fetch:scheme://uri}`         | Response from downloading `scheme://uri`. Supported schemes are `http`, `https`, `file` and `sh` (shell)              |
| `${vault:mnt:/secret/path:key}` | Retrieves a secret from [HashiCorp Vault](https://www.vaultproject.io/). See [vault integration](/advanced/secrets/). |

### Postgres Configuration

In the prior example we defined that our postgres build should run all sql files matching the relative path `sql/*.sql`.
Each sql script defined in the `build` section will be executed consecutively.

Following on from the prior example, we could configure our node to expose a data schema from mysql:

```sql
-- Create a schema for our data source
CREATE SCHEMA sources;

-- Import tables from mysql
IMPORT FOREIGN SCHEMA "db.%"
FROM SERVER mysql INTO sources;

-- Expose data schema
CREATE VIEW public.customers AS
    SELECT
        id,
        first_name,
        last_name,
        email,
        gender,
        country
     FROM sources.customers;

-- Grant access to demouser
GRANT SELECT ON public.customers TO demouser;
```

For more detailed examples check out the [development guides](/category/development-guides/).

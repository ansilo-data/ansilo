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
        user: ansilouser
        password: mysecretpass

build:
  stages:
    - sql: ${dir}/sql/*.sql
```

:::caution
The above example hard-codes sensitive passwords to provide a minimal example.
It is recommended to store any sensitive values outside of the configuration and import
them using the directives below.
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

See [configuration reference](/docs/resources/configuration-reference/) for all available options.

### Directives

Directives enable you to import configuration values from external sources.

| Directive                       | Replacement                                                                                                                                                                      |
| ------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `${env:ENV_VAR:default}`        | Environment variable `ENV_VAR` or `default` if the variable is not set                                                                                                           |
| `${dir}`                        | The directory of the configuration file                                                                                                                                          |
| `${arg:ARG_NAME}`               | The value passed to the CLI argument `-D ARG_NAME=value` when running ansilo                                                                                                     |
| `${embed:example.yml}`          | Yaml from the file `example.yml`. Useful for configuration splitting.                                                                                                            |
| `${fetch:scheme://uri}`         | Response from downloading `scheme://uri`. Supported schemes are `http`, `https`, `file` and `sh` (shell)                                                                               |
| `${vault:mnt:/secret/path:key}` | Connects to [HashiCorp Vault](https://www.vaultproject.io/) the secret from mount `mnt` at path `/secret/path` with key `key`. See [vault integration](/docs/advanced/secrets/). |

### Postgres Configuration

In the prior example we defined that our postgres build should run all sql files matching the relative path `sql/*.sql`.
Each sql script defined in the `build` section will be executed consecutively.

Following on from the prior example, we could configure our node to expose a data schema from mysql:

```sql
-- Create schema for internal tables
CREATE SCHEMA private;

-- Import tables from mysql
IMPORT FOREIGN SCHEMA "db.%"
FROM SERVER mysql INTO private;

-- Expose data schema
CREATE VIEW public.customers AS
    SELECT
        id,
        first_name,
        last_name,
        email,
        gender,
        country
     FROM private.customers;

-- Grant access to demouser
GRANT SELECT ON public.customers TO demouser;
```

For more detailed examples check out the [development guides](/docs/category/development-guides/).

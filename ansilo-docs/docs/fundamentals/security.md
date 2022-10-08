---
sidebar_position: 3
---

import LightDiagram from './diagrams/auth-light.svg'
import DarkDiagram from './diagrams/auth-dark.svg'

# Security

In Ansilo, defining access rules is broken into two distinct steps: authentication and authorisation.

<center>
    <DarkDiagram width="100%" height="auto" className="dark-only" />
    <LightDiagram width="100%" height="auto" className="light-only" />
</center>

### Authentication

The proxy is responsible for authenticating the identity of incoming requests.
This applies to both PostgreSQL and HTTP connections.
How the proxy performs authentication is defined in the `auth` section in the `ansilo.yml` configuration.

### Password Authentication

In this example we authenticate the user with a simple username and password:

```yaml
auth:
  users:
    - username: exampleuser
      password: mysupersecret!
```

In this case a user will be able connect using postgresql:

```bash
PGPASSWORD="mysupersecret!" psql -h my.ansilo.host -U exampleuser
```

Or using HTTP Basic authentication:

```bash
curl --user 'exampleuser:mysupersecret!' https://my.ansilo.host
```

:::caution
The above example hard-codes sensitive passwords to provide a minimal example.
It is recommended to store any sensitive values outside of the configuration and import
them using [directives](/docs/fundamentals/configuration/#directives).
:::

### JWT Authentication

In order to authenticate using [Json Web Tokens](https://jwt.io) the proxy must know where to find the signing keys.
This is done by specifying a _authentication provider_ of type `jwt`.

```yaml
# Retrieve the signing keys from a JWKS
auth:
  providers:
    - id: my-jwt-provider
      type: jwt
      jwk: https://my.idp.host/.well-known/jwks.json

# Or retrieve them from a local PEM-encoded public key
auth:
  providers:
    - id: my-jwt-provider
      type: jwt
      # In this example this is an RSA key but ec_public_key and ed_public_key are also supported
      rsa_public_key: file://${dir}/keys/my-rsa-public.key
```

Secondly, define the username to authenticate using JWTs:

```yaml
auth:
  users:
    - username: jwt
      provider: my-jwt-provider
```

Then will be able connect using postgresql:

```bash
PGPASSWORD="$JWT_TOKEN" psql -h my.ansilo.host -U token
```

Or using HTTP Basic authentication:

```bash
curl --user "jwt:$JWT_TOKEN" https://my.ansilo.host
```

:::tip
It is recommended to use the same username for JWT authentication across all nodes.
This makes it easier to authenticate across multiple nodes in a Data Mesh.
As a convention, when using JWT authentication specify the username as `jwt`.
:::

### Authorisation

Defining who has access to what is performed using the [PostgreSQL privileges system](https://www.postgresql.org/docs/current/ddl-priv.html).
This is configured by adding `GRANT` and `REVOKE` statements in your [SQL build scripts](/docs/fundamentals/configuration/#postgres-configuration).
 
:::tip
If you are not familiar with authorization in PostgreSQL, it is recommended to read through 
[PostgreSQL privileges system](https://www.postgresql.org/docs/current/ddl-priv.html) documentation first.
:::

### Granting access to users

We can grant access to data with postgres using the `GRANT` statement.

:::info
Users defined in the `auth` section of your `ansilo.yml` file will be created automatically.
You do not need to issue `CREATE USER` statements manually.
:::

```sql
# Grant read access on the customers view to exampleuser
GRANT SELECT ON TABLE customers TO exampleuser;

# Grant write access on the customers view to exampleuser
GRANT INSERT, UPDATE, DELETE ON TABLE customers TO exampleuser;

# Grant read access to all public tables/views
GRANT SELECT ON ALL TABLES IN SCHEMA public TO exampleuser;
```

See [GRANT documentation](https://www.postgresql.org/docs/current/sql-grant.html) for all options.

### Granting access using JWT claims

It is slightly more challenging to define access rules based when working with JWTs.
In this cause we use the `auth_context()` function which returns a `jsonb` object containing the decoded token.

#### Using foreign table callbacks

All imported foreign tables provide options to run user-defined callbacks before reading or writing to that table.

```sql
-- Import the customers table
IMPORT FOREIGN SCHEMA "db.customers"
FROM SERVER oracle INTO public;

-- Grant base query access
GRANT SELECT, INSERT, UPDATE, DELETE ON customers TO token;

-- Grant SELECT to "read" scope
ALTER TABLE customers OPTIONS (ADD before_select 'check_read_scope');

CREATE FUNCTION check_read_scope() RETURNS VOID
    RETURN STRICT('read' = ANY(string_to_array(auth_context()->'claims'->'scope'->>0, ' '), 'read scope is required'));

-- Grant ALL to "write" scope
ALTER TABLE customers OPTIONS (ADD before_modify 'check_write_scope');

CREATE FUNCTION check_write_scope() RETURNS VOID
    RETURN STRICT('write' = ANY(string_to_array(auth_context()->'claims'->'scope'->>0, ' ')), 'write scope is required');
```

:::caution
All authorisation checks defined in callbacks must be wrapped in the `STRICT` function, like in the example above.
This function will trigger an error if the check fails which prevents the query from executing.
:::

       
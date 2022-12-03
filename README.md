![Ansilo](https://uploads-ssl.webflow.com/634643648780f64459633a43/638ace9b90ec6df05064bc70_logo-with-text-sm.png)

Ansilo is a stateless postgres container with JWT auth and an universal foreign data wrapper, making it simple to build interopable data products on top of your existing databases.

The vision for Ansilo is to massively reduce the burden of having data spread across multiple databases or vendors by leveraging postgres and [SQL/MED](https://en.wikipedia.org/wiki/SQL/MED) to provide a simple and standards-compliant interface to query, copy or move data across disparate systems.  

## Get started

Check out [our getting started guide](https://docs.ansilo.io/getting-started/access/). 

## About

Ansilo is designed to help you expose analytical data from your applications and make it easy for others to consume. It does this by exposing postgres views  into your underlying database. 

It enables efficient query execution by transpiling postgres-style SQL queries into the equivalent queries to be run on the underlying platform. Yet still supporting powerful postgres-specific SQL by running the necessary parts locally.

![Ansilo High-Level Architecture](https://uploads-ssl.webflow.com/634643648780f64459633a43/638adc4465a7e42c08632d36_Ansilo%20-%20Docs%20-%20System%20Architecture.svg#gh-light-mode-only)
![Ansilo High-Level Architecture](https://uploads-ssl.webflow.com/634643648780f64459633a43/638adc8377d1c713dc90412f_Ansilo%20-%20Docs%20-%20System%20Architecture-dark.svg#gh-dark-mode-only)

## Just show me how

#### 1. Define your `ansilo.yml`:

```yml
name: Customers

networking:
  port: 65432

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

#### 2. Configure your postgres:

```sql
-- Create a schema for our data source
CREATE SCHEMA sources;

-- Import customers table from mysql
IMPORT FOREIGN SCHEMA "db.customers" 
FROM SERVER mysql INTO sources;

-- Expose data products
CREATE VIEW public.customers AS
    SELECT 
        id,
        first_name,
        last_name,
        email,
        gender,
        country
     FROM sources.customers;

-- Grant access the view
GRANT SELECT ON public.customers TO demouser;

-- Document the schema
COMMENT ON VIEW public.customers IS 'The customers of our organisation';
COMMENT ON COLUMN public.customers.id IS 'UUIDv4 identifier';
```

#### 3. You can now query your data

Use any postgres-compatible driver from any language, tool or `psql` to query your data.
Ansilo also exposes a web interface which can be used to execute basic queries without install additional software.

## Documentation

Check out [our documentation](https://docs.ansilo.io).

## License

Ansilo is source-available and released under the [BSL 1.1](./LICENSE) license.


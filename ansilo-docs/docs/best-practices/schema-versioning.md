---
sidebar_position: 2
---

# Schema versioning

It is recommended to implement a versioning scheme when publishing data products.
This will enable you to make future enhancements in a backwards-compatible way.

:::tip
The principles of versioning your data product mirror those of [REST API versioning](https://restfulapi.net/versioning/).

You may want to consider [semantic versioning](https://semver.org/) as your default versioning scheme.
:::

### Defining versioned schemas

In this example we import the `customers` table from a MySQL database and expose multiple versions of our data products.

```sql
-- Create a schema for our data source
CREATE SCHEMA sources;

-- Import customers table from mysql
IMPORT FOREIGN SCHEMA "db.customers" 
FROM SERVER mysql INTO sources;

-- Create v1 of our customers data product
CREATE VIEW customers$v1 AS
    SELECT 
        id,
        first_name,
        last_name,
        email,
        gender,
        country
     FROM sources.customers;

-- We release a new version to add the `city` column to our schema
CREATE VIEW customers$v2 AS
    SELECT 
        id,
        first_name,
        last_name,
        email,
        gender,
        country,
        city
     FROM sources.customers;


-- Grant access the views
GRANT SELECT ON customers$v1, customers$v2 TO exampleuser;
```

:::info
By convention we use the `$` symbol to separate the version identifier from the entity identifier.
In the above example, the entity is `customers` and the versions are `v1` and `v2`.

The data catalog will automatically group entities that have the same prefix before the `$` symbol.
:::
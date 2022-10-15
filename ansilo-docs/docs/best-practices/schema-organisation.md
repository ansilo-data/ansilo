---
sidebar_position: 1
---

# Schema organisation

It is important to organise your postgres database objects in within appropriate schemas.
Doing so makes it easier for other teams to interact with your node and it will be easier to maintain moving forwards.

### The `public` schema

Every node will have a `public` schema. This is a [postgres default](https://www.postgresql.org/current/ddl-schemas.html#DDL-SCHEMAS-PUBLIC)
and cannot be changed.

:::info
The `public` schema has a special meaning within an Ansilo node. All tables and views defined in the public
schema will automatically be published in the node's data catalog through the query workbench. 

It is important that only tables and views that are intended to be public are created in the `public` schema.
:::

:::tip
In Data Mesh, the `public` schema can be thought of as your Data Product.
:::

### Creating schemas for non-public objects

To have tables or views which are not published through the data catalog you must create a new schema for them.

```sql
-- Create a schema for our sources
CREATE SCHEMA sources;

-- Import customers table from mysql
IMPORT FOREIGN SCHEMA "db.customers" 
FROM SERVER mysql INTO sources;
```

See [publishing you data product](/guides/data-product-expose/) for a complete example.
---
sidebar_position: 2
---

# Publish your data product

To make data schemas available for others to use they must be exposed through
views or tables within the `public` schema.

:::info
The data catalog, displayed within the query workbench, automatically shows tables and views defined in the
`public` schema. If you do not want to expose a table/view in the data catalog, create it a different schema. 
See [schema organisation](/best-practices/schema-organisation/) for more details.
:::

### Example

In this example we expose a the `customers` table from a `mysql`.

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
GRANT SELECT ON public.customers TO exampleuser;

-- Document the schema
COMMENT ON VIEW public.customers IS 'The customers of our organisation';
COMMENT ON COLUMN public.customers.id IS 'UUIDv4 identifier';
```

By exposing the data through a view we have flexibility to implement any data normalisation,
formatting or transformations as required. 

:::tip
It is recommended to implement a versioning scheme on your public data products
to enable backwards compatibility when the underlying data changes.
See [schema versioning](/best-practices/schema-versioning) for more details. 
:::

:::tip
This example exposes the data from mysql in realtime. This means that every query to the view
will execute a query against mysql. In some cases it is preferable to cache data.
See [caching](/advanced/caching/) for more details.
:::

:::tip
You can also create writable data products using [updatable views](https://www.postgresql.org/current/sql-createview.html#SQL-CREATEVIEW-UPDATABLE-VIEWS).
:::
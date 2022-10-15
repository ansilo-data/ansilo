---
sidebar_position: 5
---

# Testing

In order to test the implementation of an Ansilo node, you can connect to the node using
any postgres compatible tools such as `psql`, [pgAdmin](https://www.pgadmin.org/) or even
using the built-in query workbench.

It is recommended that QA is performed by defining the test-cases as SQL scripts.
You may use [PL/pgSQL](https://www.postgresql.org/current/plpgsql.html) as the procedural language in
which to specify the scenarios.

Example of a SQL test script:

```sql
DO $$BEGIN
    -- Assert that the customers table can be accessed
    ASSERT (SELECT COUNT(*) FROM customers) > 0;

    
    -- Assert that insert into customers with NULL id is disallowed
    BEGIN
        INSERT INTO customers (id, name) VALUES (NULL, 'Charlie');
        RAISE EXCEPTION 'Customers insert with null id check failed'; 
    EXCEPTION WHEN raise_exception THEN
        RAISE; -- Check failed
    EXCEPTION WHEN OTHERS THEN
        NULL; -- Check passed
    END;
END$$;
```

:::info
An upcoming feature will include the ability to run SQL-based tests natively.
If you would like us to prioritise this feature contact us at [contact@ansilo.io](mailto:contact@ansilo.io).
:::
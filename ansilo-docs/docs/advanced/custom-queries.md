---
sidebar_position: 6
---

# Custom Queries

It can be useful to issue custom queries to your data store.
Often to access vendor-specific functionality or issue a query that cannot be pushed down through foreign tables.

This is enabled by the following postgres functions:

| Function signature                                                                        | Description                                                                          |
| ----------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| `remote_query(data_source: text, query: text, params: variadic any) RETURNS setof record` | Executes the supplied query on the data store, returning the result set.             |
| `remote_execute(data_source: text, query: text, params: variadic any) RETURNS bigint`     | Executes the supplied query on the data store, returning the number of affected rows |

:::danger
These functions allow users to **issue any query** on the remote data store, regardless of the `GRANT` or authorisation
rules defined in Ansilo. 

By default only the build user has access to these functions. You must explicitly issue `GRANT EXECUTE ON FUNCTION`
to any other users.
:::

### Executing a custom `SELECT`

You can run a custom `SELECT` query and return the result set.

```sql
SELECT *
FROM remote_query(
    -- The data source 
    'mysql',
    -- The query to execute
    $$ SELECT id, name FROM customers WHERE id > ? $$,
    -- Query parameters (if any)
    5
) AS
     -- Define the types of the returning columns
    results(id INT, name TEXT)
```

:::tip
As per the above example, you can use [dollar-qouted strings](https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-DOLLAR-QUOTING)
to define the custom query.
:::

### Executing a custom `INSERT`/`UPDATE`/`DELETE`

You can also issue other DML queries using `remote_execute`.

```sql
SELECT remote_execute('mysql', 'UPDATE customers SET updated = NOW()')
```

:::tip
You can allow users to only issue specific custom queries by wrapping them in a function:

```sql
-- Define a wrapper for your custom query
CREATE FUNCTION example_custom_query() RETURNS INT SECURITY DEFINER
    RETURN remote_execute('data_source', 'MY QUERY');

-- Grant the function to your user
GRANT EXECUTE ON FUNCTION example_custom_query TO example_user;
```
:::
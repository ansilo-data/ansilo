---
sidebar_position: 7
---

# Teradata

Connect to [Teradata](https://www.teradata.com/) using the JDBC driver.

### Configuration

```yaml
sources:
  - id: example
    type: jdbc.teradata
    options:
      jdbc_url: jdbc:teradata://my.teradata.host/DBS_PORT=1025,USER=example_user,PASSWORD=example_password,CHARSET=UTF16
      startup: ["SET SESSION CHARACTER SET UNICODE PASS THROUGH ON;"]
```

:::info
In order to enable unicode support it is recommended to set `CHARSET=UTF16` and include 
`startup: ["SET SESSION CHARACTER SET UNICODE PASS THROUGH ON;"]` as per the above example.
:::

### Supported options

See the [JDBC driver reference](https://teradata-docs.s3.amazonaws.com/doc/connectivity/jdbc/reference/current/frameset.html) for supported options.

### Importing schemas

You can import foreign schemas using the `%` as a wildcard or specify a table explicitly.

```sql
-- Import all tables/views from the `ExampleDB` database
IMPORT FOREIGN SCHEMA "ExampleDB.%"
FROM SERVER example INTO sources;

-- Import just the customers table/view
IMPORT FOREIGN SCHEMA "ExampleDB.customers"
FROM SERVER example INTO sources;
```

### SQL support

| Feature                     | Supported | Notes |
| --------------------------- | --------- | ----- |
| `SELECT`                    | ✅        |       |
| `INSERT`                    | ✅        |       |
| Bulk `INSERT`               | ✅        |       |
| `UPDATE`                    | ✅        |       |
| `DELETE`                    | ✅        |       |
| `WHERE` pushdown            | ✅        |       |
| `JOIN` pushdown             | ✅        |       |
| `GROUP BY` pushdown         | ✅        |       |
| `ORDER BY` pushdown         | ✅        |       |
| `LIMIT` / `OFFSET` pushdown | ✅        |       |

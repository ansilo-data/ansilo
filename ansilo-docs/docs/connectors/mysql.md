---
sidebar_position: 3
---

# MySQL

Connect to [MySQL](https://www.mysql.com) using the JDBC driver.

### Configuration

```yaml
sources:
  - id: example
    type: jdbc.mysql
    options:
      jdbc_url: jdbc:mysql://my.mysql.host:3306
      properties:
        user: example_user
        password: example_pass
```

### Supported options

See the [JDBC driver reference](https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-configuration-properties.html) for supported options.

### Importing schemas

You can import foreign schemas using the `%` as a wildcard or specify a table explicitly.

```sql
-- Import all tables/views from the `example` schema
IMPORT FOREIGN SCHEMA "example.%"
FROM SERVER example INTO sources;

-- Import just the customers table/view
IMPORT FOREIGN SCHEMA "example.customers"
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


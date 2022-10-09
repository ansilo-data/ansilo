---
sidebar_position: 5
---

# SQL Server

Connect to [SQL Server](https://www.microsoft.com/en-au/sql-server/sql-server-2019) using the JDBC driver.

### Configuration

```yaml
sources:
  - id: example
    type: jdbc.mssql
    options:
      jdbc_url: jdbc:sqlserver://my.sqlserver.host:1433;database=example_db;user=example_user;password=example_password;loginTimeout=60
```

### Supported options

See the [JDBC driver reference](https://learn.microsoft.com/en-us/sql/connect/jdbc/setting-the-connection-properties?view=sql-server-ver16) for supported options.

### Importing schemas

You can import foreign schemas using the `%` as a wildcard or specify a table explicitly.

```sql
-- Import all tables/views from the `dbo` schema
IMPORT FOREIGN SCHEMA "dbo.%"
FROM SERVER example INTO sources;

-- Import just the customers table/view
IMPORT FOREIGN SCHEMA "dbo.customers"
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


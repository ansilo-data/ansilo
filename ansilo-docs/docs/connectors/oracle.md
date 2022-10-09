---
sidebar_position: 4
---

# Oracle

Connect to [Oracle databases](https://www.oracle.com/database/) using the JDBC driver.

### Configuration

```yaml
sources:
  - id: example
    type: jdbc.oracle
    options:
      jdbc_url: jdbc:oracle:thin:@my.oracle.host/db
      properties:
        oracle.jdbc.user: oracleuser
        oracle.jdbc.password: oraclepass
```

### Supported options

See the [Oracle JDBC driver reference](https://docs.oracle.com/en/database/oracle/oracle-database/21/jajdb/oracle/jdbc/OracleDriver.html) for supported options.

### Importing schemas

You can import foreign schemas using the `%` as a wildcard or specify a table explicitly.

```sql
-- Import all tables/views from the `ORACLEUSER` schema
IMPORT FOREIGN SCHEMA "ORACLEUSER.%"
FROM SERVER example INTO sources;

-- Import just the customers table/view
IMPORT FOREIGN SCHEMA "ORACLEUSER.customers"
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

---
sidebar_position: 6
---

# SQLite

Connect to [SQLite](https://sqlite.org) using the native driver.

### Configuration

```yaml
sources:
  - id: example
    type: native.sqlite
    options:
      path: /path/to/my/sqlite.db
```

### Supported options

The path can either be file path on disk or `:memory:` for an in-memory database.

### Importing schemas

You can import foreign schemas using the `%` as a wildcard or specify a table explicitly.

```sql
-- Import all tables/views
IMPORT FOREIGN SCHEMA "%"
FROM SERVER example INTO sources;

-- Import just the customers table/view
IMPORT FOREIGN SCHEMA "customers"
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


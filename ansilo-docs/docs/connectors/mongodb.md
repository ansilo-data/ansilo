---
sidebar_position: 8
---

# MongoDB

Connect to [MongoDB](https://www.mongodb.com/) using the native driver.

### Configuration

```yaml
sources:
  - id: example
    type: native.mongodb
    options:
      url: mongodb://example_user:example_pass@my.mongo.host:27017/db
      # Disable transactions in the case of standalone deployments
      disable_transactions: false
```

### Supported options

See the [MongoDB docs](https://www.mongodb.com/docs/manual/reference/connection-string/) for supported options.

### Importing schemas

You can import foreign schemas using the `*` as a wildcard or specify a table explicitly.

```sql
-- Import all tables/views from the `example` database
IMPORT FOREIGN SCHEMA "example.*"
FROM SERVER example INTO sources;

-- Import just the customers table/view
IMPORT FOREIGN SCHEMA "example.customers"
FROM SERVER example INTO sources;
```

:::info
Each imported collection from MongoDB will be created as table with the following schema:

| Column | Type    |
| ------ | ------- |
| `doc`  | `jsonb` |

The `doc` column contains the JSON document for each item in the collection.
:::

### SQL support

| Feature                     | Supported | Notes |
| --------------------------- | --------- | ----- |
| `SELECT`                    | ✅        |       |
| `INSERT`                    | ✅        |       |
| Bulk `INSERT`               | ✅        |       |
| `UPDATE`                    | ✅        |       |
| `DELETE`                    | ✅        |       |
| `WHERE` pushdown            | ✅        |       |
| `JOIN` pushdown             | -         |       |
| `GROUP BY` pushdown         | ❌        |       |
| `ORDER BY` pushdown         | ✅        |       |
| `LIMIT` / `OFFSET` pushdown | ✅        |       |

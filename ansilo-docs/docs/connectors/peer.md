---
sidebar_position: 2
---

# Peer

The peer connector enables you to connect to other Ansilo nodes in your Data Mesh.

### Configuration

```yaml
sources:
  - id: example
    type: peer
    options:
      url: https://example.peer.node
      username: example_user
      password: example_pass
```

:::tip
You can omit the `username` and `password` fields to enable passthrough authentication into peer nodes.
This is useful for [JWT authentication](http://localhost:3000/fundamentals/security/#jwt-authentication).
:::

### Importing schemas

You can import foreign schemas using the `%` as a wildcard or specify a table explicitly.

```sql
-- Import all tables/views
IMPORT FOREIGN SCHEMA "%"
FROM SERVER example INTO sources;

-- Import just the customers tables/views
IMPORT FOREIGN SCHEMA "%"
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

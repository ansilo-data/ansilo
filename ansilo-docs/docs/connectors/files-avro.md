---
sidebar_position: 9
---

# Files (Avro)

Read or write data to [Avro](https://avro.apache.org/)-files on disk using the native driver.

### Configuration

```yaml
sources:
  - id: example
    type: file.avro
    options:
      path: /path/to/avro/folder/
```

### Supported options

| Option | Description                                                |
| ------ | ---------------------------------------------------------- |
| `path` | The path of the folder where the avro files will be stored |

### Importing schemas

You can import foreign schemas using the `*` as a wildcard or specify a file name explicitly.

```sql
-- Import all avro files in the configured `path`
IMPORT FOREIGN SCHEMA "*"
FROM SERVER example INTO sources;

-- Import just a single file from the `path`
IMPORT FOREIGN SCHEMA "example.avro"
FROM SERVER example INTO sources;
```

:::info
Only files ending with `.avro` will be imported.
:::

:::tip
Imported tables will be named using the full file name, eg `example.avro`.

To reference this table you need to quote the table name in your queries.

```sql
-- Retrieve data from the avro file
SELECT * FROM sources."example.avro";
```

:::

### SQL support

| Feature                     | Supported | Notes                                                          |
| --------------------------- | --------- | -------------------------------------------------------------- |
| `SELECT`                    | ✅        |                                                                |
| `INSERT`                    | ✅        |                                                                |
| Bulk `INSERT`               | ✅        |                                                                |
| `UPDATE`                    | -         |                                                                |
| `DELETE`                    | ✅        | Conditions are not supported. `DELETE` will truncate the file. |
| `WHERE` pushdown            | -         |                                                                |
| `JOIN` pushdown             | -         |                                                                |
| `GROUP BY` pushdown         | -         |                                                                |
| `ORDER BY` pushdown         | -         |                                                                |
| `LIMIT` / `OFFSET` pushdown | -         |                                                                |

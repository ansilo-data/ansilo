---
sidebar_position: 3
---

# Documentation

Providing rich documentation in your data catalog is a great way to make it easier for other teams 
to start using your data product.

The data catalog is automatically generated using tables and views defined in the `public` schema.

### Documenting tables and views

We use [postgres comments](https://www.postgresql.org/current/sql-comment.html) to provide additional
descriptions to our tables and views.

```sql
-- Comment on a the customers view
COMMENT ON VIEW customers IS 'The customers of our organisation';

-- Comment on specific columns
COMMENT ON COLUMN customers.id IS 'UUIDv4 identifier';
```

:::tip
For multiline comments you can use [dollar-qouted strings](https://www.postgresql.org/current/sql-syntax-lexical.html#SQL-SYNTAX-DOLLAR-QUOTING).
:::
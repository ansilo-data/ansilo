---
sidebar_position: 1
---

# Overview

Ansilo supports the most prevalent relational, NoSQL and other types of data sources.
We will keep adding support for new data sources, feel free to [contact us](/todo) and submit a feature request for your data store!

## Support Matrix

|                              | Readable | Writable | Condition Pushdown | Join Pushdown | Aggregation Pushdown | Sort/Limit/Offset Pushdown |
| ---------------------------- | -------- | -------- | ------------------ | ------------- | -------------------- | -------------------------- |
| [PostgreSQL](./postgresql)   | ✅       | ✅       | ✅                 | ✅            | ✅                   | ✅                         |
| [MySQL](./mysql)             | ✅       | ✅       | ✅                 | ✅            | ✅                   | ✅                         |
| [Oracle](./oracle)           | ✅       | ✅       | ✅                 | ✅            | ✅                   | ✅                         |
| [SQL Server](./sql-server)   | ✅       | ✅       | ✅                 | ✅            | ✅                   | ✅                         |
| [SQLite](./sqlite)           | ✅       | ✅       | ✅                 | ✅            | ✅                   | ✅                         |
| [Teradata](./teradata)       | ✅       | ✅       | ✅                 | ✅            | ✅                   | ✅                         |
| [MongoDB](./mongodb)         | ✅       | ✅       | ✅                 | -             | ❌                   | ✅                         |
| [Files (Avro)](./files-avro) | ✅       | ✅       | -                  | -             | -                    | -                          |

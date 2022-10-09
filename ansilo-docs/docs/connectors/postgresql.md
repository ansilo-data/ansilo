---
sidebar_position: 3
---

# PostgreSQL

Connect to [PostgreSQL databases](https://www.postgresql.org/) using the native driver.

### Configuration

```yaml
sources:
  - id: postgres
    type: native.postgres
    options:
      url: host=my.postgres.host port=5432 user=example_user password=example_pass dbname=example_db
```

### Supported options

| Key                    | Description                                                                                                                                                                                                                                                                                                                  |
| ---------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `user`                 | The username to authenticate with. Required.                                                                                                                                                                                                                                                                                 |
| `password`             | The password to authenticate with.                                                                                                                                                                                                                                                                                           |
| `dbname`               | The name of the database to connect to. Defaults to the username.                                                                                                                                                                                                                                                            |
| `options`              | Command line options used to configure the server.                                                                                                                                                                                                                                                                           |
| `application_name`     | Sets the `application_name` parameter on the server.                                                                                                                                                                                                                                                                         |
| `sslmode`              | Controls usage of TLS. If set to `disable`, TLS will not be used. If set to `prefer`, TLS will be used if available, but not used otherwise. If set to `require`, TLS will be forced to be used. Defaults to prefer.                                                                                                         |
| `host`                 | The host to connect to. On Unix platforms, if the host starts with a `/` character it is treated as the path to the directory containing Unix domain sockets. Otherwise, it is treated as a hostname. Multiple hosts can be specified, separated by commas. Each host will be tried in turn when connecting.                 |
| `port`                 | The port to connect to. Multiple ports can be specified, separated by commas. The number of ports must be either 1, in which case it will be used for all hosts, or the same as the number of hosts. Defaults to 5432 if omitted or the empty string.                                                                        |
| `connect_timeout`      | The time limit in seconds applied to each socket-level connection attempt. Note that hostnames can resolve to multiple IP addresses, and this limit is applied to each address. Defaults to no timeout.                                                                                                                      |
| `keepalives`           | Controls the use of TCP keepalive. A value of 0 disables keepalive and nonzero integers enable it. This option is ignored when connecting with Unix sockets. Defaults to `on`.                                                                                                                                               |
| `keepalives_idle`      | The number of seconds of inactivity after which a keepalive message is sent to the server. This option is ignored when connecting with Unix sockets. Defaults to 2 hours.                                                                                                                                                    |
| `target_session_attrs` | Specifies requirements of the session. If set to `read-write`, the client will check that the transaction_read_write session parameter is set to on. This can be used to connect to the primary server in a database cluster as opposed to the secondary read-only mirrors. Defaults to `all`.                               |
| `channel_binding`      | Controls usage of channel binding in the authentication process. If set to `disable`, channel binding will not be used. If set to `prefer`, channel binding will be used if available, but not used otherwise. If set to `require`, the authentication process will fail if channel binding is not used. Defaults to prefer. |

See [tokio postgres docs](https://docs.rs/tokio-postgres/latest/tokio_postgres/config/struct.Config.html) for more details.

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

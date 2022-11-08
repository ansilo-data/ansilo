---
sidebar_position: 3
---

# Consuming a data product

## Consuming from a _peer node_

A _peer node_ is another instance of Ansilo within your Data Mesh.
You can consume data products from other nodes using the same process as [connecting to your data store](../data-source).

### Step 1: Connect to a peer node

To read, ingest or write to an external data product from peer node, it is added
to the `sources` list in the `ansilo.yml`.

```yaml
# References the node running at https://customers.ansilo.host
sources:
  - id: customers
    type: peer
    options:
      url: https://customers.ansilo.host
```

:::info
By default, when connecting to peer nodes the authenticated the username and password will passthrough.
This works well with [JWT authentication](/fundamentals/security/#jwt-authentication) but is unlikely to work
with [password authentication](/fundamentals/security/#password-authentication). 
In this case you can explicitly specify the credentials. See [peer connector](/connectors/peer/) for details.
:::

### Step 2: Import the schemas

The schemas can be imported using [`IMPORT FOREIGN SCHEMA`](https://www.postgresql.org/current/sql-importforeignschema.html).

```sql
-- Create a schema for peer tables
CREATE SCHEMA peer;

-- Import foreign tables into the peer schema
IMPORT FOREIGN SCHEMA "%" 
FROM SERVER customers INTO peer;
```

The data from the products node can now be queried using standard SQL.

:::info
The tables imported from a peer node are those which defined in the peer node's `public` schema.
Tables in schema other than `public` cannot be imported by an external node.
:::


### Step 3: Query the data product

Now that the schema's have been imported you can use SQL to retrieve or modify data from the data product.

```sql
-- Retrieve data using SELECT
SELECT * FROM peer.customers;

-- Modify the data using INSERT/UPDATE/DELETE
UPDATE peer.customers SET name = '...' WHERE id = 123;

-- Ingest data into our local datastore
INSERT INTO sources.customers (id, name)
SELECT * FROM id, name FROM peer.customers;
```

## Consuming programmatically

You may also consume data products from your programming language of choice using a postgres database driver.

### Example using python and psycopg

```python
import psycopg

# Connect to your ansilo node
with psycopg.connect("host=customers.ansilo.host port=1234 client_encoding=utf8 user=example password=example") as conn:

    # Open a cursor to perform database operations
    with conn.cursor() as cur:

        # Execute a query
        cur.execute("SELECT * FROM customers")

        # Retrieve query results
        records = cur.fetchall()
```


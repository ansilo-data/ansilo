---
sidebar_position: 3
---

# Consuming a data product

:::info
A _peer node_ is another instance of Ansilo within your Data Mesh.
:::

:::info
Referencing data from other nodes follows the same process as [connecting to your data store](../data-source).
:::

### Step 1: Connect to a _peer node_

To read, ingest or write to an external data product from peer node, it is added
to the `sources` list in the `ansilo.yml`.

```yaml
# References the node running at https://products.ansilo.host
sources:
  - id: products
    type: peer
    options:
      url: https://products.ansilo.host
```

:::info
By default, when connecting to peer nodes the authenticated the username and password will passthrough.
This works well with [JWT authentication](/docs/fundamentals/security/#jwt-authentication) but is unlikely to work
with [password authentication](/docs/fundamentals/security/#password-authentication). 
In this case you can explicitly specify the credentials. See [peer connector](/docs/connectors/peer/) for details.
:::

### Step 2: Import the schemas

The schemas can be imported using [`IMPORT FOREIGN SCHEMA`](https://www.postgresql.org/docs/current/sql-importforeignschema.html).

```sql
-- Create a schema for peer tables
CREATE SCHEMA peer;

-- Import foreign tables into the peer schema
IMPORT FOREIGN SCHEMA "%" 
FROM SERVER products INTO peer;
```

The data from the products node can now be queried using standard SQL.

:::info
The tables imported from a peer node are those which defined in the peer node's `public` schema.
Tables in schema other than `public` cannot be imported by an external node.
:::

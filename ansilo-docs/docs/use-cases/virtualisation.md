---
sidebar_position: 4
---
import Diagram from './diagrams/virtualisation.svg'

# Virtualisation

Leverage unified SQL and Ansilo's powerful query translation engine to query across your data stores.

<center>
    <Diagram width="70%" height="auto" class="auto-invert" />
</center>

### Unified SQL

Ansilo exposes an industry-standard [PostgreSQL](https://postgresql.org) interface into each data store, regardless of the underlying
technology. This enables any platform which is compatible with PostgreSQL to start querying across your data stores. 
Covering all **common BI tools, programming languages, Jupyter** and many more platforms.


### Translation Engine

Ansilo is built with a powerful query translation engine that is capable of pushing down conditions, joins, aggregations and windowing
of queries. The full-query pushdown technology enables optimal query plans for efficient query execution.
See the list of supported [connectors](/docs/connectors/overview/).

### Query Workbench

The Ansilo workbench provides a self-service browser-based interface into each data store.
This enables both technical and non-technical staff to query across data stores using SQL. 

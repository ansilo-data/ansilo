---
sidebar_position: 1
---
import DataMesh from '/docs/use-cases/diagrams/data-mesh.svg'
import OverviewDiagram from './diagrams/system-overview.svg'

# Architecture

To make the most out of developing with Ansilo, it is important to understand the principles
of [Data Mesh](https://www.datamesh-architecture.com/) and the key components underpinning Ansilo.

<center>
    <DataMesh width="70%" height="auto" className="auto-invert" />
</center>

### Data Mesh 

A _node_ is an individual instance of Ansilo that is running on your infrastructure.
Each node is typically connected to a single logical data store, where it exposes
data schemas that can be read or written to. This way we have a clear and consistent 
interface into each data store. Some nodes may only expose data schemas for reading,
some for writing and some for both. The exact topology would depend on your organisation.

The network of nodes can be considered the logical Data Mesh of your organisation.
Multiple nodes can connect together providing interoperability between data stores,
with the ability to run SQL queries that read, analyse or move data across the network.

# System Design

Each node is deployed as stateless container composed of the following subsystems:

<center>
    <OverviewDiagram width="100%" height="auto" className="auto-invert" />
</center>

### Multi-protocol proxy

The proxy is the entrypoint into each node. Whether you are executing queries
via the workbench, running automated jobs or exposing data to your reporting tools
all communication is passed through the proxy.

The proxy provides exposes two protocols:

 1. **PostgreSQL**: for executing queries using standard postgres tools, libraries, programming
    languages or other Postgres-compatible platforms. 
 2. **HTTP**: provides the browser-based workbench interface.

The proxy performs authentication of incoming requests, supporting JWT and password-based authentication.

### PostgreSQL

At the heart of each instance is a PostgreSQL server. 
The data schemas for each node are defined through [foreign tables](https://www.postgresql.org/docs/current/sql-createforeigntable.html) and [views](https://www.postgresql.org/docs/current/sql-createview.html).
And Postgres is also the workhorse that, plans, optimises and executes all SQL queries. 
The majority of the development work using Ansilo is configuring PostgreSQL.

Unlike standard a Postgres server, this instance is considered stateless.
The foreign tables enable the querying of connected data stores
without the need for copying data. 

### Workbench / HTTP API

The workbench provides a browser-based interface for non-technical users to browse the data catalog
and execute queries. 

Each node also exposes a REST API that can be used to execute queries. In some cases this is easier
to use in some platforms than the PostgreSQL protocol.

### Data Source Connectors

Each node connects to its data store using a [PostgreSQL Foreign Data Wrapper](https://www.postgresql.org/docs/current/ddl-foreign-data.html).
Ansilo comes with a sophisticated framework for translating SQL into optimised queries for each data store.
This enables querying of external data using standard postgres SQL without having to copy data.

See the natively [supported connectors and SQL compatibility](/docs/connectors/overview/) for reference.
---
sidebar_position: 2
---
import WarehouseDiagram from './diagrams/etl-warehouse.svg'
import CloudDiagram from './diagrams/hybrid-cloud-activate.svg'

# ETL

Ansilo significantly reduces the cost of ETL/ELT by using unified SQL across data stores instead of brittle
data pipelines.

<center>
    <WarehouseDiagram width="70%" height="auto" class="auto-invert" />
</center>

### Unified SQL

Ansilo exposes a [PostgreSQL](https://postgresql.org) interface into each data store, regardless of the underlying
technology. This provides a unified SQL interface across relational, NoSQL and even flat-files. See the list of supported
[connectors](/docs/connectors/overview/).

Rather than spending costly development time building custom and often brittle data pipelines, the most common forms of ETL
can be distilled down into SQL queries that can fit onto a single monitor.

### Job Scheduling

Ansilo natively supports running unified SQL queries on a regular schedule. This enables common ETL
patterns such as batch jobs. Where relevant, jobs execution can be controlled from an external 
orchestration tool such as [Airflow](https://airflow.apache.org/) or equivalent.

### Access Federation

With native support for [Json Web Token](https://jwt.io) authorization, Ansilo enables your teams to define
tightly scoped access control, without the need for shared passwords and manual secret rotation.

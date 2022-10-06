---
sidebar_position: 3
---
import ActivateDiagram from './diagrams/hybrid-cloud-activate.svg'
import Consolidate from './diagrams/hybrid-cloud-consolidate.svg'

# Hybrid Cloud

Reduce the costs and risks of moving data between on-prem and cloud by leveraging Ansilo's
decentralised architecture. 

<center>
    <ActivateDiagram width="70%" height="auto" class="auto-invert" />
</center>

### Decentralised Architecture

Ansilo is the first data integration platform with a decentralised architecture at its heart.
This enables instances of Ansilo to be **deployed across network zones and system trust levels**
where each instance has only the minimum required access to the data stores within its zone.

### Network Security

Ansilo uses industry-standard TLS, encrypting traffic between instances and prevent man-in-the-middle attacks.
With native support for [Json Web Token](https://jwt.io) authorization, access across multiple instances
can be tightly controlled without the need for passwords or secrets.

### Unified SQL

Ansilo exposes a [PostgreSQL](https://postgresql.org) interface into each data store, regardless of the underlying
technology. Using unified SQL you can transfer large volumes of data securely between data stores, without the need
for files or persistent storage. See the list of supported [connectors](/docs/connectors/overview/).


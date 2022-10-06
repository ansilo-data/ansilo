---
sidebar_position: 5
---
import Diagram from './diagrams/data-sharing.svg'

# Data Sharing

Provide a standardised interface into your shared datasets with partner organisations
using Ansilo's [PostgreSQL](https://postgresql.org) interface.

<center>
    <Diagram width="70%" height="auto" class="auto-invert" />
</center>

### Unified SQL

Ansilo exposes an industry-standard [PostgreSQL](https://postgresql.org) interface into each shared datasets. 
This enables partner organisations to freely integrate using the tech stack of their choosing.

### Token Security

Ansilo uses industry-standard TLS, encrypting traffic between instances and prevent man-in-the-middle attacks.
With native support for [Json Web Token](https://jwt.io) authorization, access can be tightly controlled 
without the need for passwords or secrets.

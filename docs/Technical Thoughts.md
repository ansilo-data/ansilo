Thoughts
========

In order to have a workable distributed query engine we need an existing framework to build on as building one from scratch is too costly.
The current options are 

1. postgres with postgres_fdw and custom connectors
2. sqlite with virtual table connectors 
3. existing engine such as Trino

I am very much a fan of the philosophy behind SQLite and think the "lightness" and extensibility matches the use case very well here so I'm leaning towards that option. The way I'd imagine it working is I'd build custom virtual table adaptors for:

1. data sources (eg Oracle, Postgres, RMBDBS, file etc)
2. remote squirrel nodes which further proxy to their data sources

Really a big fan of the "simple" interface provided for vtables: https://www.sqlite.org/vtab.html
Rust is such a good fit for these implementations: https://docs.rs/rusqlite/latest/rusqlite/vtab/index.html

=========

On further analysis I believe the inability for SQLite to "push-down" aggregations and joins is a deal breaker.
It also doesn't help that it has no parallel query execution.
With this in mind I'm currently planning on building Ansilo on PostgresSQL using fdw.

Fundementally each ansilo node needs IO at the dataset/record level.
Some examples:

1. Reading from an app's DB and exposing the data product
2. A user executing a SQL query via the workbench UI
3. A scheduled job exporting a result set to a parqueet file. 
4. A scheduled job exporting a result set to a table in a data warehouse

This means there must be the concept of a "data sources" and "data destinations".
A single database could even be both, so more generally there is an array of "data repository" which could support read, write or both.

Playing into those use-cases from an interfacing perspective, there are really the following use cases we need to support:

1. A user accessing an Ansilo node via it's UI
2. A system executing a query or triggering a job
3. A another Ansilo node executing a query

All of these could be supported by a standard REST API interface.
I think this is the way to go for the MVP.

I did have the thought that potentially it could be useful to direct access to the Postgres instance for compatibility with other ETL tools/platforms.
Not certain it is necessary for the MVP at this stage. Handling authentication could be very tricky in this case.

With regards to authentication, we are enterprise-focused, so almost definitely auth is federated through some external IdP/JWT token.
This means we really just need to authenticate tokens on incoming requests and somehow federate the access down to the Postgres instance.



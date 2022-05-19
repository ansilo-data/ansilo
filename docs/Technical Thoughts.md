Thoughts
========

In order to have a workable distributed query engine we need an existing framework to build on as building one from scratch is too costly.
The current options are 

1. postgres with postgres_fwd and custom connectors
2. sqlite with virtual table connectors 
3. existing engine such as Trino

I am very much a fan of the philosophy behind SQLite and think the "lightness" and extensibility matches the use case very well here so I'm leaning towards that option. The way I'd imagine it working is I'd build custom virtual table adaptors for:

1. data sources (eg Oracle, Postgres, RMBDBS, file etc)
2. remote squirrel nodes which further proxy to their data sources

Really a big fan of the "simple" interface provided for vtables: https://www.sqlite.org/vtab.html
Rust is such a good fit for these implementations: https://docs.rs/rusqlite/latest/rusqlite/vtab/index.html


/// Module for implementation of the protocol used by our postgres extension (ansilo-pgx)
/// to retrieve data from our connectors in order to implement postgres FDW (foreign data wrapper)
/// @see https://www.postgresql.org/docs/current/postgres-fdw.html

pub mod proto;
pub mod channel;
pub mod server;
pub mod bincode;
pub mod connection;

#[cfg(test)]
mod test;
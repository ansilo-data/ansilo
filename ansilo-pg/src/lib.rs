/// This module orchestrates our postgres instance and provides an api
/// to execute queries against it. Postgres is run as a child process in
/// a dedicated uid/gid to limit the attack surface it poses.
/// 
/// In order for postgres to retrieve data from our sources, the ansilo-pgx
/// extension is installed which creates a FDW which connects back to our
/// ansilo process over a unix socket.

pub mod conf;
pub mod proc;
pub mod initdb;
pub mod server;
pub mod connection;

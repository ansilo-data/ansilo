use std::time::Duration;

use postgres::{Client, NoTls};

/// Connects to the ansilo instance running on the supplied port
pub fn connect(port: u16) -> Client {
    // TODO: auth
    Client::configure()
        .connect_timeout(Duration::from_secs(30))
        .port(port)
        .host("localhost")
        .user("ansiloapp")
        .dbname("postgres")
        .connect(NoTls)
        .unwrap()
}

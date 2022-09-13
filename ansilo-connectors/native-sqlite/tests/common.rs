use ansilo_connectors_native_sqlite::{SqliteConnection, SqliteConnectionConfig, SqliteConnector};

pub fn connect_to_sqlite() -> SqliteConnection {
    SqliteConnector::connect(SqliteConnectionConfig {
        path: ":memory:".into(),
    })
    .unwrap()
}

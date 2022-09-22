use std::{fs, path::PathBuf};

use ansilo_connectors_native_sqlite::{SqliteConnection, SqliteConnectionConfig, SqliteConnector};
use ansilo_logging::info;
use glob::glob;
use tempfile::NamedTempFile;

// Creates a new sqlite db
pub fn init_sqlite_sql(init_sql_path: PathBuf) -> (SqliteConnection, PathBuf) {
    let path = NamedTempFile::new()
        .unwrap()
        .path()
        .with_extension(".sqlite");
    let path = path.to_str().unwrap();

    let mut connection = SqliteConnector::connect(SqliteConnectionConfig {
        path: path.into(),
        extensions: vec![],
    })
    .unwrap();

    for path in glob(init_sql_path.to_str().unwrap())
        .unwrap()
        .map(|i| i.unwrap())
    {
        info!("Running sqlite init script: {}", path.display());
        let sql = fs::read_to_string(path).unwrap();
        let statements = sql.split("$$").filter(|s| s.trim().len() > 0);

        for stmt in statements {
            connection.execute_modify(stmt, vec![]).unwrap();
        }
    }

    (connection, path.into())
}

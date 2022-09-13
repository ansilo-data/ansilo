use std::collections::HashMap;

use ansilo_core::{
    config::{EntityAttributeConfig, EntityConfig, EntitySourceConfig, NodeConfig},
    err::{Context, Error, Result},
};

use ansilo_connectors_base::interface::{EntityDiscoverOptions, EntitySearcher};
use ansilo_logging::warn;
use fallible_iterator::FallibleIterator;
use rusqlite::ToSql;

use crate::{from_sqlite_type, SqliteConnection, SqliteTableOptions};

use super::SqliteEntitySourceConfig;

/// The entity searcher for Sqlite
pub struct SqliteEntitySearcher {}

impl EntitySearcher for SqliteEntitySearcher {
    type TConnection = SqliteConnection;
    type TEntitySourceConfig = SqliteEntitySourceConfig;

    fn discover(
        connection: &mut Self::TConnection,
        _nc: &NodeConfig,
        opts: EntityDiscoverOptions,
    ) -> Result<Vec<EntityConfig>> {
        let tables = {
            let mut query = connection
                .con()
                .prepare(
                    r#"
                SELECT name
                FROM sqlite_schema
                WHERE 1=1
                AND type = 'table'
                AND name LIKE ?
            "#,
                )
                .context("Failed to prepare query")?;

            let tables = query
                .query(&[&opts
                    .remote_schema
                    .as_ref()
                    .unwrap_or(&"%".to_string())
                    .to_sql()?])
                .context("Failed to execute query")?;

            tables
                .map(|row| row.get::<_, String>("name"))
                .collect::<Vec<String>>()?
        };

        let entities = tables
            .into_iter()
            .filter_map(
                |table| match parse_entity_config(connection, table.clone()) {
                    Ok(conf) => Some(conf),
                    Err(err) => {
                        warn!("Failed to import schema for table \"{}\": {:?}", table, err);
                        None
                    }
                },
            )
            .collect();

        Ok(entities)
    }
}

pub(crate) fn parse_entity_config(
    con: &mut SqliteConnection,
    table: String,
) -> Result<EntityConfig> {
    let mut query = con
        .con()
        .prepare("SELECT * FROM pragma_table_info(?)")
        .context("Failed to prepare query")?;

    let rows = query
        .query(&[&table.to_sql()?])
        .context("Failed to execute query")?;

    let cols = rows
        .map(|row| {
            Ok((
                row.get::<_, String>("name")?,
                row.get::<_, String>("type")?,
                row.get::<_, bool>("notnull")?,
                row.get::<_, bool>("pk")?,
            ))
        })
        .map_err(|e| Error::msg(e.to_string()))
        .collect::<Vec<(String, String, bool, bool)>>()?;

    Ok(EntityConfig::new(
        table.clone(),
        None,
        None,
        vec![],
        cols.into_iter()
            .filter_map(|c| {
                let name = c.0.clone();
                parse_column(name.as_str(), c)
                    .map_err(|e| warn!("Ignoring column '{}': {:?}", name, e))
                    .ok()
            })
            .collect(),
        vec![],
        EntitySourceConfig::from(SqliteEntitySourceConfig::Table(SqliteTableOptions::new(
            table.clone(),
            HashMap::new(),
        )))?,
    ))
}

fn parse_column(
    name: &str,
    c: (String, String, bool, bool),
) -> Result<EntityAttributeConfig, ansilo_core::err::Error> {
    Ok(EntityAttributeConfig::new(
        name.to_string(),
        None,
        from_sqlite_type(&c.1)?,
        c.2,
        !c.3,
    ))
}

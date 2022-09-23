use std::collections::HashMap;

use ansilo_core::{
    config::{EntityAttributeConfig, EntityConfig, EntitySourceConfig, NodeConfig},
    data::{DataType, DataValue, StringOptions},
    err::{bail, Context, Result},
};

use ansilo_connectors_base::{
    common::query::QueryParam,
    interface::{Connection, EntityDiscoverOptions, EntitySearcher, QueryHandle, ResultSet},
};
use ansilo_connectors_jdbc_base::{JdbcConnection, JdbcQuery};
use ansilo_logging::warn;
use itertools::Itertools;

use crate::TeradataJdbcTableOptions;

use super::TeradataJdbcEntitySourceConfig;

/// The entity searcher for Teradata JDBC
pub struct TeradataJdbcEntitySearcher {}

impl EntitySearcher for TeradataJdbcEntitySearcher {
    type TConnection = JdbcConnection;
    type TEntitySourceConfig = TeradataJdbcEntitySourceConfig;

    fn discover(
        connection: &mut Self::TConnection,
        _nc: &NodeConfig,
        opts: EntityDiscoverOptions,
    ) -> Result<Vec<EntityConfig>> {
        // Query teradata's information schema tables to retrieve all column definitions
        // Importantly we order the results by table and then by column position
        // when lets us efficiently group the result by table using `group_by` below.
        // Additionally, we the results to be deterministic and return the columns
        // the user-defined order on the teradata side.
        // @see ColumnsV https://docs.teradata.com/r/oiS9ixs2ypIQvjTUOJfgoA/fQ8NslP6DDESV0ZiODLlIw
        // @see TablesV https://docs.teradata.com/r/oiS9ixs2ypIQvjTUOJfgoA/JKGDTOsfv6_gr8wswcE9eA
        let cols = connection
            .prepare(JdbcQuery::new(
                r#"
                SELECT
                    T.DatabaseName,
                    T.TableName,
                    T.CommentString AS TableComment,
                    C.ColumnName,
                    C.ColumnTitle,
                    C.CommentString AS ColumnComment,
                    C.ColumnType,
                    C.Nullable,
                    C.ColumnLength,
                    C.DecimalTotalDigits,
                    C.DecimalFractionalDigits,
                    C.ColumnPosition,
                    C.IdColType IS NOT NULL as PrimaryKey
                FROM DBC.TablesV T
                INNER JOIN DBC.ColumnsV C ON T.DatabaseName = C.DatabaseName AND T.TableName = C.TableName
                WHERE (T.DatabaseName || '.' || T.TableName) LIKE ?
                AND T.TableKind IN ('O', 'T', 'V')
                ORDER BY T.DatabaseName, T.TableName, C.ColumnPosition
            "#,
                vec![QueryParam::constant(DataValue::Utf8String(
                    opts.remote_schema
                        .as_ref()
                        .map(|i| i.as_str())
                        .unwrap_or("%")
                        .into(),
                ))],
            ))?
            .execute_query()?;

        let cols = cols.reader()?.iter_rows().collect::<Result<Vec<_>>>()?;
        let tables = cols.into_iter().group_by(|row| {
            (
                row["DatabaseName"].as_utf8_string().unwrap().clone(),
                row["TableName"].as_utf8_string().unwrap().clone(),
            )
        });

        let entities = tables
            .into_iter()
            .filter_map(|((database, table), cols)| {
                match parse_entity_config(&database, &table, cols.into_iter()) {
                    Ok(conf) => Some(conf),
                    Err(err) => {
                        warn!(
                            "Failed to import schema for table \"{}.{}\": {:?}",
                            database, table, err
                        );
                        None
                    }
                }
            })
            .collect();

        Ok(entities)
    }
}

pub(crate) fn parse_entity_config(
    database: &String,
    table: &String,
    cols: impl Iterator<Item = HashMap<String, DataValue>>,
) -> Result<EntityConfig> {
    let cols = cols.collect::<Vec<_>>();
    Ok(EntityConfig::new(
        table.clone(),
        None,
        cols[0]["TableComment"].as_utf8_string().map(|s| s.clone()),
        vec![],
        cols.into_iter()
            .filter_map(|c| {
                let name = c["ColumnName"].as_utf8_string().or_else(|| {
                    warn!("Failed to parse column name");
                    None
                })?;
                parse_column(name, &c)
                    .map_err(|e| warn!("Ignoring column '{}': {:?}", name, e))
                    .ok()
            })
            .collect(),
        vec![],
        EntitySourceConfig::from(TeradataJdbcEntitySourceConfig::Table(
            TeradataJdbcTableOptions::new(database.clone(), table.clone(), HashMap::new()),
        ))?,
    ))
}

pub(crate) fn parse_column(
    name: &str,
    c: &HashMap<String, DataValue>,
) -> Result<EntityAttributeConfig> {
    let col_type = from_teradata_col(&c)?;

    Ok(EntityAttributeConfig::new(
        name.to_string(),
        c["ColumnComment"]
            .as_utf8_string()
            .or_else(|| c["ColumnTitle"].as_utf8_string())
            .map(|s| s.clone()),
        col_type,
        *c["PrimaryKey"].as_boolean().context("PrimaryKey")?,
        c["Nullable"].as_utf8_string().context("Nullable")? == "Y",
    ))
}

pub(crate) fn from_teradata_col(col: &HashMap<String, DataValue>) -> Result<DataType> {
    let td_type = col["ColumnType"].as_utf8_string().context("ColumnType")?;

    // @see https://docs.teradata.com/r/oiS9ixs2ypIQvjTUOJfgoA/fQ8NslP6DDESV0ZiODLlIw
    Ok(match td_type.as_str() {
        "CF" | "CO" | "CV" | "LF" | "LV" | "UV" => {
            let length = col["ColumnLength"]
                .clone()
                .try_coerce_into(&DataType::UInt32)
                .ok()
                .and_then(|i| i.as_u_int32().cloned())
                .and_then(|i| if i >= 1 { Some(i) } else { None });

            DataType::Utf8String(StringOptions::new(length))
        }
        "I1" => DataType::Int8,
        "I2" => DataType::Int16,
        "I" => DataType::Int32,
        "I8" => DataType::Int64,
        "F" => DataType::Float64,
        "BF" | "BO" | "BV" => DataType::Binary,
        "JSON" => DataType::JSON,
        "DA" => DataType::Date,
        "AT" => DataType::Time,
        "TS" => DataType::DateTime,
        "TZ" => DataType::DateTimeWithTZ,
        _ => {
            bail!("Encountered unknown data type '{td_type}'");
        }
    })
}

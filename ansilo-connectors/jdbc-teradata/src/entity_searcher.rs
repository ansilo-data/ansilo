use std::collections::HashMap;

use ansilo_core::{
    config::{EntityAttributeConfig, EntityConfig, EntitySourceConfig, NodeConfig},
    data::{DataType, DataValue, DecimalOptions, StringOptions},
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
                    C.DataBaseName AS DataBaseName,
                    C.TableName AS TableName,
                    C.ColumnName AS ColumnName,
                    T.CommentString AS TableComment,
                    E.ColumnTitle AS ColumnTitle,
                    E.CommentString AS ColumnComment,
                    E.ColumnType AS ColumnType,
                    E.Nullable AS Nullable,
                    E.ColumnLength AS ColumnLength,
                    E.DecimalTotalDigits AS DecimalTotalDigits,
                    E.DecimalFractionalDigits AS DecimalFractionalDigits,
                    E.ColumnID AS ColumnID,
                    C.PrimaryKey AS PrimaryKey
                FROM (
                    SELECT
                        C.DataBaseName,
                        C.TableName,
                        C.ColumnName,
                        SUM(CASE WHEN I.IndexType IS NULL THEN 0 ELSE 1 END) AS PrimaryKey
                    FROM DBC.ColumnsV AS C
                    LEFT JOIN DBC.IndicesV AS I ON I.IndexType = 'K' AND I.DataBaseName = C.DataBaseName AND I.TableName = C.TableName AND I.ColumnName = C.ColumnName
                    WHERE (C.DataBaseName || '.' || C.TableName) LIKE ?
                    GROUP BY C.DataBaseName, C.TableName, C.ColumnName
                ) AS C
                INNER JOIN DBC.TablesV AS T ON T.DataBaseName = C.DataBaseName AND T.TableName = C.TableName
                INNER JOIN DBC.ColumnsV AS E ON E.DataBaseName = C.DataBaseName AND E.TableName = C.TableName AND E.ColumnName = C.ColumnName
                WHERE T.TableKind IN ('O', 'T', 'V')
                ORDER BY C.DataBaseName, C.TableName, E.ColumnID
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
                row["DataBaseName"].as_utf8_string().unwrap().clone(),
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
        *c["PrimaryKey"]
            .clone()
            .try_coerce_into(&DataType::Int32)
            .unwrap_or(DataValue::Int32(0))
            .as_int32()
            .unwrap()
            > 0,
        c["Nullable"].as_utf8_string().context("Nullable")? == "Y",
    ))
}

pub(crate) fn from_teradata_col(col: &HashMap<String, DataValue>) -> Result<DataType> {
    let td_type = col["ColumnType"].as_utf8_string().context("ColumnType")?;

    // @see https://docs.teradata.com/r/oiS9ixs2ypIQvjTUOJfgoA/fQ8NslP6DDESV0ZiODLlIw
    Ok(match td_type.trim() {
        "CF" | "CO" | "CV" | "LF" | "LV" | "UV" => {
            let length = col["ColumnLength"]
                .clone()
                .try_coerce_into(&DataType::UInt32)
                .ok()
                .and_then(|i| i.as_u_int32().cloned())
                .and_then(|i| if i >= 1 { Some(i) } else { None });

            DataType::Utf8String(StringOptions::new(length))
        }
        "D" => {
            let precision = col["DecimalTotalDigits"]
                .clone()
                .try_coerce_into(&DataType::UInt16)
                .ok()
                .and_then(|i| i.as_u_int16().cloned());
            let scale = col["DecimalFractionalDigits"]
                .clone()
                .try_coerce_into(&DataType::UInt16)
                .ok()
                .and_then(|i| i.as_u_int16().cloned());

            DataType::Decimal(DecimalOptions::new(precision, scale))
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
        "TZ" | "SZ" => DataType::DateTimeWithTZ,
        _ => {
            bail!("Encountered unknown data type '{td_type}'");
        }
    })
}

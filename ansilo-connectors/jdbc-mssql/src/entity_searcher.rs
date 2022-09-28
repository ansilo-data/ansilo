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

use crate::MssqlJdbcTableOptions;

use super::MssqlJdbcEntitySourceConfig;

/// The entity searcher for Mssql JDBC
pub struct MssqlJdbcEntitySearcher {}

impl EntitySearcher for MssqlJdbcEntitySearcher {
    type TConnection = JdbcConnection;
    type TEntitySourceConfig = MssqlJdbcEntitySourceConfig;

    fn discover(
        connection: &mut Self::TConnection,
        _nc: &NodeConfig,
        opts: EntityDiscoverOptions,
    ) -> Result<Vec<EntityConfig>> {
        // Query mssql's information schema tables to retrieve all column definitions
        // Importantly we order the results by table and then by column position
        // when lets us efficiently group the result by table using [group_by] below.
        // Additionally, we the results to be deterministic and return the columns
        // the user-defined order on the oracle side.
        let cols = connection
            .prepare(JdbcQuery::new(
                r#"
                SELECT
                    T.TABLE_SCHEMA,
                    T.TABLE_NAME,
                    C.COLUMN_NAME,
                    C.DATA_TYPE,
                    C.IS_NULLABLE,
                    C.CHARACTER_MAXIMUM_LENGTH,
                    C.NUMERIC_PRECISION,
                    C.NUMERIC_SCALE,
                    C.ORDINAL_POSITION,
                    (
                        SELECT COUNT(1)
                        FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE U
                        INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS S ON U.CONSTRAINT_NAME = S.CONSTRAINT_NAME AND U.TABLE_NAME = S.TABLE_NAME
                        WHERE S.CONSTRAINT_TYPE = 'Primary Key'
                        AND S.TABLE_NAME = T.TABLE_NAME
                        AND U.COLUMN_NAME = C.COLUMN_NAME
                    ) AS COLUMN_PK
                FROM INFORMATION_SCHEMA.TABLES T
                INNER JOIN INFORMATION_SCHEMA.COLUMNS C ON T.TABLE_SCHEMA = C.TABLE_SCHEMA AND T.TABLE_NAME = C.TABLE_NAME
                WHERE 1=1
                AND CONCAT(T.TABLE_SCHEMA, '.', T.TABLE_NAME) LIKE ?
                ORDER BY T.TABLE_SCHEMA, T.TABLE_NAME, C.ORDINAL_POSITION
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
                row["TABLE_SCHEMA"].as_utf8_string().unwrap().clone(),
                row["TABLE_NAME"].as_utf8_string().unwrap().clone(),
            )
        });

        let entities = tables
            .into_iter()
            .filter_map(|((schema, table), cols)| {
                match parse_entity_config(&schema, &table, cols.into_iter()) {
                    Ok(conf) => Some(conf),
                    Err(err) => {
                        warn!(
                            "Failed to import schema for table \"{}.{}\": {:?}",
                            schema, table, err
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
    schema: &String,
    table: &String,
    cols: impl Iterator<Item = HashMap<String, DataValue>>,
) -> Result<EntityConfig> {
    Ok(EntityConfig::minimal(
        table.clone(),
        cols.filter_map(|c| {
            let name = c["COLUMN_NAME"].as_utf8_string().or_else(|| {
                warn!("Failed to parse column name");
                None
            })?;
            parse_column(name, &c)
                .map_err(|e| warn!("Ignoring column '{}': {:?}", name, e))
                .ok()
        })
        .collect(),
        EntitySourceConfig::from(MssqlJdbcEntitySourceConfig::Table(
            MssqlJdbcTableOptions::new(schema.clone(), table.clone(), HashMap::new()),
        ))?,
    ))
}

pub(crate) fn parse_column(
    name: &str,
    c: &HashMap<String, DataValue>,
) -> Result<EntityAttributeConfig> {
    let data_type = from_mssql_type(&c)?;

    Ok(EntityAttributeConfig::new(
        name.to_string(),
        None,
        data_type,
        *c["COLUMN_PK"].as_int32().context("COLUMN_PK")? > 0,
        c["IS_NULLABLE"].as_utf8_string().context("IS_NULLABLE")? == "YES",
    ))
}

pub(crate) fn from_mssql_type(col: &HashMap<String, DataValue>) -> Result<DataType> {
    let data_type = &col["DATA_TYPE"]
        .as_utf8_string()
        .context("DATA_TYPE")?
        .to_uppercase();
    let precision = col["NUMERIC_PRECISION"]
        .clone()
        .try_coerce_into(&DataType::UInt16)
        .ok()
        .and_then(|i| i.as_u_int16().cloned());

    Ok(match data_type.as_str() {
        "CHAR" | "NCHAR" | "VARCHAR" | "NVARCHAR" | "TEXT" | "NTEXT" => {
            let length = col["CHARACTER_MAXIMUM_LENGTH"]
                .clone()
                .try_coerce_into(&DataType::UInt32)
                .ok()
                .and_then(|i| i.as_u_int32().cloned())
                .and_then(|i| if i >= 1 { Some(i) } else { None });

            DataType::Utf8String(StringOptions::new(length))
        }
        "BIT" => DataType::Boolean,
        "TINYINT" => DataType::UInt8,
        "SMALLINT" => DataType::Int16,
        "INT" => DataType::Int32,
        "BIGINT" => DataType::Int64,
        "DECIMAL" | "NUMERIC" => {
            let scale = col["NUMERIC_SCALE"]
                .clone()
                .try_coerce_into(&DataType::UInt16)
                .ok()
                .and_then(|i| i.as_u_int16().cloned());

            DataType::Decimal(DecimalOptions::new(precision, scale))
        }
        "FLOAT" | "REAL" if precision.is_some() && precision.unwrap() <= 24 => DataType::Float32,
        "FLOAT" | "REAL" => DataType::Float64,
        "BINARY" | "VARBINARY" => DataType::Binary,
        "DATE" => DataType::Date,
        "TIME" => DataType::Time,
        "SMALLDATETIME" | "TIMESTAMP" | "DATETIME" | "DATETIME2" => DataType::DateTime,
        "DATETIMEOFFSET" => DataType::DateTimeWithTZ,
        "UNIQUEIDENTIFIER" => DataType::Uuid,
        _ => {
            bail!("Encountered unknown data type '{data_type}'");
        }
    })
}

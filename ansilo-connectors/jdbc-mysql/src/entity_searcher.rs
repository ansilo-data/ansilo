use std::collections::HashMap;

use ansilo_core::{
    config::{EntityAttributeConfig, EntityConfig, EntitySourceConfig, NodeConfig},
    data::{DataType, DataValue, DecimalOptions, StringOptions},
    err::{Context, Result},
};

use ansilo_connectors_base::interface::{
    Connection, EntityDiscoverOptions, EntitySearcher, QueryHandle, ResultSet,
};
use ansilo_connectors_jdbc_base::{JdbcConnection, JdbcQuery, JdbcQueryParam};
use ansilo_logging::warn;
use itertools::Itertools;

use crate::MysqlJdbcTableOptions;

use super::MysqlJdbcEntitySourceConfig;

/// The entity searcher for Mysql JDBC
pub struct MysqlJdbcEntitySearcher {}

impl EntitySearcher for MysqlJdbcEntitySearcher {
    type TConnection = JdbcConnection;
    type TEntitySourceConfig = MysqlJdbcEntitySourceConfig;

    fn discover(
        connection: &mut Self::TConnection,
        _nc: &NodeConfig,
        opts: EntityDiscoverOptions,
    ) -> Result<Vec<EntityConfig>> {
        // Query mysql's information schema tables to retrieve all column definitions
        // Importantly we order the results by table and then by column position
        // when lets us efficiently group the result by table using `group_by` below.
        // Additionally, we the results to be deterministic and return the columns
        // the user-defined order on the oracle side.
        let cols = connection
            .prepare(JdbcQuery::new(
                r#"
                SELECT
                    T.TABLE_SCHEMA,
                    T.TABLE_NAME,
                    C.COLUMN_NAME,
                    C.COLUMN_KEY,
                    C.DATA_TYPE,
                    C.COLUMN_TYPE,
                    C.IS_NULLABLE,
                    C.CHARACTER_MAXIMUM_LENGTH,
                    C.NUMERIC_PRECISION,
                    C.NUMERIC_SCALE,
                    C.ORDINAL_POSITION
                FROM INFORMATION_SCHEMA.TABLES T
                INNER JOIN INFORMATION_SCHEMA.COLUMNS C ON T.TABLE_SCHEMA = C.TABLE_SCHEMA AND T.TABLE_NAME = C.TABLE_NAME
                WHERE 1=1
                AND CONCAT(T.TABLE_SCHEMA, '.', T.TABLE_NAME) LIKE ?
                ORDER BY T.TABLE_SCHEMA, T.TABLE_NAME, C.ORDINAL_POSITION
            "#,
                vec![JdbcQueryParam::Constant(DataValue::Utf8String(
                    opts.remote_schema
                        .as_ref()
                        .map(|i| i.as_str())
                        .unwrap_or("%")
                        .into(),
                ))],
            ))?
            .execute()?;

        let cols = cols.reader()?.iter_rows().collect::<Result<Vec<_>>>()?;
        let tables = cols.into_iter().group_by(|row| {
            (
                row["TABLE_SCHEMA"].as_utf8_string().unwrap().clone(),
                row["TABLE_NAME"].as_utf8_string().unwrap().clone(),
            )
        });

        let entities = tables
            .into_iter()
            .filter_map(|((db, table), cols)| {
                match parse_entity_config(&db, &table, cols.into_iter()) {
                    Ok(conf) => Some(conf),
                    Err(err) => {
                        warn!(
                            "Failed to import schema for table \"{}.{}\": {:?}",
                            db, table, err
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
    db: &String,
    table: &String,
    cols: impl Iterator<Item = HashMap<String, DataValue>>,
) -> Result<EntityConfig> {
    Ok(EntityConfig::minimal(
        format!("{}.{}", db.clone(), table.clone()),
        cols.map(|c| {
            Ok(EntityAttributeConfig::new(
                c["COLUMN_NAME"]
                    .as_utf8_string()
                    .context("COLUMN_NAME")?
                    .clone(),
                None,
                from_mysql_type(&c)?,
                c["COLUMN_KEY"].as_utf8_string().context("COLUMN_KEY")? == "PRI",
                c["IS_NULLABLE"].as_utf8_string().context("IS_NULLABLE")? == "YES",
            ))
        })
        .collect::<Result<Vec<_>>>()?,
        EntitySourceConfig::from(MysqlJdbcEntitySourceConfig::Table(
            MysqlJdbcTableOptions::new(Some(db.clone()), table.clone(), HashMap::new()),
        ))?,
    ))
}

pub(crate) fn from_mysql_type(col: &HashMap<String, DataValue>) -> Result<DataType> {
    let data_type = &col["DATA_TYPE"]
        .as_utf8_string()
        .context("DATA_TYPE")?
        .to_uppercase();
    let col_type = &col["COLUMN_TYPE"]
        .as_utf8_string()
        .context("COLUMN_TYPE")?
        .to_uppercase();

    Ok(match data_type.as_str() {
        "CHAR" | "NCHAR" | "VARCHAR" | "NVARCHAR" | "TINYTEXT" | "TEXT" | "MEDIUMTEXT"
        | "LONGTEXT" => {
            let length = col["CHARACTER_MAXIMUM_LENGTH"]
                .clone()
                .try_coerce_into(&DataType::UInt32)
                .ok()
                .and_then(|i| i.as_u_int32().cloned())
                .and_then(|i| if i >= 1 { Some(i) } else { None });

            DataType::Utf8String(StringOptions::new(length))
        }
        "BIT" if col_type == "BIT(1)" => DataType::Boolean,
        "TINYINT" if col_type.contains("UNSIGNED") => DataType::UInt8,
        "SMALLINT" if col_type.contains("UNSIGNED") => DataType::UInt16,
        "INT" if col_type.contains("UNSIGNED") => DataType::UInt32,
        "BIGINT" if col_type.contains("UNSIGNED") => DataType::UInt64,
        "TINYINT" => DataType::Int8,
        "SMALLINT" => DataType::Int16,
        "INT" => DataType::Int32,
        "BIGINT" => DataType::Int64,
        "DECIMAL" => {
            let precision = col["NUMERIC_PRECISION"]
                .clone()
                .try_coerce_into(&DataType::UInt16)
                .ok()
                .and_then(|i| i.as_u_int16().cloned());
            let scale = col["NUMERIC_SCALE"]
                .clone()
                .try_coerce_into(&DataType::UInt16)
                .ok()
                .and_then(|i| i.as_u_int16().cloned());

            DataType::Decimal(DecimalOptions::new(precision, scale))
        }

        "FLOAT" => DataType::Float32,
        "DOUBLE" => DataType::Float64,
        "BINARY" | "VARBINARY" | "BIT" | "TINYBLOB" | "MEDIUMBLOB" | "BLOB" | "LONGBLOB" => {
            DataType::Binary
        }
        "JSON" => DataType::JSON,
        // Just map ENUM/SET to strings
        "ENUM" | "SET" => DataType::Utf8String(StringOptions::default()),
        "DATE" => DataType::Date,
        "TIME" => DataType::Time,
        "DATETIME" => DataType::DateTime,
        "TIMESTAMP" => DataType::DateTimeWithTZ,
        "YEAR" => DataType::UInt16,
        // Default unknown data types to json
        _ => {
            warn!("Encountered unknown data type '{col_type}', defaulting to JSON seralisation");
            DataType::JSON
        }
    })
}

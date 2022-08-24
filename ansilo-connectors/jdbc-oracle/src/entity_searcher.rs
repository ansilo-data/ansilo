use std::collections::HashMap;

use ansilo_core::{
    config::{EntityAttributeConfig, EntityConfig, EntitySourceConfig, NodeConfig},
    data::{DataType, DataValue, DecimalOptions, StringOptions},
    err::{Context, Result},
};

use ansilo_connectors_base::interface::{Connection, EntitySearcher, QueryHandle, ResultSet};
use ansilo_connectors_jdbc_base::{JdbcConnection, JdbcDefaultTypeMapping, JdbcQuery};
use ansilo_logging::warn;
use itertools::Itertools;

use crate::OracleJdbcTableOptions;

use super::OracleJdbcEntitySourceConfig;

/// The entity searcher for Oracle JDBC
pub struct OracleJdbcEntitySearcher {}

impl EntitySearcher for OracleJdbcEntitySearcher {
    type TConnection = JdbcConnection<JdbcDefaultTypeMapping>;
    type TEntitySourceConfig = OracleJdbcEntitySourceConfig;

    fn discover(connection: &mut Self::TConnection, _nc: &NodeConfig) -> Result<Vec<EntityConfig>> {
        let cols = connection
            .prepare(JdbcQuery::new(
                r#"
                SELECT
                    T.OWNER,
                    T.TABLE_NAME,
                    C.COLUMN_NAME,
                    C.DATA_TYPE,
                    C.NULLABLE,
                    C.CHAR_LENGTH,
                    C.DATA_PRECISION,
                    C.DATA_SCALE
                FROM ALL_TABLES T
                INNER JOIN ALL_TAB_COLUMNS C ON T.OWNER = C.OWNER AND T.TABLE_NAME = C.TABLE_NAME
                WHERE 1=1
                AND T.TEMPORARY = 'N'
                AND T.NESTED = 'NO'
                AND T.DROPPED = 'NO'
                AND T.OWNER = 'ANSILO_ADMIN'
            "#,
                vec![],
            ))?
            .execute()?;

        let cols = cols.reader()?.iter_rows().collect::<Result<Vec<_>>>()?;
        let tables = cols.into_iter().into_group_map_by(|row| {
            (
                row["OWNER"].as_utf8_string().unwrap().clone(),
                row["TABLE_NAME"].as_utf8_string().unwrap().clone(),
            )
        });

        let entities = tables
            .into_iter()
            .filter_map(
                |((owner, table), cols)| match parse_entity_config(&owner, &table, cols) {
                    Ok(conf) => Some(conf),
                    Err(err) => {
                        warn!(
                            "Failed to import schema for table \"{}.{}\": {:?}",
                            owner, table, err
                        );
                        None
                    }
                },
            )
            .collect();

        Ok(entities)
    }
}

pub(crate) fn parse_entity_config(
    owner: &String,
    table: &String,
    cols: Vec<HashMap<String, DataValue>>,
) -> Result<EntityConfig> {
    Ok(EntityConfig::minimal(
        format!("{}.{}", owner.clone(), table.clone()),
        cols.into_iter()
            .map(|c| {
                Ok(EntityAttributeConfig::new(
                    c["COLUMN_NAME"]
                        .as_utf8_string()
                        .context("COLUMN_NAME")?
                        .clone(),
                    None,
                    from_oracle_type(&c)?,
                    false,
                    c["NULLABLE"].as_utf8_string().context("NULLABLE")? == "Y",
                ))
            })
            .collect::<Result<Vec<_>>>()?,
        EntitySourceConfig::from(OracleJdbcEntitySourceConfig::Table(
            OracleJdbcTableOptions::new(Some(owner.clone()), table.clone(), HashMap::new()),
        ))?,
    ))
}

pub(crate) fn from_oracle_type(col: &HashMap<String, DataValue>) -> Result<DataType> {
    let ora_type = col["DATA_TYPE"].as_utf8_string().context("DATA_TYPE")?;
    let normalised_type = ora_type
        .chars()
        .filter(|c| match c {
            'A'..='Z' => true,
            ' ' | '_' => true,
            _ => false,
        })
        .collect::<String>();
    Ok(match normalised_type.as_str() {
        "CHAR" | "NCHAR" | "VARCHAR" | "VARCHAR2" | "NVARCHAR" | "NVARCHAR2" | "CLOB" | "NCLOB" => {
            let length = col["CHAR_LENGTH"]
                .clone()
                .try_coerce_into(&DataType::UInt32)
                .ok()
                .and_then(|i| i.as_u_int32().cloned())
                .and_then(|i| if i >= 1 { Some(i) } else { None });

            DataType::Utf8String(StringOptions::new(length))
        }
        "NUMBER" | "FLOAT" => {
            let precision = col["DATA_PRECISION"]
                .clone()
                .try_coerce_into(&DataType::UInt16)
                .ok()
                .and_then(|i| i.as_u_int16().cloned());
            let scale = col["DATA_SCALE"]
                .clone()
                .try_coerce_into(&DataType::UInt16)
                .ok()
                .and_then(|i| i.as_u_int16().cloned());

            DataType::Decimal(DecimalOptions::new(precision, scale))
        }
        "BINARY_FLOAT" => DataType::Float32,
        "BINARY_DOUBLE" => DataType::Float64,
        "RAW" | "LONG RAW" | "BFILE" | "BLOB" => DataType::Binary,
        "JSON" => DataType::JSON,
        "DATE" => DataType::Date,
        "TIMESTAMP" => DataType::DateTime,
        "TIMESTAMP WITH TIME ZONE" | "TIMESTAMP WITH LOCAL TIME ZONE" => DataType::DateTimeWithTZ,
        // Default unknown data types to json
        _ => {
            warn!("Encountered unknown data type '{ora_type}', defaulting to JSON seralisation");
            DataType::JSON
        }
    })
}

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

use crate::OracleJdbcTableOptions;

use super::OracleJdbcEntitySourceConfig;

/// The entity searcher for Oracle JDBC
pub struct OracleJdbcEntitySearcher {}

impl EntitySearcher for OracleJdbcEntitySearcher {
    type TConnection = JdbcConnection;
    type TEntitySourceConfig = OracleJdbcEntitySourceConfig;

    fn discover(
        connection: &mut Self::TConnection,
        _nc: &NodeConfig,
        opts: EntityDiscoverOptions,
    ) -> Result<Vec<EntityConfig>> {
        // Query oracle's information schema tables to retrieve all column definitions
        // Importantly we order the results by table and then by column position
        // when lets us efficiently group the result by table using `group_by` below.
        // Additionally, we the results to be deterministic and return the columns
        // the user-defined order on the oracle side.
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
                    C.DATA_SCALE,
                    C.COLUMN_ID
                FROM (
                    SELECT T.OWNER, T.TABLE_NAME 
                    FROM ALL_TABLES T
                    WHERE T.TEMPORARY = 'N' AND T.NESTED = 'NO' AND T.DROPPED = 'NO'
                    UNION
                    SELECT T.OWNER, T.VIEW_NAME 
                    FROM ALL_VIEWS T
                    UNION
                    SELECT T.OWNER, T.MVIEW_NAME 
                    FROM ALL_MVIEWS T
                ) T
                INNER JOIN ALL_TAB_COLUMNS C ON T.OWNER = C.OWNER AND T.TABLE_NAME = C.TABLE_NAME
                WHERE (T.OWNER || '.' || T.TABLE_NAME) LIKE ?
                ORDER BY T.OWNER, T.TABLE_NAME, C.COLUMN_ID
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
                row["OWNER"].as_utf8_string().unwrap().clone(),
                row["TABLE_NAME"].as_utf8_string().unwrap().clone(),
            )
        });

        let entities = tables
            .into_iter()
            .filter_map(|((owner, table), cols)| {
                match parse_entity_config(&owner, &table, cols.into_iter()) {
                    Ok(conf) => Some(conf),
                    Err(err) => {
                        warn!(
                            "Failed to import schema for table \"{}.{}\": {:?}",
                            owner, table, err
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
    owner: &String,
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
        EntitySourceConfig::from(OracleJdbcEntitySourceConfig::Table(
            OracleJdbcTableOptions::new(Some(owner.clone()), table.clone(), HashMap::new()),
        ))?,
    ))
}

pub(crate) fn parse_column(
    name: &str,
    c: &HashMap<String, DataValue>,
) -> Result<EntityAttributeConfig> {
    let data_type = from_oracle_type(&c)?;

    Ok(EntityAttributeConfig::new(
        name.to_string(),
        None,
        data_type,
        false,
        c["NULLABLE"].as_utf8_string().context("NULLABLE")? == "Y",
    ))
}

pub(crate) fn from_oracle_type(col: &HashMap<String, DataValue>) -> Result<DataType> {
    let ora_type = col["DATA_TYPE"].as_utf8_string().context("DATA_TYPE")?;

    // Strip out type modifiers, eg "TIMESTAMP(6)" --> "TIMESTAMP"
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

            match (precision, scale) {
                (Some(0..=2), Some(0)) => DataType::Int8,
                (Some(0..=4), Some(0)) => DataType::Int16,
                (Some(0..=9), Some(0)) => DataType::Int32,
                (Some(0..=18), Some(0)) => DataType::Int64,
                _ => DataType::Decimal(DecimalOptions::new(precision, scale)),
            }
        }
        "BINARY_FLOAT" => DataType::Float32,
        "BINARY_DOUBLE" => DataType::Float64,
        "RAW" | "LONG RAW" | "LONG" | "BFILE" | "BLOB" => DataType::Binary,
        "JSON" => DataType::JSON,
        "DATE" => DataType::Date,
        "TIMESTAMP" => DataType::DateTime,
        "TIMESTAMP WITH TIME ZONE" | "TIMESTAMP WITH LOCAL TIME ZONE" => DataType::DateTimeWithTZ,
        _ => {
            bail!("Encountered unknown data type '{ora_type}'");
        }
    })
}

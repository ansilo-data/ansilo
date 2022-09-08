use std::{collections::HashMap, marker::PhantomData, ops::DerefMut};

use ansilo_core::{
    config::{EntityAttributeConfig, EntityConfig, EntitySourceConfig, NodeConfig},
    data::{DataType, DataValue, DecimalOptions, StringOptions},
    err::{bail, Context, Result},
};

use ansilo_connectors_base::{
    common::query::QueryParam,
    interface::{Connection, EntityDiscoverOptions, EntitySearcher, QueryHandle, ResultSet},
};
use ansilo_logging::warn;
use itertools::Itertools;
use tokio_postgres::Client;

use crate::{PostgresConnection, PostgresQuery, PostgresTableOptions};

use super::PostgresEntitySourceConfig;

/// The entity searcher for Postgres
pub struct PostgresEntitySearcher<T> {
    _data: PhantomData<T>,
}

impl<T: DerefMut<Target = Client>> EntitySearcher for PostgresEntitySearcher<T> {
    type TConnection = PostgresConnection<T>;
    type TEntitySourceConfig = PostgresEntitySourceConfig;

    fn discover(
        connection: &mut Self::TConnection,
        _nc: &NodeConfig,
        opts: EntityDiscoverOptions,
    ) -> Result<Vec<EntityConfig>> {
        // Query postgres's information schema tables to retrieve all column definitions
        // Importantly we order the results by table and then by column position
        // when lets us efficiently group the result by table using `group_by` below.
        // Additionally, we the results to be deterministic and return the columns
        // the user-defined order on the oracle side.
        let mut query = connection
            .prepare(PostgresQuery::new(
                r#"
                SELECT * FROM (
                    SELECT
                        t.table_schema,
                        t.table_name,
                        c.column_name,
                        c.is_identity,
                        c.data_type,
                        c.is_nullable,
                        c.character_maximum_length,
                        c.numeric_precision,
                        c.numeric_scale,
                        c.ordinal_position
                    FROM information_schema.tables t
                    INNER JOIN information_schema.columns C ON t.table_schema = c.table_schema AND t.table_name = c.table_name
                    WHERE 1=1
                    AND concat(t.table_schema, '.', t.table_name) LIKE $1
                    AND t.table_type != 'LOCAL TEMPORARY'
                    UNION ALL
                    -- Include materialised views
                    SELECT 
                        s.nspname as table_schema, 
                        t.relname as table_name,
                        a.attname as column_name,
                        'NO' AS is_identity,
                        pg_catalog.format_type(a.atttypid, NULL) as data_type,
                        CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END as is_nullable,
                        information_schema._pg_char_max_length(a.atttypid, a.atttypmod) as character_maximum_length,
                        information_schema._pg_numeric_precision(a.atttypid, a.atttypmod) as numeric_precision,
                        information_schema._pg_numeric_scale(a.atttypid, a.atttypmod) as numeric_scale,
                        CAST(a.attnum AS information_schema.cardinal_number) AS ordinal_position
                    FROM pg_attribute a
                    INNER JOIN pg_class t on a.attrelid = t.oid
                    INNER JOIN pg_namespace s on t.relnamespace = s.oid
                    WHERE 1=1
                    AND t.relkind = 'm'
                    AND a.attnum > 0 
                    AND NOT a.attisdropped
                    AND concat(s.nspname, '.', t.relname) LIKE $1
                ) AS a
                ORDER BY a.table_schema, a.table_name, a.ordinal_position
            "#,
                vec![QueryParam::Constant(
                    DataValue::Utf8String(
                        opts.remote_schema
                            .as_ref()
                            .map(|i| i.as_str())
                            .unwrap_or("%")
                            .into(),
                    )
                )],
            ))?;

        let cols = query.execute_query()?;

        let cols = cols.reader()?.iter_rows().collect::<Result<Vec<_>>>()?;
        let tables = cols.into_iter().group_by(|row| {
            (
                row["table_schema"].as_utf8_string().unwrap().clone(),
                row["table_name"].as_utf8_string().unwrap().clone(),
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
        table.clone(),
        cols.map(|c| {
            Ok(EntityAttributeConfig::new(
                c["column_name"]
                    .as_utf8_string()
                    .context("column_name")?
                    .clone(),
                None,
                from_postgres_type(&c)?,
                c["is_identity"].as_utf8_string().context("is_identity")? == "YES",
                c["is_nullable"].as_utf8_string().context("is_nullable")? == "YES",
            ))
        })
        .collect::<Result<Vec<_>>>()?,
        EntitySourceConfig::from(PostgresEntitySourceConfig::Table(
            PostgresTableOptions::new(Some(db.clone()), table.clone(), HashMap::new()),
        ))?,
    ))
}

pub(crate) fn from_postgres_type(col: &HashMap<String, DataValue>) -> Result<DataType> {
    let data_type = &col["data_type"]
        .as_utf8_string()
        .context("data_type")?
        .to_uppercase();

    Ok(match data_type.as_str() {
        "CHAR" | "CHARACTER" | "TEXT" | "VARCHAR" | "CITEXT" | "NAME" | "UNKNOWN"
        | "CHARACTER VARYING" => {
            let length = col["character_maximum_length"]
                .clone()
                .try_coerce_into(&DataType::UInt32)
                .ok()
                .and_then(|i| i.as_u_int32().cloned())
                .and_then(|i| if i >= 1 { Some(i) } else { None });

            DataType::Utf8String(StringOptions::new(length))
        }
        "BOOLEAN" | "BIT" => DataType::Boolean,
        "\"CHAR\"" => DataType::Int8,
        "SMALLINT" => DataType::Int16,
        "INTEGER" => DataType::Int32,
        "BIGINT" => DataType::Int64,
        "NUMERIC" => {
            let precision = col["numeric_precision"]
                .clone()
                .try_coerce_into(&DataType::UInt16)
                .ok()
                .and_then(|i| i.as_u_int16().cloned());
            let scale = col["numeric_scale"]
                .clone()
                .try_coerce_into(&DataType::UInt16)
                .ok()
                .and_then(|i| i.as_u_int16().cloned());

            DataType::Decimal(DecimalOptions::new(precision, scale))
        }

        "FLOAT4" | "REAL" => DataType::Float32,
        "FLOAT8" | "DOUBLE" | "DOUBLE PRECISION" => DataType::Float64,
        "BYTEA" | "VARBINARY" | "TINYBLOB" | "MEDIUMBLOB" | "BLOB" => DataType::Binary,
        "JSON" | "JSONB" => DataType::JSON,
        "DATE" => DataType::Date,
        "TIME" | "TIME WITHOUT TIME ZONE" => DataType::Time,
        "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => DataType::DateTime,
        "TIMESTAMP WITH TIME ZONE" => DataType::DateTimeWithTZ,
        "UUID" => DataType::Uuid,
        // Default unknown data types to json
        _ => {
            bail!("Encountered unsupported data type '{data_type}'");
        }
    })
}

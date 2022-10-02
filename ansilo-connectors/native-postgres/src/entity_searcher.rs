use std::{collections::HashMap, marker::PhantomData, ops::DerefMut};

use ansilo_core::{
    config::{EntityAttributeConfig, EntityConfig, EntitySourceConfig, NodeConfig},
    data::{DataType, DecimalOptions, StringOptions},
    err::{bail, Context, Result},
};

use ansilo_connectors_base::interface::{EntityDiscoverOptions, EntitySearcher};
use ansilo_logging::warn;
use itertools::Itertools;
use tokio_postgres::{Client, Row};

use crate::{runtime, PostgresConnection, PostgresTableOptions};

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
        runtime().block_on(Self::discover_async(connection.client(), opts))
    }
}

impl<T: DerefMut<Target = Client>> PostgresEntitySearcher<T> {
    pub async fn discover_async(
        connection: &Client,
        opts: EntityDiscoverOptions,
    ) -> Result<Vec<EntityConfig>> {
        // Query postgres's information schema tables to retrieve all column definitions
        // Importantly we order the results by table and then by column position
        // when lets us efficiently group the result by table using `group_by` below.
        // Additionally, we the results to be deterministic and return the columns
        // the user-defined order on the oracle side.
        let rows = connection
            .query(
                r#"
                SELECT * FROM (
                    SELECT
                        t.table_schema,
                        t.table_name,
                        pg_catalog.obj_description(format('"%s"."%s"', t.table_schema, t.table_name)::regclass::oid, 'pg_class') as table_description,
                        c.column_name,
                        c.is_identity,
                        c.data_type,
                        c.is_nullable,
                        c.character_maximum_length,
                        c.numeric_precision,
                        c.numeric_scale,
                        c.ordinal_position,
                        pg_catalog.col_description(format('"%s"."%s"', t.table_schema, t.table_name)::regclass::oid, c.ordinal_position) as column_description
                    FROM information_schema.tables t
                    INNER JOIN information_schema.columns C ON t.table_schema = c.table_schema AND t.table_name = c.table_name
                    WHERE 1=1
                    AND concat(t.table_schema, '.', t.table_name) LIKE $1
                    AND t.table_type != 'LOCAL TEMPORARY'
                    AND NOT (t.table_schema = ANY($2))
                    UNION ALL
                    -- Include materialised views
                    SELECT 
                        s.nspname as table_schema, 
                        t.relname as table_name,
                        pg_catalog.obj_description(t.oid, 'pg_class') as table_description,
                        a.attname as column_name,
                        'NO' AS is_identity,
                        pg_catalog.format_type(a.atttypid, NULL) as data_type,
                        CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END as is_nullable,
                        information_schema._pg_char_max_length(a.atttypid, a.atttypmod) as character_maximum_length,
                        information_schema._pg_numeric_precision(a.atttypid, a.atttypmod) as numeric_precision,
                        information_schema._pg_numeric_scale(a.atttypid, a.atttypmod) as numeric_scale,
                        CAST(a.attnum AS information_schema.cardinal_number) AS ordinal_position,
                        pg_catalog.col_description(t.oid, CAST(a.attnum AS information_schema.cardinal_number)) as column_description
                    FROM pg_attribute a
                    INNER JOIN pg_class t on a.attrelid = t.oid
                    INNER JOIN pg_namespace s on t.relnamespace = s.oid
                    WHERE 1=1
                    AND t.relkind = 'm'
                    AND a.attnum > 0 
                    AND NOT a.attisdropped
                    AND concat(s.nspname, '.', t.relname) LIKE $1
                    AND NOT (s.nspname = ANY($2))
                ) AS a
                ORDER BY a.table_schema, a.table_name, a.ordinal_position
            "#,
            &[
                    &opts.remote_schema
                        .as_ref()
                        .map(|i| i.as_str())
                        .unwrap_or("%"),
                    &opts.other.get("exclude_internal").map_or_else(
                        || vec![],
                        |_| vec!["information_schema", "pg_catalog", "ansilo_catalog"]
                    )
                ],
            ).await?;

        let tables = rows.into_iter().group_by(|row| {
            (
                row.get::<_, String>("table_schema"),
                row.get::<_, String>("table_name"),
            )
        });

        let entities = tables
            .into_iter()
            .filter_map(|((schema, table), cols)| {
                match parse_entity_config(&schema, &table, cols.collect_vec(), &opts) {
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
    cols: Vec<Row>,
    opts: &EntityDiscoverOptions,
) -> Result<EntityConfig> {
    let id = if opts.other.contains_key("include_schema_in_id") {
        format!("{}.{}", schema, table)
    } else {
        table.clone()
    };

    Ok(EntityConfig::new(
        id,
        None,
        cols[0]
            .try_get("table_description")
            .context("table_description")?,
        vec![],
        cols.into_iter()
            .filter_map(|c| {
                let name: String = c
                    .try_get("column_name")
                    .map_err(|e| warn!("Failed to parse column name: {:?}", e))
                    .ok()?;
                parse_column(name.as_str(), c)
                    .map_err(|e| warn!("Ignoring column '{}': {:?}", name, e))
                    .ok()
            })
            .collect(),
        vec![],
        EntitySourceConfig::from(PostgresEntitySourceConfig::Table(
            PostgresTableOptions::new(Some(schema.clone()), table.clone(), HashMap::new()),
        ))?,
    ))
}

fn parse_column(name: &str, c: Row) -> Result<EntityAttributeConfig, ansilo_core::err::Error> {
    Ok(EntityAttributeConfig::new(
        name.to_string(),
        c.try_get("column_description")
            .context("column_description")?,
        from_postgres_type(&c)?,
        c.try_get::<_, String>("is_identity")
            .context("is_identity")?
            == "YES",
        c.try_get::<_, String>("is_nullable")
            .context("is_nullable")?
            == "YES",
    ))
}

pub(crate) fn from_postgres_type(col: &Row) -> Result<DataType> {
    let data_type = &col
        .try_get::<_, String>("data_type")
        .context("data_type")?
        .to_uppercase();

    Ok(match data_type.as_str() {
        "CHAR" | "CHARACTER" | "TEXT" | "VARCHAR" | "CITEXT" | "NAME" | "UNKNOWN"
        | "CHARACTER VARYING" => {
            let length = col
                .try_get::<_, Option<i32>>("character_maximum_length")
                .context("character_maximum_length")?
                .and_then(|i| if i >= 1 { Some(i) } else { None });

            DataType::Utf8String(StringOptions::new(length.map(|i| i as _)))
        }
        "BOOLEAN" | "BIT" => DataType::Boolean,
        "\"CHAR\"" => DataType::Int8,
        "SMALLINT" => DataType::Int16,
        "INTEGER" => DataType::Int32,
        "BIGINT" => DataType::Int64,
        "NUMERIC" => {
            let precision = col
                .try_get::<_, Option<i32>>("numeric_precision")
                .context("numeric_precision")?;
            let scale = col
                .try_get::<_, Option<i32>>("numeric_scale")
                .context("numeric_scale")?;

            DataType::Decimal(DecimalOptions::new(
                precision.map(|i| i as _),
                scale.map(|i| i as _),
            ))
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
        _ => {
            bail!("Encountered unsupported data type '{data_type}'");
        }
    })
}

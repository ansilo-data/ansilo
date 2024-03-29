use std::ffi::c_void;

use ansilo_core::{
    config::{EntityAttributeConfig, EntityConfig, EntitySourceConfig},
    err::{bail, Context, Result},
    sqlil,
};

use cstr::cstr;
use pgx::{
    pg_sys::{get_rel_name, strcmp, DefElem, GetForeignServer, GetForeignTable, Oid},
    *,
};

use crate::{
    fdw::common::ServerOptions,
    util::{
        def_elem::{def_get_owned_utf8_string, parse_def_elems_to_hash_map},
        string::parse_to_owned_utf8_string,
        syscache::PgSysCacheItem,
        table::PgTable,
    },
};

use super::from_pg_type;

extension_sql!(
    r#"
    CREATE FUNCTION __ansilo_private."get_entity_config"(
        "foreign_table_oid" oid
    ) RETURNS jsonb
    VOLATILE PARALLEL UNSAFE STRICT
    LANGUAGE c /* Rust */
    AS 'MODULE_PATHNAME', 'get_entity_config_wrapper';
    "#,
    name = "get_entity_config",
    requires = ["ansilo_private_schema"]
);

/// Retrieves the entity config as a JSON payload
/// This is used to get a consistent view
/// of the entity when exposing the data catalog.
#[pg_extern(sql = "")]
unsafe fn get_entity_config(foreign_table_oid: Oid) -> Option<JsonB> {
    {
        // First check if this oid is actually referencing a valid foreign table
        // pg_sys does not have the appropriate type for Form_pg_foreign_table
        // but no matter, we just need to check if the point is valid or not.
        let table = PgSysCacheItem::<c_void>::search(
            pg_sys::SysCacheIdentifier_FOREIGNTABLEREL,
            [foreign_table_oid.into_datum().unwrap()],
        );

        if table.is_none() {
            return None;
        }
    }

    let config = entity_config_from_foreign_table(foreign_table_oid).unwrap();

    Some(JsonB(serde_json::to_value(config).unwrap()))
}

pub(crate) unsafe fn get_entity_id_from_foreign_table(
    foreign_table_oid: Oid,
) -> Result<sqlil::EntityId> {
    let foreign_table = GetForeignTable(foreign_table_oid);
    parse_entity_id_from_rel((*foreign_table).relid)
}

pub(crate) unsafe fn parse_entity_id_from_rel(relid: Oid) -> Result<sqlil::EntityId> {
    // First check if there is a table option defining the table name
    let foreign_table = GetForeignTable(relid);

    if foreign_table.is_null() {
        bail!("Invalid foreign table oid");
    }

    let options = PgList::<DefElem>::from_pg((*foreign_table).options);

    for opt in options.iter_ptr() {
        if strcmp((*opt).defname, cstr!("entity_id").as_ptr()) == 0 {
            return Ok(sqlil::EntityId::new(def_get_owned_utf8_string(opt)?));
        }
    }

    // Secondly, if there is not explicit option, use the name of the table
    let table_name = {
        let name = get_rel_name(relid);
        parse_to_owned_utf8_string(name).context("Failed to get table name")?
    };

    Ok(sqlil::EntityId::new(table_name))
}

/// Reverse engineers a postgres table definition into an EntityConfig.
pub(crate) unsafe fn entity_config_from_foreign_table(
    foreign_table_oid: Oid,
) -> Result<EntityConfig> {
    let foreign_table = GetForeignTable(foreign_table_oid);
    let server = GetForeignServer((*foreign_table).serverid);
    let options = PgList::<DefElem>::from_pg((*foreign_table).options);

    // In the case of tables created via IMPORT FOREIGN SCHEMA, they will likely
    // contain the "__config" options containing a yaml-serialised string of the
    // config describing how to pull the data from the data sources. In this case
    // it is generated by the connector.
    if let Some(opt) = options
        .iter_ptr()
        .find(|i| strcmp((**i).defname, cstr!("__config").as_ptr()) == 0)
    {
        return serde_yaml::from_str::<EntityConfig>(&def_get_owned_utf8_string(opt)?)
            .context("Failed to parse __config option as yaml");
    }

    // Alternatively, these could be user-defined tables table are also used to
    // reference external data. So we support users defining the connector config
    // using table options and map the key-values to the entity config.
    let server_options = ServerOptions::parse(PgList::<DefElem>::from_pg((*server).options))?;

    let table = PgTable::open((*foreign_table).relid as _, pg_sys::NoLock as _)
        .context("Could not find relation from foreign table relid")?;

    // Map the attribute configuration from table columns
    let attrs = table
        .attrs()
        .map(|a| {
            let opts = PgList::<DefElem>::from_pg(pg_sys::GetForeignColumnOptions(
                table.relid(),
                a.attnum,
            ));
            let opts = parse_def_elems_to_hash_map(opts).unwrap();

            Ok(EntityAttributeConfig::new(
                a.name().to_string(),
                None,
                from_pg_type((*a).atttypid)?,
                opts.get("primary_key") == Some(&"true".into()),
                !(*a).attnotnull,
            ))
        })
        .collect::<Result<Vec<_>>>()
        .context("Failed to map table columns")?;

    let source_config = if !options.is_empty() {
        serde_yaml::Value::Mapping(
            options
                .iter_ptr()
                .map(|i| {
                    Ok((
                        parse_to_owned_utf8_string((*i).defname)?,
                        def_get_owned_utf8_string(i)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .map(|(k, v)| (serde_yaml::Value::String(k), serde_yaml::Value::String(v)))
                .collect(),
        )
    } else {
        // No config found for table, just return null
        serde_yaml::Value::Null
    };

    Ok(EntityConfig::new(
        table.name().to_string(),
        None,
        None,
        vec![],
        attrs,
        vec![],
        EntitySourceConfig::new(server_options.data_source, source_config),
    ))
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

    use pretty_assertions::assert_eq;

    use ansilo_core::data::*;

    fn get_table_oid(client: &mut SpiClient, table_name: &str) -> Oid {
        client
            .select(
                &format!(r#"SELECT '"{}"'::regclass::oid"#, table_name),
                None,
                None,
            )
            .next()
            .unwrap()
            .by_ordinal(1)
            .unwrap()
            .value::<Oid>()
            .unwrap()
    }

    #[pg_test]
    fn test_fdw_common_parse_entity_id_from_foreign_table() {
        let oid = Spi::connect(|mut client| {
            client.update(
                r#"CREATE SERVER IF NOT EXISTS test_srv FOREIGN DATA WRAPPER ansilo_fdw"#,
                None,
                None,
            );
            client.update(
                r#"CREATE FOREIGN TABLE IF NOT EXISTS "example_entity" (col INTEGER) SERVER test_srv"#,
                None,
                None,
            );

            Ok(Some(get_table_oid(&mut client, "example_entity")))
        })
        .unwrap();

        let entity = unsafe { get_entity_id_from_foreign_table(oid).unwrap() };

        assert_eq!(entity, sqlil::entity("example_entity"));
    }

    #[pg_test]
    fn parse_entity_id_from_foreign_table_with_explicit_option() {
        let oid = Spi::connect(|mut client| {
            client.update(
                r#"CREATE SERVER IF NOT EXISTS test_srv FOREIGN DATA WRAPPER ansilo_fdw"#,
                None,
                None,
            );
            client.update(
                r#"
                CREATE FOREIGN TABLE IF NOT EXISTS "example_entity" 
                (col INTEGER) 
                SERVER test_srv 
                OPTIONS (entity_id 'my_entity')
                "#,
                None,
                None,
            );

            Ok(Some(get_table_oid(&mut client, "example_entity")))
        })
        .unwrap();

        let entity = unsafe { get_entity_id_from_foreign_table(oid).unwrap() };

        assert_eq!(entity, sqlil::entity("my_entity"));
    }

    #[pg_test]
    fn test_fdw_common_parse_entity_config_from_foreign_table() {
        let oid = Spi::connect(|mut client| {
            client.update(
                r#"
                CREATE SERVER IF NOT EXISTS test_srv 
                FOREIGN DATA WRAPPER ansilo_fdw 
                OPTIONS (data_source 'source', socket 'unused')
                "#,
                None,
                None,
            );
            client.update(
                r#"
                CREATE FOREIGN TABLE IF NOT EXISTS "example_entity" 
                (
                    col_a INTEGER, 
                    col_b VARCHAR(255),
                    col_not_null NUMERIC NOT NULL
                ) 
                SERVER test_srv
                "#,
                None,
                None,
            );

            Ok(Some(get_table_oid(&mut client, "example_entity")))
        })
        .unwrap();

        let entity = unsafe { entity_config_from_foreign_table(oid).unwrap() };

        assert_eq!(
            entity,
            EntityConfig::new(
                "example_entity".into(),
                None,
                None,
                vec![],
                vec![
                    EntityAttributeConfig::new("col_a".into(), None, DataType::Int32, false, true),
                    EntityAttributeConfig::new(
                        "col_b".into(),
                        None,
                        DataType::Utf8String(StringOptions::default()),
                        false,
                        true
                    ),
                    EntityAttributeConfig::new(
                        "col_not_null".into(),
                        None,
                        DataType::Decimal(DecimalOptions::default()),
                        false,
                        false
                    )
                ],
                vec![],
                EntitySourceConfig::new("source".into(), serde_yaml::Value::Null),
            )
        );
    }

    #[pg_test]
    fn parse_entity_config_from_foreign_table_with_serialized_config() {
        let conf = EntityConfig::minimal(
            "test_entity",
            vec![EntityAttributeConfig::minimal("test_col", DataType::Int32)],
            EntitySourceConfig::minimal("source"),
        );
        let oid = Spi::connect(|mut client| {
            client.update(
                r#"
                CREATE SERVER IF NOT EXISTS test_srv 
                FOREIGN DATA WRAPPER ansilo_fdw 
                OPTIONS (data_source 'source', socket 'unused')
                "#,
                None,
                None,
            );
            client.update(
                &format!(
                    r#"
                CREATE FOREIGN TABLE IF NOT EXISTS "example_entity" 
                (
                    col_a INTEGER
                ) 
                SERVER test_srv
                OPTIONS (
                    __config '{}'
                )
                "#,
                    serde_yaml::to_string(&conf).unwrap()
                ),
                None,
                None,
            );

            Ok(Some(get_table_oid(&mut client, "example_entity")))
        })
        .unwrap();

        let entity = unsafe { entity_config_from_foreign_table(oid).unwrap() };

        assert_eq!(entity, conf);
    }

    #[pg_test]
    fn parse_entity_config_from_foreign_table_with_user_defined_config() {
        let oid = Spi::connect(|mut client| {
            client.update(
                r#"
                CREATE SERVER IF NOT EXISTS test_srv 
                FOREIGN DATA WRAPPER ansilo_fdw 
                OPTIONS (data_source 'source', socket 'unused')
                "#,
                None,
                None,
            );
            client.update(
                r#"
                CREATE FOREIGN TABLE IF NOT EXISTS "example_entity" 
                (
                    col_a INTEGER
                ) 
                SERVER test_srv
                OPTIONS (
                    foo 'bar',
                    baz 'qux'
                )
                "#,
                None,
                None,
            );

            Ok(Some(get_table_oid(&mut client, "example_entity")))
        })
        .unwrap();

        let entity = unsafe { entity_config_from_foreign_table(oid).unwrap() };

        assert_eq!(
            entity,
            EntityConfig::new(
                "example_entity".into(),
                None,
                None,
                vec![],
                vec![EntityAttributeConfig::new(
                    "col_a".into(),
                    None,
                    DataType::Int32,
                    false,
                    true
                ),],
                vec![],
                EntitySourceConfig::new(
                    "source".into(),
                    serde_yaml::Value::Mapping(
                        vec![
                            (
                                serde_yaml::Value::String("foo".into()),
                                serde_yaml::Value::String("bar".into()),
                            ),
                            (
                                serde_yaml::Value::String("baz".into()),
                                serde_yaml::Value::String("qux".into()),
                            )
                        ]
                        .into_iter()
                        .collect()
                    )
                ),
            )
        );
    }

    #[pg_test]
    fn test_fdw_common_parse_entity_config_primary_key_cols() {
        let oid = Spi::connect(|mut client| {
            client.update(
                r#"
                CREATE SERVER IF NOT EXISTS test_srv 
                FOREIGN DATA WRAPPER ansilo_fdw 
                OPTIONS (data_source 'source', socket 'unused')
                "#,
                None,
                None,
            );
            client.update(
                r#"
                CREATE FOREIGN TABLE IF NOT EXISTS "example_entity" 
                (
                    col_a INTEGER OPTIONS (primary_key 'true'), 
                    col_b VARCHAR(255)
                ) 
                SERVER test_srv
                "#,
                None,
                None,
            );

            Ok(Some(get_table_oid(&mut client, "example_entity")))
        })
        .unwrap();

        let entity = unsafe { entity_config_from_foreign_table(oid).unwrap() };

        assert_eq!(
            entity,
            EntityConfig::new(
                "example_entity".into(),
                None,
                None,
                vec![],
                vec![
                    EntityAttributeConfig::new("col_a".into(), None, DataType::Int32, true, true),
                    EntityAttributeConfig::new(
                        "col_b".into(),
                        None,
                        DataType::Utf8String(StringOptions::default()),
                        false,
                        true
                    ),
                ],
                vec![],
                EntitySourceConfig::new("source".into(), serde_yaml::Value::Null),
            )
        );
    }

    #[pg_test]
    fn test_fdw_common_get_entity_config_on_table() {
        let entity: JsonB = Spi::connect(|client| {
            client.update(
                r#"
                CREATE SERVER IF NOT EXISTS test_srv 
                FOREIGN DATA WRAPPER ansilo_fdw 
                OPTIONS (data_source 'source', socket 'unused')
                "#,
                None,
                None,
            );
            client.update(
                r#"
                CREATE FOREIGN TABLE IF NOT EXISTS "example_entity" 
                (
                    col_a INTEGER
                ) 
                SERVER test_srv
                "#,
                None,
                None,
            );

            Ok(client
                .select(
                    "SELECT __ansilo_private.get_entity_config('example_entity'::regclass)",
                    Some(1),
                    None,
                )
                .first()
                .get_one::<JsonB>())
        })
        .unwrap();

        let entity: EntityConfig = serde_json::from_value(entity.0).unwrap();

        assert_eq!(
            entity,
            EntityConfig::new(
                "example_entity".into(),
                None,
                None,
                vec![],
                vec![EntityAttributeConfig::new(
                    "col_a".into(),
                    None,
                    DataType::Int32,
                    false,
                    true
                ),],
                vec![],
                EntitySourceConfig::new("source".into(), serde_yaml::Value::Null),
            )
        );
    }

    #[pg_test]
    fn test_get_entity_config_on_base_table_oid_returns_null() {
        let entity = Spi::connect(|client| {
            client.update(
                r#"
                CREATE TABLE IF NOT EXISTS "example_base_table" 
                (
                    col_a INTEGER
                ) 
                "#,
                None,
                None,
            );

            Ok(client
                .select(
                    "SELECT __ansilo_private.get_entity_config('example_base_table'::regclass)",
                    Some(1),
                    None,
                )
                .first()
                .get_one::<JsonB>())
        });

        assert!(entity.is_none());
    }

    #[pg_test]
    fn test_fdw_common_get_entity_config_invalid_oid_returns_null() {
        let entity = Spi::connect(|client| {
            Ok(client
                .select(
                    "SELECT __ansilo_private.get_entity_config(1234567)",
                    Some(1),
                    None,
                )
                .first()
                .get_one::<JsonB>())
        });

        assert!(entity.is_none());
    }
}

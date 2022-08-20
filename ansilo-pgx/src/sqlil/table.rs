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
        def_elem::def_get_owned_utf8_string, string::parse_to_owned_utf8_string, table::PgTable,
    },
};

use super::from_pg_type;

pub(crate) unsafe fn get_entity_id_from_foreign_table(
    foreign_table_oid: Oid,
) -> Result<sqlil::EntityId> {
    let foreign_table = GetForeignTable(foreign_table_oid);
    parse_entity_id_from_rel((*foreign_table).relid)
}

pub(crate) unsafe fn parse_entity_id_from_rel(relid: Oid) -> Result<sqlil::EntityId> {
    let table_name = {
        let name = get_rel_name(relid);
        parse_to_owned_utf8_string(name).context("Failed to get table name")?
    };

    Ok(sqlil::EntityId::new(table_name))
}

pub(crate) unsafe fn entity_config_from_foreign_table(
    foreign_table_oid: Oid,
) -> Result<EntityConfig> {
    let foreign_table = GetForeignTable(foreign_table_oid);
    let server = GetForeignServer((*foreign_table).serverid);
    let options = PgList::<DefElem>::from_pg((*foreign_table).options);

    let server_options = ServerOptions::parse(PgList::<DefElem>::from_pg((*server).options))?;

    let table = PgTable::open((*foreign_table).relid as _, pg_sys::NoLock as _)
        .context("Could not find relation from foreign table relid")?;

    let attrs = table
        .attrs()
        .map(|a| {
            Ok(EntityAttributeConfig::new(
                a.name().to_string(),
                None,
                from_pg_type((*a).atttypid)?,
                false,
                (*a).attnotnull,
            ))
        })
        .collect::<Result<Vec<_>>>()
        .context("Failed to map table columns")?;

    let source_config = if let Some(opt) = options
        .iter_ptr()
        .find(|i| strcmp((**i).defname, cstr!("__config").as_ptr()) == 0)
    {
        serde_yaml::to_value(def_get_owned_utf8_string(opt)?)
            .context("Failed to parse __config option as yaml")?
    } else {
        serde_yaml::Value::Null
    };

    Ok(EntityConfig::new(
        table.name().to_string(),
        Some(table.name().to_string()),
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

    use ansilo_core::data::DataType;
    use ansilo_core::data::StringOptions;

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

            let oid = client
                .select(
                    r#"SELECT '"example_entity"'::regclass::oid"#,
                    None,
                    None,
                )
                .next()
                .unwrap()
                .by_ordinal(1)
                .unwrap()
                .value::<Oid>()
                .unwrap();

            Ok(Some(oid))
        })
        .unwrap();

        unsafe {
            let entity = get_entity_id_from_foreign_table(oid).unwrap();

            assert_eq!(entity, sqlil::entity("example_entity"));
        }
    }

    #[pg_test]
    fn test_fdw_common_parse_entity_config_from_foreign_table() {
        let oid = Spi::connect(|mut client| {
            client.update(
                r#"CREATE SERVER IF NOT EXISTS test_srv FOREIGN DATA WRAPPER ansilo_fdw OPTIONS (data_source 'source', socket 'unused')"#,
                None,
                None,
            );
            client.update(
                r#"CREATE FOREIGN TABLE IF NOT EXISTS "example_entity" (col_a INTEGER, col_b VARCHAR(255)) SERVER test_srv"#,
                None,
                None,
            );

            let oid = client
                .select(
                    r#"SELECT '"example_entity"'::regclass::oid"#,
                    None,
                    None,
                )
                .next()
                .unwrap()
                .by_ordinal(1)
                .unwrap()
                .value::<Oid>()
                .unwrap();

            Ok(Some(oid))
        })
        .unwrap();

        unsafe {
            let entity = entity_config_from_foreign_table(oid).unwrap();

            assert_eq!(
                entity,
                EntityConfig::new(
                    "example_entity".into(),
                    Some("example_entity".into()),
                    None,
                    vec![],
                    vec![
                        EntityAttributeConfig::new(
                            "col_a".into(),
                            None,
                            DataType::Int32,
                            false,
                            false,
                        ),
                        EntityAttributeConfig::new(
                            "col_b".into(),
                            None,
                            DataType::Utf8String(StringOptions::default()),
                            false,
                            false,
                        )
                    ],
                    vec![],
                    EntitySourceConfig::new("source".into(), serde_yaml::Value::Null),
                )
            );
        }
    }
}

use ansilo_core::{
    err::{Context, Result},
    sqlil,
};

use pgx::{
    pg_sys::{get_rel_name, GetForeignTable, Oid},
    *,
};

use crate::util::string::parse_to_owned_utf8_string;

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

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

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
}

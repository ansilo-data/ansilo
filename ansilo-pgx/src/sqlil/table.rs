use ansilo_core::{
    err::{bail, Context, Result},
    sqlil,
};

use pgx::{
    pg_sys::{get_rel_name, GetForeignTable, Oid},
    *,
};

use crate::util::string::parse_to_owned_utf8_string;

pub(crate) unsafe fn parse_entity_version_id_from_foreign_table(
    foreign_table_oid: Oid,
) -> Result<sqlil::EntityVersionIdentifier> {
    let foreign_table = GetForeignTable(foreign_table_oid);
    parse_entity_version_id_from_rel((*foreign_table).relid)
}

pub(crate) unsafe fn parse_entity_version_id_from_rel(
    relid: Oid,
) -> Result<sqlil::EntityVersionIdentifier> {
    let table_name = {
        let name = get_rel_name(relid);
        parse_to_owned_utf8_string(name).context("Failed to get table name")?
    };

    parse_entity_version_id(table_name)
}

pub(crate) fn parse_entity_version_id(
    table_name: impl Into<String>,
) -> Result<sqlil::EntityVersionIdentifier> {
    let table_name: String = table_name.into();
    let mut parts = table_name.split(':');
    let entity_id = parts.next().context("Table name cannot be empty")?;
    let version_id = parts.next().unwrap_or("latest");

    if entity_id.is_empty() {
        bail!("Entity id cannot be empty string");
    }

    if version_id.is_empty() {
        bail!("Entity id cannot be empty string");
    }

    Ok(sqlil::entity(entity_id, version_id))
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
                r#"CREATE FOREIGN TABLE IF NOT EXISTS "example_entity:1.0.0" (col INTEGER) SERVER test_srv"#,
                None,
                None,
            );

            let oid = client
                .select(
                    r#"SELECT '"example_entity:1.0.0"'::regclass::oid"#,
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
            let entity = parse_entity_version_id_from_foreign_table(oid).unwrap();

            assert_eq!(entity, sqlil::entity("example_entity", "1.0.0"));
        }
    }
}

#[cfg(test)]
mod pg_tests {
    use super::*;

    #[test]
    fn test_fdw_common_parse_entity_id() {
        assert_eq!(
            parse_entity_version_id("entity:version").unwrap(),
            sqlil::entity("entity", "version")
        );
        assert_eq!(
            parse_entity_version_id("entity").unwrap(),
            sqlil::entity("entity", "latest")
        );
        parse_entity_version_id(":").unwrap_err();
        parse_entity_version_id("entity:").unwrap_err();
        parse_entity_version_id(":version").unwrap_err();
    }
}

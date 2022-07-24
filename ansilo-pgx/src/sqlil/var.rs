use ansilo_core::{
    err::{bail, Context, Result},
    sqlil,
};
use pgx::{pg_schema, pg_sys};

use crate::{fdw::ctx::{FdwContext, PlannerContext}, util::string::parse_to_owned_utf8_string};

use super::{
    datum::from_pg_type, table::parse_entity_version_id_from_foreign_table, ConversionContext,
};

pub(super) unsafe fn convert_var(
    node: *const pg_sys::Var,
    ctx: &mut ConversionContext,
    planner: &PlannerContext,
    _fdw: &FdwContext,
) -> Result<sqlil::Expr> {
    // @see https://doxygen.postgresql.org/deparse_8c_source.html#l02667 (deparseVar)

    let rel = planner
        .get_scan_rel()
        .context("Failed to find base/join rel in current query context")?;

    if pg_sys::bms_is_member((*node).varno as _, (*rel).relids) && (*node).varlevelsup == 0 {
        if (*node).varattno == 0 {
            panic!("Returning entire rows as experssions is currently not supported");
        }

        // If the var node references of the foreign entities we append it a attribute of that entity
        let rte = pg_sys::planner_rt_fetch((*node).varno, planner.root() as *mut _);
        let entity = parse_entity_version_id_from_foreign_table((*rte).relid)?;
        let attr_id =
            parse_to_owned_utf8_string(pg_sys::get_attname((*rte).relid, (*node).varattno, false))?;

        Ok(sqlil::Expr::attr(
            entity.entity_id,
            entity.version_id,
            attr_id,
        ))
    } else {
        // The input will be treated like a parameter in the query
        let r#type = from_pg_type((*node).vartype).context("Failed to determine type of column")?;
        let param_id = ctx.register_param(node as *mut pg_sys::Var as *mut _);

        Ok(sqlil::Expr::Parameter(sqlil::Parameter::new(
            r#type, param_id,
        )))
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use pgx::*;

    use crate::sqlil::test;

    #[pg_test]
    fn test_sqlil_convert_var_col() {
        Spi::connect(|mut client| {
            let _ = client.update(
                r#"
            CREATE SERVER dummy_srv FOREIGN DATA WRAPPER null_fdw;
            CREATE FOREIGN TABLE tab (col INTEGER) SERVER dummy_srv;"#,
                None,
                None,
            );
            Ok(Some(()))
        });

        let expr = test::convert_simple_expr("SELECT tab.col FROM tab").unwrap();

        assert_eq!(expr, sqlil::Expr::attr("tab", "latest", "col"));
    }

    #[pg_test]
    fn test_sqlil_convert_var_col_with_explicit_version() {
        Spi::connect(|mut client| {
            let _ = client.update(
                r#"
            CREATE SERVER dummy_srv FOREIGN DATA WRAPPER null_fdw;
            CREATE FOREIGN TABLE "tab:1.0" (col INTEGER) SERVER dummy_srv;"#,
                None,
                None,
            );
            Ok(Some(()))
        });

        let expr = test::convert_simple_expr(r#"SELECT tab.col FROM "tab:1.0" as tab"#).unwrap();

        assert_eq!(expr, sqlil::Expr::attr("tab", "1.0", "col"));
    }
}

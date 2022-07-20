use ansilo_core::{
    err::{bail, Context, Result},
    sqlil,
};
use pgx::{
    pg_schema,
    pg_sys::{self, Node},
};

use crate::fdw::ctx::FdwContext;

use super::{datum::from_pg_type, ConversionContext, PlannerContext};

pub(super) unsafe fn convert_param(
    node: *const pg_sys::Param,
    ctx: &mut ConversionContext,
    _planner: &PlannerContext,
    _fdw: &FdwContext,
) -> Result<sqlil::Expr> {
    // @see https://doxygen.postgresql.org/deparse_8c_source.html#l00405 (deparseParam)
    if (*node).paramkind == pg_sys::ParamKind_PARAM_MULTIEXPR {
        bail!("MULTIEXPR params are not supported");
    }

    let r#type =
        from_pg_type((*node).paramtype).context("Failed to determine type of query parameter")?;
    // Register the mapping of the pg param node to our sqlil parameter
    let param_id = ctx.register_param(node as *mut pg_sys::Param as *mut _);

    Ok(sqlil::Expr::Parameter(sqlil::Parameter::new(
        r#type, param_id,
    )))
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use pgx::*;

    use crate::sqlil::test;
    use ansilo_core::{data::DataType, sqlil::Parameter};

    #[pg_test]
    fn test_sqlil_convert_param() {
        let mut ctx = ConversionContext::new();
        let expr =
            test::convert_simple_expr_with_context("SELECT $1", &mut ctx, vec![DataType::Int32])
                .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::Parameter(Parameter::new(DataType::Int32, 1))
        );
    }
}

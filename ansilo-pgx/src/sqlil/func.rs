use ansilo_core::{
    err::{bail, Context, Result},
    sqlil,
};
use pgx::{
    pg_sys::{self, Node, Oid},
    *,
};

use crate::{fdw::ctx::FdwContext, util::syscache::PgSysCacheItem};

use super::{convert, datum::from_pg_type, ConversionContext, PlannerContext};

pub(super) unsafe fn convert_func_expr(
    node: *const pg_sys::FuncExpr,
    ctx: &mut ConversionContext,
    planner: &PlannerContext,
    fdw: &FdwContext,
) -> Result<sqlil::Expr> {
    // @see https://doxygen.postgresql.org/deparse_8c_source.html#l02930 (deparseFuncExpr)

    let args = pgx::PgList::<Node>::from_pg((*node).args);

    if (*node).funcformat == pg_sys::CoercionForm_COERCE_IMPLICIT_CAST
        || (*node).funcformat == pg_sys::CoercionForm_COERCE_EXPLICIT_CAST
    {
        let mut typemod = 0;
        if pg_sys::exprIsLengthCoercion(node as *const _, &mut typemod as *mut _) {
            bail!("Explicit type length conversion is not supported")
        }

        // Map to a type cast expression
        let expr = convert(args.head().unwrap(), ctx, planner, fdw)?;
        let r#type = from_pg_type((*node).funcresulttype).context("Unsupported type cast")?;
        return Ok(sqlil::Expr::Cast(sqlil::Cast::new(Box::new(expr), r#type)));
    }

    if (*node).funcretset {
        bail!("Set returning functions are not supported");
    }

    if (*node).funcvariadic {
        bail!("Variadic functions are not supported");
    }

    let func_name = {
        let cached_func = PgSysCacheItem::<pg_sys::FormData_pg_proc>::search(
            pg_sys::SysCacheIdentifier_PROCOID as _,
            [pgx::Datum::from((*node).funcid as Oid)],
        )
        .context("Failed to look up function from sys cache")?;

        pg_sys::name_data_to_str(&cached_func.proname).to_string()
    };

    // TODO: map all functions
    Ok(sqlil::Expr::FunctionCall(match func_name.as_str() {
        "char_length" | "character_length" => {
            sqlil::FunctionCall::Length(Box::new(convert(args.head().unwrap(), ctx, planner, fdw)?))
        }
        _ => bail!("Unsupported function {}", func_name),
    }))
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

    use crate::sqlil::test;
    use ansilo_core::common::data::*;

    #[pg_test]
    fn test_sqlil_convert_func_char_length() {
        // Need to use a query param in order to prevent postgres const-evaluating the expression during planning
        let mut ctx = ConversionContext::new();
        let expr = test::convert_simple_expr_with_context(
            "SELECT CHAR_LENGTH($1)",
            &mut ctx,
            vec![DataType::rust_string()],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::FunctionCall(sqlil::FunctionCall::Length(Box::new(
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::rust_string(), 1))
            )))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_func_explicit_cast() {
        // Need to use a query param in order to prevent postgres const-evaluating the expression during planning
        let mut ctx = ConversionContext::new();
        let expr = test::convert_simple_expr_with_context(
            "SELECT $1::integer",
            &mut ctx,
            vec![DataType::Int16],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::Cast(sqlil::Cast::new(
                Box::new(sqlil::Expr::Parameter(sqlil::Parameter::new(
                    DataType::Int16,
                    1
                ))),
                DataType::Int32
            ))
        );
    }
}

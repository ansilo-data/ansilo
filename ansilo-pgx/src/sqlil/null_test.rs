use ansilo_core::{
    err::{bail, Result},
    sqlil,
};
use pgx::{
    pg_sys::{self, Node},
    *,
};

use crate::fdw::ctx::FdwContext;

use super::*;

/// @see https://doxygen.postgresql.org/deparse_8c.html#a5c4650fb1a80edba78ada8ef67b24d49
pub unsafe fn convert_null_test(
    node: *const pg_sys::NullTest,
    ctx: &mut ConversionContext,
    planner: &PlannerContext,
    fdw: &FdwContext,
) -> Result<sqlil::Expr> {
    if (*node).argisrow {
        bail!("Performing IS [NOT] NULL check on rows is not supported");
    }

    let expr = convert((*node).arg as *const _, ctx, planner, fdw)?;

    Ok(sqlil::Expr::UnaryOp(sqlil::UnaryOp::new(
        if (*node).nulltesttype == pg_sys::NullTestType_IS_NULL {
            sqlil::UnaryOpType::IsNull
        } else {
            sqlil::UnaryOpType::IsNotNull
        },
        expr,
    )))
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

    use crate::sqlil::test;
    use ansilo_core::common::data::*;

    #[pg_test]
    fn test_sqlil_convert_null_test_is_null() {
        let expr = test::convert_simple_expr_with_context(
            "SELECT $1 IS NULL",
            &mut ConversionContext::new(),
            vec![DataType::Int32],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::UnaryOp(sqlil::UnaryOp::new(
                sqlil::UnaryOpType::IsNull,
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1))
            ))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_null_test_is_not_null() {
        let expr = test::convert_simple_expr_with_context(
            "SELECT $1 IS NOT NULL",
            &mut ConversionContext::new(),
            vec![DataType::Int32],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::UnaryOp(sqlil::UnaryOp::new(
                sqlil::UnaryOpType::IsNotNull,
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1))
            ))
        );
    }
}

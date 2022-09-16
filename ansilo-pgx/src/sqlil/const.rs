use ansilo_core::{
    data::DataValue,
    err::{Context, Result},
    sqlil,
};
use pgx::*;

use crate::fdw::ctx::{FdwContext, PlannerContext};

use super::{datum::from_datum, ConversionContext};

pub(super) unsafe fn convert_const(
    node: *const pg_sys::Const,
    _ctx: &mut ConversionContext,
    _planner: &PlannerContext,
    _fdw: &FdwContext,
) -> Result<sqlil::Expr> {
    if (*node).constisnull {
        return Ok(sqlil::Expr::constant(DataValue::Null));
    }

    let val = from_datum((*node).consttype, (*node).constvalue)
        .context("Failed to evaluation const expr")?;

    Ok(sqlil::Expr::constant(val))
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

    use crate::sqlil::test;
    use ansilo_core::data::rust_decimal::Decimal;

    #[pg_test]
    fn test_sqlil_convert_const_int() {
        assert_eq!(
            test::convert_simple_expr("SELECT 123").unwrap(),
            sqlil::Expr::constant(DataValue::Int32(123))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_const_string() {
        assert_eq!(
            test::convert_simple_expr("SELECT 'hello from pg'::text").unwrap(),
            sqlil::Expr::constant(DataValue::Utf8String("hello from pg".into()))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_const_bytea() {
        assert_eq!(
            test::convert_simple_expr("SELECT 'hello from pg'::bytea").unwrap(),
            sqlil::Expr::constant(DataValue::Binary("hello from pg".as_bytes().to_vec()))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_const_decimal() {
        assert_eq!(
            test::convert_simple_expr("SELECT 123.456").unwrap(),
            sqlil::Expr::constant(DataValue::Decimal(Decimal::new(123456, 3)))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_const_bool() {
        assert_eq!(
            test::convert_simple_expr("SELECT TRUE").unwrap(),
            sqlil::Expr::constant(DataValue::Boolean(true))
        );
        assert_eq!(
            test::convert_simple_expr("SELECT FALSE").unwrap(),
            sqlil::Expr::constant(DataValue::Boolean(false))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_const_null() {
        assert_eq!(
            test::convert_simple_expr("SELECT NULL").unwrap(),
            sqlil::Expr::constant(DataValue::Null)
        );
    }
}

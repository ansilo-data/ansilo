use ansilo_core::{
    err::{bail, Context, Result},
    sqlil::{self, SubstringCall},
};
use pgx::{
    pg_sys::{self, Node, Oid},
    *,
};

use crate::{
    fdw::ctx::{FdwContext, PlannerContext},
    util::syscache::PgSysCacheItem,
};

use super::{convert, datum::from_pg_type, ConversionContext};

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

    let mut arg = |idx: usize| convert(args.get_ptr(idx).unwrap(), ctx, planner, fdw);

    Ok(sqlil::Expr::FunctionCall(
        match (func_name.as_str(), args.len()) {
            ("char_length" | "character_length", 1) => {
                sqlil::FunctionCall::Length(Box::new(arg(0)?))
            }
            ("abs", 1) => sqlil::FunctionCall::Abs(Box::new(arg(0)?)),
            ("upper", 1) => sqlil::FunctionCall::Uppercase(Box::new(arg(0)?)),
            ("lower", 1) => sqlil::FunctionCall::Lowercase(Box::new(arg(0)?)),
            ("substring", 3) => {
                sqlil::FunctionCall::Substring(SubstringCall::new(arg(0)?, arg(1)?, arg(2)?))
            }
            ("gen_random_uuid", 0) => sqlil::FunctionCall::Uuid,
            _ => bail!("Unsupported function {}", func_name),
        },
    ))
}

pub(super) unsafe fn convert_coalesce_expr(
    node: *const pg_sys::CoalesceExpr,
    ctx: &mut ConversionContext,
    planner: &PlannerContext,
    fdw: &FdwContext,
) -> Result<sqlil::Expr> {
    let args = pgx::PgList::<Node>::from_pg((*node).args);

    Ok(sqlil::Expr::FunctionCall(sqlil::FunctionCall::Coalesce(
        args.iter_ptr()
            .map(|arg| convert(arg, ctx, planner, fdw).map(Box::new))
            .collect::<Result<Vec<_>>>()?,
    )))
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

    use crate::sqlil::test;
    use ansilo_core::data::*;

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

    #[pg_test]
    fn test_sqlil_convert_func_abs() {
        let mut ctx = ConversionContext::new();
        let expr = test::convert_simple_expr_with_context(
            "SELECT ABS($1)",
            &mut ctx,
            vec![DataType::Int32],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::FunctionCall(sqlil::FunctionCall::Abs(Box::new(sqlil::Expr::Parameter(
                sqlil::Parameter::new(DataType::Int32, 1)
            ))))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_func_upper() {
        let mut ctx = ConversionContext::new();
        let expr = test::convert_simple_expr_with_context(
            "SELECT UPPER($1)",
            &mut ctx,
            vec![DataType::Utf8String(StringOptions::default())],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::FunctionCall(sqlil::FunctionCall::Uppercase(Box::new(
                sqlil::Expr::Parameter(sqlil::Parameter::new(
                    DataType::Utf8String(StringOptions::default()),
                    1
                ))
            )))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_func_lower() {
        let mut ctx = ConversionContext::new();
        let expr = test::convert_simple_expr_with_context(
            "SELECT LOWER($1)",
            &mut ctx,
            vec![DataType::Utf8String(StringOptions::default())],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::FunctionCall(sqlil::FunctionCall::Lowercase(Box::new(
                sqlil::Expr::Parameter(sqlil::Parameter::new(
                    DataType::Utf8String(StringOptions::default()),
                    1
                ))
            )))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_func_substring() {
        let mut ctx = ConversionContext::new();
        let expr = test::convert_simple_expr_with_context(
            "SELECT SUBSTRING($1 FROM $2 FOR $3)",
            &mut ctx,
            vec![
                DataType::Utf8String(StringOptions::default()),
                DataType::Int32,
                DataType::Int32,
            ],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::FunctionCall(sqlil::FunctionCall::Substring(sqlil::SubstringCall::new(
                sqlil::Expr::Parameter(sqlil::Parameter::new(
                    DataType::Utf8String(StringOptions::default()),
                    1
                )),
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 2)),
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 3)),
            )))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_func_gen_random_uuid() {
        let mut ctx = ConversionContext::new();
        let expr =
            test::convert_simple_expr_with_context("SELECT GEN_RANDOM_UUID()", &mut ctx, vec![])
                .unwrap();

        assert_eq!(expr, sqlil::Expr::FunctionCall(sqlil::FunctionCall::Uuid));
    }

    #[pg_test]
    fn test_sqlil_convert_func_coalesce() {
        let mut ctx = ConversionContext::new();
        let expr = test::convert_simple_expr_with_context(
            "SELECT COALESCE($1, $2, $3)",
            &mut ctx,
            vec![DataType::Int32, DataType::Int32, DataType::Int32],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::FunctionCall(sqlil::FunctionCall::Coalesce(vec![
                Box::new(sqlil::Expr::Parameter(sqlil::Parameter::new(
                    DataType::Int32,
                    1
                ))),
                Box::new(sqlil::Expr::Parameter(sqlil::Parameter::new(
                    DataType::Int32,
                    2
                ))),
                Box::new(sqlil::Expr::Parameter(sqlil::Parameter::new(
                    DataType::Int32,
                    3
                ))),
            ]))
        );
    }
}

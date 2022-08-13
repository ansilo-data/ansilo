use ansilo_core::{
    data::DataValue,
    err::{bail, Context, Result},
    sqlil::{self, StringAggCall},
};
use pgx::{
    pg_sys::{self, Node, Oid},
    *,
};

use crate::{
    fdw::ctx::{FdwContext, PlannerContext},
    util::syscache::PgSysCacheItem,
};

use super::*;

/// @see https://doxygen.postgresql.org/deparse_8c.html#a35a84c656589f8c52bf9a2c5917a9468
pub(super) unsafe fn convert_aggref(
    node: *const pg_sys::Aggref,
    ctx: &mut ConversionContext,
    planner: &PlannerContext,
    fdw: &FdwContext,
) -> Result<sqlil::Expr> {
    if (*node).aggsplit != pg_sys::AggSplit_AGGSPLIT_SIMPLE {
        bail!("Only non-split aggregation supported");
    }

    if (*node).aggkind != b'n' as i8 {
        bail!("Only simple unordered aggregates are supported");
    }

    if !(*node).aggorder.is_null() {
        bail!("Only unordered aggregates are supported");
    }

    let func_name = {
        let cached_func = PgSysCacheItem::<pg_sys::FormData_pg_proc>::search(
            pg_sys::SysCacheIdentifier_PROCOID as _,
            [pgx::Datum::from((*node).aggfnoid as Oid)],
        )
        .context("Failed to look up aggregate function from sys cache")?;

        pg_sys::name_data_to_str(&cached_func.proname).to_string()
    };

    let args = PgList::<pg_sys::TargetEntry>::from_pg((*node).args);
    let mut args = args
        .iter_ptr()
        .filter(|e| !(**e).resjunk)
        .map(|e| (*e).expr as *mut Node)
        .map(|e| convert(e, ctx, planner, fdw))
        .collect::<Result<Vec<_>>>()?;

    if !(*node).aggdistinct.is_null() {
        return Ok(sqlil::Expr::AggregateCall(
            match (func_name.as_str(), args.len()) {
                ("count", 1) if !(*node).aggstar => {
                    sqlil::AggregateCall::CountDistinct(Box::new(args.remove(0)))
                }
                _ => bail!(
                    "Function '{}' is not supported with DISTINCT clause",
                    func_name
                ),
            },
        ));
    }

    // TODO: map all functions
    Ok(sqlil::Expr::AggregateCall(
        match (func_name.as_str(), args.len()) {
            ("count", _) if (*node).aggstar => sqlil::AggregateCall::Count,
            ("sum", 1) => sqlil::AggregateCall::Sum(Box::new(args.remove(0))),
            ("min", 1) => sqlil::AggregateCall::Min(Box::new(args.remove(0))),
            ("max", 1) => sqlil::AggregateCall::Max(Box::new(args.remove(0))),
            ("avg", 1) => sqlil::AggregateCall::Average(Box::new(args.remove(0))),
            ("string_agg", 2)
                if matches!(
                    args.get(1).unwrap(),
                    sqlil::Expr::Constant(sqlil::Constant {
                        value: DataValue::Utf8String(_),
                    }),
                ) =>
            {
                sqlil::AggregateCall::StringAgg(StringAggCall::new(
                    Box::new(args.remove(0)),
                    match args.remove(0) {
                        sqlil::Expr::Constant(sqlil::Constant {
                            value: DataValue::Utf8String(c),
                        }) => c,
                        _ => unreachable!(),
                    },
                ))
            }
            _ => bail!(
                "Aggregate function '{}' ({} args) is not supported",
                func_name,
                args.len()
            ),
        },
    ))
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

    use crate::sqlil::test;
    use ansilo_core::data::*;

    #[pg_test]
    fn test_sqlil_convert_aggref_count_star() {
        // Need to use a query param in order to prevent postgres const-evaluating the expression during planning
        let mut ctx = ConversionContext::new();
        let expr =
            test::convert_simple_expr_with_context("SELECT COUNT(*)", &mut ctx, vec![]).unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::AggregateCall(sqlil::AggregateCall::Count)
        );
    }

    #[pg_test]
    fn test_sqlil_convert_aggref_count_distinct() {
        // Need to use a query param in order to prevent postgres const-evaluating the expression during planning
        let mut ctx = ConversionContext::new();
        let expr = test::convert_simple_expr_with_context(
            "SELECT COUNT(DISTINCT $1)",
            &mut ctx,
            vec![DataType::Int32],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::AggregateCall(sqlil::AggregateCall::CountDistinct(Box::new(
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1))
            )))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_aggref_sum() {
        // Need to use a query param in order to prevent postgres const-evaluating the expression during planning
        let mut ctx = ConversionContext::new();
        let expr = test::convert_simple_expr_with_context(
            "SELECT SUM($1)",
            &mut ctx,
            vec![DataType::Int32],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::AggregateCall(sqlil::AggregateCall::Sum(Box::new(
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1))
            )))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_aggref_min() {
        let mut ctx = ConversionContext::new();
        let expr = test::convert_simple_expr_with_context(
            "SELECT MIN($1)",
            &mut ctx,
            vec![DataType::Int32],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::AggregateCall(sqlil::AggregateCall::Min(Box::new(
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1))
            )))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_aggref_max() {
        let mut ctx = ConversionContext::new();
        let expr = test::convert_simple_expr_with_context(
            "SELECT MAX($1)",
            &mut ctx,
            vec![DataType::Int32],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::AggregateCall(sqlil::AggregateCall::Max(Box::new(
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1))
            )))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_aggref_avg() {
        let mut ctx = ConversionContext::new();
        let expr = test::convert_simple_expr_with_context(
            "SELECT AVG($1)",
            &mut ctx,
            vec![DataType::Int32],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::AggregateCall(sqlil::AggregateCall::Average(Box::new(
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1))
            )))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_aggref_string_agg() {
        let mut ctx = ConversionContext::new();
        let expr = test::convert_simple_expr_with_context(
            "SELECT STRING_AGG($1, '::')",
            &mut ctx,
            vec![DataType::Utf8String(StringOptions::default())],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::AggregateCall(sqlil::AggregateCall::StringAgg(StringAggCall::new(
                Box::new(sqlil::Expr::Parameter(sqlil::Parameter::new(
                    DataType::Utf8String(StringOptions::default()),
                    1
                ))),
                "::".into()
            )))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_aggref_string_agg_dynamic_delimiter_fails() {
        let mut ctx = ConversionContext::new();
        test::convert_simple_expr_with_context(
            "SELECT STRING_AGG($1, $2)",
            &mut ctx,
            vec![
                DataType::Utf8String(StringOptions::default()),
                DataType::Utf8String(StringOptions::default()),
            ],
        )
        .unwrap_err();
    }
}

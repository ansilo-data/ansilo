use ansilo_core::{
    err::{bail, Context, Result},
    sqlil,
};
use pgx::{
    pg_sys::{self, FormData_pg_operator, Node},
    *,
};

use crate::{
    fdw::ctx::{FdwContext, PlannerContext},
    util::syscache::PgSysCacheItem,
};

use super::*;

pub unsafe fn convert_op_expr(
    node: *const pg_sys::OpExpr,
    ctx: &mut ConversionContext,
    planner: &PlannerContext,
    fdw: &FdwContext,
) -> Result<sqlil::Expr> {
    let op = get_operator((*node).opno)?;

    if op.oprkind == b'l' as i8 || op.oprkind == b'r' as i8 {
        return convert_unary_op_expr(node, op, ctx, planner, fdw);
    }

    if (*op).oprkind == b'b' as i8 {
        return convert_binary_op_expr(node, op, ctx, planner, fdw);
    }

    bail!("Unknown operator kind: {}", op.oprkind)
}

pub unsafe fn convert_unary_op_expr(
    node: *const pg_sys::OpExpr,
    op: PgSysCacheItem<FormData_pg_operator>,
    ctx: &mut ConversionContext,
    planner: &PlannerContext,
    fdw: &FdwContext,
) -> Result<sqlil::Expr> {
    let op = pg_sys::name_data_to_str(&op.oprname);
    let expr = PgList::<Node>::from_pg((*node).args).head().unwrap();
    let expr = convert(expr, ctx, planner, fdw)?;

    Ok(sqlil::Expr::UnaryOp(sqlil::UnaryOp::new(
        match op {
            "-" => sqlil::UnaryOpType::Negate,
            "not" => sqlil::UnaryOpType::LogicalNot,
            "~" => sqlil::UnaryOpType::BitwiseNot,
            _ => bail!("Unsupported unary operator: '{}'", op),
        },
        expr,
    )))
}

pub unsafe fn convert_binary_op_expr(
    node: *const pg_sys::OpExpr,
    op: PgSysCacheItem<FormData_pg_operator>,
    ctx: &mut ConversionContext,
    planner: &PlannerContext,
    fdw: &FdwContext,
) -> Result<sqlil::Expr> {
    let op = pg_sys::name_data_to_str(&op.oprname);
    let operands = PgList::<Node>::from_pg((*node).args);
    let left = operands.get_ptr(0).unwrap();
    let right = operands.get_ptr(1).unwrap();
    let left = convert(left, ctx, planner, fdw)?;
    let right = convert(right, ctx, planner, fdw)?;

    Ok(sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
        left,
        convert_binary_op(op)?,
        right,
    )))
}

pub unsafe fn convert_scalar_op_array_expr(
    node: *const pg_sys::ScalarArrayOpExpr,
    ctx: &mut ConversionContext,
    planner: &PlannerContext,
    fdw: &FdwContext,
) -> Result<sqlil::Expr> {
    let op = get_operator((*node).opno)?;
    let op = pg_sys::name_data_to_str(&op.oprname);
    let op = convert_binary_op(op)?;
    let agg_op = if (*node).useOr {
        sqlil::BinaryOpType::LogicalOr
    } else {
        sqlil::BinaryOpType::LogicalAnd
    };
    let operands = PgList::<Node>::from_pg((*node).args);
    let left = operands.get_ptr(0).unwrap();
    let right = operands.get_ptr(1).unwrap();

    if (*right).type_ != pg_sys::NodeTag_T_ArrayExpr {
        bail!("Op array expression mapping only supported on RHS array expression")
    }

    let left = convert(left, ctx, planner, fdw)?;
    let right = convert_list(
        (*(right as *const pg_sys::ArrayExpr)).elements,
        ctx,
        planner,
        fdw,
    )?;

    if right.is_empty() {
        bail!("Op array expression RHS cannot be empty");
    }

    let expr = right
        .into_iter()
        .map(|elem| sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(left.clone(), op, elem)))
        .reduce(|acc, op| sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(acc, agg_op, op)))
        .unwrap();

    Ok(expr)
}

fn get_operator<'a>(opno: u32) -> Result<PgSysCacheItem<'a, FormData_pg_operator>> {
    let op = PgSysCacheItem::<'a, FormData_pg_operator>::search(
        pg_sys::SysCacheIdentifier_OPEROID,
        [Datum::from(opno)],
    )
    .context("Failed to lookup operator in sys cache")?;

    Ok(op)
}

fn convert_binary_op(op: &str) -> Result<sqlil::BinaryOpType> {
    Ok(match op {
        "+" => sqlil::BinaryOpType::Add,
        "-" => sqlil::BinaryOpType::Subtract,
        "*" => sqlil::BinaryOpType::Multiply,
        "/" => sqlil::BinaryOpType::Divide,
        "||" => sqlil::BinaryOpType::Concat,
        "=" => sqlil::BinaryOpType::Equal,
        "<>" => sqlil::BinaryOpType::NotEqual,
        "%" => sqlil::BinaryOpType::Modulo,
        "^" => sqlil::BinaryOpType::Exponent,
        "&" => sqlil::BinaryOpType::BitwiseAnd,
        "|" => sqlil::BinaryOpType::BitwiseOr,
        "#" => sqlil::BinaryOpType::BitwiseXor,
        "<<" => sqlil::BinaryOpType::BitwiseShiftLeft,
        ">>" => sqlil::BinaryOpType::BitwiseShiftRight,
        "~" => sqlil::BinaryOpType::Regexp,
        ">" => sqlil::BinaryOpType::GreaterThan,
        ">=" => sqlil::BinaryOpType::GreaterThanOrEqual,
        "<" => sqlil::BinaryOpType::LessThan,
        "<=" => sqlil::BinaryOpType::LessThanOrEqual,
        _ => bail!("Unsupported binary operator: '{}'", op),
    })
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

    use crate::sqlil::test;
    use ansilo_core::data::*;

    #[pg_test]
    fn test_sqlil_convert_op_unary_negate() {
        let expr = test::convert_simple_expr_with_context(
            "SELECT -$1",
            &mut ConversionContext::new(),
            vec![DataType::Int32],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::UnaryOp(sqlil::UnaryOp::new(
                sqlil::UnaryOpType::Negate,
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1))
            ))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_op_unary_not() {
        let expr = test::convert_simple_expr_with_context(
            "SELECT NOT $1",
            &mut ConversionContext::new(),
            vec![DataType::Boolean],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::UnaryOp(sqlil::UnaryOp::new(
                sqlil::UnaryOpType::LogicalNot,
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Boolean, 1))
            ))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_op_unary_bitwise_not() {
        let expr = test::convert_simple_expr_with_context(
            "SELECT ~$1",
            &mut ConversionContext::new(),
            vec![DataType::Int32],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::UnaryOp(sqlil::UnaryOp::new(
                sqlil::UnaryOpType::BitwiseNot,
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1))
            ))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_op_binary_add() {
        let expr = test::convert_simple_expr_with_context(
            "SELECT $1 + $2",
            &mut ConversionContext::new(),
            vec![DataType::Int32, DataType::Int32],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1)),
                sqlil::BinaryOpType::Add,
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 2))
            ))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_op_binary_subtract() {
        let expr = test::convert_simple_expr_with_context(
            "SELECT $1 - $2",
            &mut ConversionContext::new(),
            vec![DataType::Int32, DataType::Int32],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1)),
                sqlil::BinaryOpType::Subtract,
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 2))
            ))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_op_binary_multiply() {
        let expr = test::convert_simple_expr_with_context(
            "SELECT $1 * $2",
            &mut ConversionContext::new(),
            vec![DataType::Int32, DataType::Int32],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1)),
                sqlil::BinaryOpType::Multiply,
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 2))
            ))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_op_binary_divide() {
        let expr = test::convert_simple_expr_with_context(
            "SELECT $1 / $2",
            &mut ConversionContext::new(),
            vec![DataType::Int32, DataType::Int32],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1)),
                sqlil::BinaryOpType::Divide,
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 2))
            ))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_op_binary_modulo() {
        let expr = test::convert_simple_expr_with_context(
            "SELECT $1 % $2",
            &mut ConversionContext::new(),
            vec![DataType::Int32, DataType::Int32],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1)),
                sqlil::BinaryOpType::Modulo,
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 2))
            ))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_op_binary_exponent() {
        let expr = test::convert_simple_expr_with_context(
            "SELECT $1 ^ $2",
            &mut ConversionContext::new(),
            vec![DataType::Float64, DataType::Float64],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Float64, 1)),
                sqlil::BinaryOpType::Exponent,
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Float64, 2))
            ))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_op_string_concat() {
        let expr = test::convert_simple_expr_with_context(
            "SELECT $1 || $2",
            &mut ConversionContext::new(),
            vec![
                DataType::Utf8String(StringOptions::default()),
                DataType::Utf8String(StringOptions::default()),
            ],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::Parameter(sqlil::Parameter::new(
                    DataType::Utf8String(StringOptions::default()),
                    1
                )),
                sqlil::BinaryOpType::Concat,
                sqlil::Expr::Parameter(sqlil::Parameter::new(
                    DataType::Utf8String(StringOptions::default()),
                    2
                ))
            ))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_op_binary_eq() {
        let expr = test::convert_simple_expr_with_context(
            "SELECT $1 = $2",
            &mut ConversionContext::new(),
            vec![DataType::Int32, DataType::Int32],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1)),
                sqlil::BinaryOpType::Equal,
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 2))
            ))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_op_binary_neq() {
        for sql in ["SELECT $1 != $2", "SELECT $1 <> $2"].into_iter() {
            let expr = test::convert_simple_expr_with_context(
                sql,
                &mut ConversionContext::new(),
                vec![DataType::Int32, DataType::Int32],
            )
            .unwrap();

            assert_eq!(
                expr,
                sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                    sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1)),
                    sqlil::BinaryOpType::NotEqual,
                    sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 2))
                ))
            );
        }
    }

    #[pg_test]
    fn test_sqlil_convert_op_binary_bitwise_and() {
        let expr = test::convert_simple_expr_with_context(
            "SELECT $1 & $2",
            &mut ConversionContext::new(),
            vec![DataType::Int32, DataType::Int32],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1)),
                sqlil::BinaryOpType::BitwiseAnd,
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 2))
            ))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_op_binary_bitwise_or() {
        let expr = test::convert_simple_expr_with_context(
            "SELECT $1 | $2",
            &mut ConversionContext::new(),
            vec![DataType::Int32, DataType::Int32],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1)),
                sqlil::BinaryOpType::BitwiseOr,
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 2))
            ))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_op_binary_bitwise_xor() {
        let expr = test::convert_simple_expr_with_context(
            "SELECT $1 # $2",
            &mut ConversionContext::new(),
            vec![DataType::Int32, DataType::Int32],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1)),
                sqlil::BinaryOpType::BitwiseXor,
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 2))
            ))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_op_binary_bitwise_shift_left() {
        let expr = test::convert_simple_expr_with_context(
            "SELECT $1 << $2",
            &mut ConversionContext::new(),
            vec![DataType::Int32, DataType::Int32],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1)),
                sqlil::BinaryOpType::BitwiseShiftLeft,
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 2))
            ))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_op_binary_bitwise_shift_right() {
        let expr = test::convert_simple_expr_with_context(
            "SELECT $1 >> $2",
            &mut ConversionContext::new(),
            vec![DataType::Int32, DataType::Int32],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1)),
                sqlil::BinaryOpType::BitwiseShiftRight,
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 2))
            ))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_op_binary_bitwise_regexp() {
        let expr = test::convert_simple_expr_with_context(
            "SELECT $1 ~ $2",
            &mut ConversionContext::new(),
            vec![
                DataType::Utf8String(StringOptions::default()),
                DataType::Utf8String(StringOptions::default()),
            ],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::Parameter(sqlil::Parameter::new(
                    DataType::Utf8String(StringOptions::default()),
                    1
                )),
                sqlil::BinaryOpType::Regexp,
                sqlil::Expr::Parameter(sqlil::Parameter::new(
                    DataType::Utf8String(StringOptions::default()),
                    2
                ))
            ))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_op_binary_greater_than() {
        let expr = test::convert_simple_expr_with_context(
            "SELECT $1 > $2",
            &mut ConversionContext::new(),
            vec![DataType::Int32, DataType::Int32],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1)),
                sqlil::BinaryOpType::GreaterThan,
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 2))
            ))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_op_binary_greater_than_eq() {
        let expr = test::convert_simple_expr_with_context(
            "SELECT $1 >= $2",
            &mut ConversionContext::new(),
            vec![DataType::Int32, DataType::Int32],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1)),
                sqlil::BinaryOpType::GreaterThanOrEqual,
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 2))
            ))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_op_binary_less_than() {
        let expr = test::convert_simple_expr_with_context(
            "SELECT $1 < $2",
            &mut ConversionContext::new(),
            vec![DataType::Int32, DataType::Int32],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1)),
                sqlil::BinaryOpType::LessThan,
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 2))
            ))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_op_binary_less_than_eq() {
        let expr = test::convert_simple_expr_with_context(
            "SELECT $1 <= $2",
            &mut ConversionContext::new(),
            vec![DataType::Int32, DataType::Int32],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1)),
                sqlil::BinaryOpType::LessThanOrEqual,
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 2))
            ))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_op_binary_in_operator() {
        let expr = test::convert_simple_expr_with_context(
            "SELECT $1 IN ($2, $3)",
            &mut ConversionContext::new(),
            vec![DataType::Int32, DataType::Int32, DataType::Int32],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                    sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1)),
                    sqlil::BinaryOpType::Equal,
                    sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 2))
                )),
                sqlil::BinaryOpType::LogicalOr,
                sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                    sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1)),
                    sqlil::BinaryOpType::Equal,
                    sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 3))
                ))
            ))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_op_binary_in_operator_single_elem() {
        let expr = test::convert_simple_expr_with_context(
            "SELECT $1 IN ($2)",
            &mut ConversionContext::new(),
            vec![DataType::Int32, DataType::Int32],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1)),
                sqlil::BinaryOpType::Equal,
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 2))
            )),
        );
    }

    #[pg_test]
    fn test_sqlil_convert_op_binary_not_in_operator() {
        let expr = test::convert_simple_expr_with_context(
            "SELECT $1 NOT IN ($2, $3)",
            &mut ConversionContext::new(),
            vec![DataType::Int32, DataType::Int32, DataType::Int32],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                    sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1)),
                    sqlil::BinaryOpType::NotEqual,
                    sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 2))
                )),
                sqlil::BinaryOpType::LogicalAnd,
                sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                    sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1)),
                    sqlil::BinaryOpType::NotEqual,
                    sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 3))
                ))
            ))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_op_binary_not_in_operator_single_elem() {
        let expr = test::convert_simple_expr_with_context(
            "SELECT $1 NOT IN ($2)",
            &mut ConversionContext::new(),
            vec![DataType::Int32, DataType::Int32],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1)),
                sqlil::BinaryOpType::NotEqual,
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 2))
            )),
        );
    }

    #[pg_test]
    fn test_sqlil_convert_op_binary_any_op() {
        let expr = test::convert_simple_expr_with_context(
            "SELECT $1 > ANY (ARRAY[$2, $3])",
            &mut ConversionContext::new(),
            vec![DataType::Int32, DataType::Int32, DataType::Int32],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                    sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1)),
                    sqlil::BinaryOpType::GreaterThan,
                    sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 2))
                )),
                sqlil::BinaryOpType::LogicalOr,
                sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                    sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1)),
                    sqlil::BinaryOpType::GreaterThan,
                    sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 3))
                ))
            ))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_op_binary_all_op() {
        let expr = test::convert_simple_expr_with_context(
            "SELECT $1 > ALL (ARRAY[$2, $3])",
            &mut ConversionContext::new(),
            vec![DataType::Int32, DataType::Int32, DataType::Int32],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                    sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1)),
                    sqlil::BinaryOpType::GreaterThan,
                    sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 2))
                )),
                sqlil::BinaryOpType::LogicalAnd,
                sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                    sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1)),
                    sqlil::BinaryOpType::GreaterThan,
                    sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 3))
                ))
            ))
        );
    }
}

use std::collections::HashMap;

use anyhow::{bail, Context, Result};

use crate::data::{DataType, DataValue};

use super::*;

/// Provides an interface to evaluating expressions to resultant DataValue
///
/// Sometimes we it useful to perform local evaluation of some expressions when
/// they cannot be pushed down to the data source but sit outside of postgres.
///
/// We only support a small subset of operators which we can add to ad-hoc or as required
/// for future connectors.
pub struct ExprEvaluator {}

impl ExprEvaluator {
    /// Determine whether local evaluation of the expression is supported
    pub fn can_eval(expr: &Expr) -> bool {
        match expr {
            Expr::Attribute(_) => true,
            Expr::Constant(_) => true,
            Expr::UnaryOp(op) => match op.r#type {
                UnaryOpType::LogicalNot => true,
                UnaryOpType::IsNull => true,
                UnaryOpType::IsNotNull => true,
                _ => false,
            },
            Expr::BinaryOp(op) => match op.r#type {
                BinaryOpType::LogicalAnd => true,
                BinaryOpType::LogicalOr => true,
                BinaryOpType::Concat => true,
                BinaryOpType::JsonExtract => true,
                _ => false,
            },
            Expr::Cast(_) => true,
            _ => false,
        }
    }

    /// Finds the attribute id's required to evaluate
    /// this expression.
    pub fn required_attrs(expr: &Expr) -> Vec<AttributeId> {
        let mut attrs = vec![];

        expr.walk(&mut |e| {
            if let Expr::Attribute(attr) = e {
                attrs.push(attr.clone());
            }
        });

        attrs
    }

    /// Performs the evaluation of the supplied expression
    ///
    /// Any attribute expressions will be retrieved from the supplied map
    /// We assume that only a single entity is referenced by the underlying expressions.
    pub fn eval(expr: &Expr, attrs: &HashMap<String, DataValue>) -> Result<DataValue> {
        let res = match expr {
            Expr::Attribute(att) => attrs
                .get(&att.attribute_id)
                .with_context(|| {
                    format!("Attribute id '{}' has no supplied value", att.attribute_id)
                })?
                .clone(),
            Expr::Constant(c) => c.value.clone(),
            Expr::UnaryOp(op) => Self::eval_unary_op(op, attrs)?,
            Expr::BinaryOp(op) => Self::eval_bin_op(op, attrs)?,
            Expr::Cast(cast) => Self::eval_cast(cast, attrs)?,
            _ => bail!("Unsupported expr: {:?}", expr),
        };

        Ok(res)
    }

    fn eval_unary_op(op: &UnaryOp, attrs: &HashMap<String, DataValue>) -> Result<DataValue> {
        let arg = Self::eval(&op.expr, attrs)?;

        let res = match op.r#type {
            UnaryOpType::LogicalNot => DataValue::Boolean(
                !*arg
                    .try_coerce_into(&DataType::Boolean)?
                    .as_boolean()
                    .unwrap(),
            ),
            UnaryOpType::IsNull => DataValue::Boolean(arg.is_null()),
            UnaryOpType::IsNotNull => DataValue::Boolean(!arg.is_null()),
            _ => bail!("Unsupported op {:?}", op),
        };

        Ok(res)
    }

    fn eval_bin_op(op: &BinaryOp, attrs: &HashMap<String, DataValue>) -> Result<DataValue> {
        let left = Self::eval(&op.left, attrs)?;
        let right = Self::eval(&op.right, attrs)?;

        let res = match op.r#type {
            BinaryOpType::LogicalAnd => {
                let left = left.try_coerce_into(&DataType::Boolean)?;
                let right = right.try_coerce_into(&DataType::Boolean)?;

                DataValue::Boolean(*left.as_boolean().unwrap() && *right.as_boolean().unwrap())
            }
            BinaryOpType::LogicalOr => {
                let left = left.try_coerce_into(&DataType::Boolean)?;
                let right = right.try_coerce_into(&DataType::Boolean)?;

                DataValue::Boolean(*left.as_boolean().unwrap() || *right.as_boolean().unwrap())
            }
            BinaryOpType::Concat => {
                let left = left.try_coerce_into(&DataType::rust_string())?;
                let right = right.try_coerce_into(&DataType::rust_string())?;

                DataValue::Utf8String(format!(
                    "{}{}",
                    left.as_utf8_string().unwrap(),
                    right.as_utf8_string().unwrap()
                ))
            }
            BinaryOpType::JsonExtract => {
                let left = left.try_coerce_into(&DataType::JSON)?;
                let json = left.as_json().unwrap();
                let json: serde_json::Value =
                    serde_json::from_str(json).context("Could not parse json")?;

                match json {
                    serde_json::Value::Array(arr)
                        if right.clone().try_coerce_into(&DataType::UInt32).is_ok() =>
                    {
                        let idx = *right
                            .try_coerce_into(&DataType::UInt32)
                            .unwrap()
                            .as_u_int32()
                            .unwrap();

                        DataValue::JSON(serde_json::to_string(
                            arr.get(idx as usize).unwrap_or(&serde_json::Value::Null),
                        )?)
                    }
                    serde_json::Value::Object(obj)
                        if right
                            .clone()
                            .try_coerce_into(&DataType::rust_string())
                            .is_ok() =>
                    {
                        let idx = right
                            .try_coerce_into(&DataType::rust_string())
                            .unwrap()
                            .as_utf8_string()
                            .unwrap()
                            .clone();

                        DataValue::JSON(serde_json::to_string(
                            obj.get(&idx).unwrap_or(&serde_json::Value::Null),
                        )?)
                    }
                    _ => bail!("Could not extract json {:?} using key {:?}", left, right),
                }
            }
            _ => bail!("Unsupported op {:?}", op),
        };

        Ok(res)
    }

    fn eval_cast(cast: &Cast, attrs: &HashMap<String, DataValue>) -> Result<DataValue> {
        let arg = Self::eval(&cast.expr, attrs)?;

        arg.try_coerce_into(&cast.r#type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_can_eval() {
        assert_eq!(
            ExprEvaluator::can_eval(&Expr::constant(DataValue::UInt32(123))),
            true
        );
        assert_eq!(
            ExprEvaluator::can_eval(&Expr::Parameter(Parameter::new(DataType::Int32, 1))),
            false
        );
    }

    #[test]
    fn test_find_attrs() {
        assert_eq!(
            ExprEvaluator::required_attrs(&Expr::constant(DataValue::UInt32(123))),
            vec![]
        );
        assert_eq!(
            ExprEvaluator::required_attrs(&Expr::attr("e", "att")),
            vec![AttributeId::new("e", "att")]
        );
        assert_eq!(
            ExprEvaluator::required_attrs(&Expr::BinaryOp(BinaryOp::new(
                Expr::attr("e", "att1"),
                BinaryOpType::Equal,
                Expr::attr("e", "att2")
            ))),
            vec![AttributeId::new("e", "att1"), AttributeId::new("e", "att2")]
        );
    }

    #[test]
    fn test_eval_constant() {
        assert_eq!(
            ExprEvaluator::eval(&Expr::constant(DataValue::UInt32(123)), &HashMap::new()).unwrap(),
            DataValue::UInt32(123)
        );
    }

    #[test]
    fn test_eval_attr() {
        assert_eq!(
            ExprEvaluator::eval(
                &Expr::attr("entity", "att"),
                &[("att".into(), DataValue::Int16(1))].into_iter().collect()
            )
            .unwrap(),
            DataValue::Int16(1)
        );
    }

    #[test]
    fn test_eval_attr_not_supplied() {
        ExprEvaluator::eval(&Expr::attr("entity", "att"), &HashMap::new()).unwrap_err();
    }

    #[test]
    fn test_eval_unary_op_logical_not() {
        assert_eq!(
            ExprEvaluator::eval(
                &Expr::UnaryOp(UnaryOp::new(
                    UnaryOpType::LogicalNot,
                    Expr::constant(DataValue::Boolean(false)),
                )),
                &HashMap::new(),
            )
            .unwrap(),
            DataValue::Boolean(true)
        );
    }

    #[test]
    fn test_eval_unary_op_is_null() {
        assert_eq!(
            ExprEvaluator::eval(
                &Expr::UnaryOp(UnaryOp::new(
                    UnaryOpType::IsNull,
                    Expr::constant(DataValue::Boolean(false)),
                )),
                &HashMap::new(),
            )
            .unwrap(),
            DataValue::Boolean(false)
        );

        assert_eq!(
            ExprEvaluator::eval(
                &Expr::UnaryOp(UnaryOp::new(
                    UnaryOpType::IsNull,
                    Expr::constant(DataValue::Null),
                )),
                &HashMap::new(),
            )
            .unwrap(),
            DataValue::Boolean(true)
        );
    }

    #[test]
    fn test_eval_unary_op_is_not_null() {
        assert_eq!(
            ExprEvaluator::eval(
                &Expr::UnaryOp(UnaryOp::new(
                    UnaryOpType::IsNotNull,
                    Expr::constant(DataValue::Boolean(false)),
                )),
                &HashMap::new(),
            )
            .unwrap(),
            DataValue::Boolean(true)
        );

        assert_eq!(
            ExprEvaluator::eval(
                &Expr::UnaryOp(UnaryOp::new(
                    UnaryOpType::IsNotNull,
                    Expr::constant(DataValue::Null),
                )),
                &HashMap::new(),
            )
            .unwrap(),
            DataValue::Boolean(false)
        );
    }

    #[test]
    fn test_eval_bin_op_logical_and() {
        assert_eq!(
            ExprEvaluator::eval(
                &Expr::BinaryOp(BinaryOp::new(
                    Expr::constant(DataValue::Boolean(false)),
                    BinaryOpType::LogicalAnd,
                    Expr::constant(DataValue::Boolean(false)),
                )),
                &HashMap::new(),
            )
            .unwrap(),
            DataValue::Boolean(false)
        );
        assert_eq!(
            ExprEvaluator::eval(
                &Expr::BinaryOp(BinaryOp::new(
                    Expr::constant(DataValue::Boolean(true)),
                    BinaryOpType::LogicalAnd,
                    Expr::constant(DataValue::Boolean(false)),
                )),
                &HashMap::new(),
            )
            .unwrap(),
            DataValue::Boolean(false)
        );
        assert_eq!(
            ExprEvaluator::eval(
                &Expr::BinaryOp(BinaryOp::new(
                    Expr::constant(DataValue::Boolean(true)),
                    BinaryOpType::LogicalAnd,
                    Expr::constant(DataValue::Boolean(true)),
                )),
                &HashMap::new(),
            )
            .unwrap(),
            DataValue::Boolean(true)
        );
    }

    #[test]
    fn test_eval_bin_op_logical_or() {
        assert_eq!(
            ExprEvaluator::eval(
                &Expr::BinaryOp(BinaryOp::new(
                    Expr::constant(DataValue::Boolean(false)),
                    BinaryOpType::LogicalOr,
                    Expr::constant(DataValue::Boolean(false)),
                )),
                &HashMap::new(),
            )
            .unwrap(),
            DataValue::Boolean(false)
        );
        assert_eq!(
            ExprEvaluator::eval(
                &Expr::BinaryOp(BinaryOp::new(
                    Expr::constant(DataValue::Boolean(true)),
                    BinaryOpType::LogicalOr,
                    Expr::constant(DataValue::Boolean(false)),
                )),
                &HashMap::new(),
            )
            .unwrap(),
            DataValue::Boolean(true)
        );
        assert_eq!(
            ExprEvaluator::eval(
                &Expr::BinaryOp(BinaryOp::new(
                    Expr::constant(DataValue::Boolean(true)),
                    BinaryOpType::LogicalOr,
                    Expr::constant(DataValue::Boolean(true)),
                )),
                &HashMap::new(),
            )
            .unwrap(),
            DataValue::Boolean(true)
        );
    }

    #[test]
    fn test_eval_bin_op_concat() {
        assert_eq!(
            ExprEvaluator::eval(
                &Expr::BinaryOp(BinaryOp::new(
                    Expr::constant(DataValue::Utf8String("abc".into())),
                    BinaryOpType::Concat,
                    Expr::constant(DataValue::Utf8String("123".into())),
                )),
                &HashMap::new(),
            )
            .unwrap(),
            DataValue::Utf8String("abc123".into())
        );
    }

    #[test]
    fn test_eval_bin_op_json_extract_obj() {
        assert_eq!(
            ExprEvaluator::eval(
                &Expr::BinaryOp(BinaryOp::new(
                    Expr::constant(DataValue::JSON(r#"{"foo": "bar"}"#.into())),
                    BinaryOpType::JsonExtract,
                    Expr::constant(DataValue::Utf8String("foo".into())),
                )),
                &HashMap::new(),
            )
            .unwrap(),
            DataValue::JSON(r#""bar""#.into())
        );
    }

    #[test]
    fn test_eval_bin_op_json_extract_arr() {
        assert_eq!(
            ExprEvaluator::eval(
                &Expr::BinaryOp(BinaryOp::new(
                    Expr::constant(DataValue::JSON(r#"["a", "b", "c"]"#.into())),
                    BinaryOpType::JsonExtract,
                    Expr::constant(DataValue::UInt32(1)),
                )),
                &HashMap::new(),
            )
            .unwrap(),
            DataValue::JSON(r#""b""#.into())
        );
    }

    #[test]
    fn test_eval_bin_op_json_extract_obj_invalid_key() {
        assert_eq!(
            ExprEvaluator::eval(
                &Expr::BinaryOp(BinaryOp::new(
                    Expr::constant(DataValue::JSON(r#"{"foo": "bar"}"#.into())),
                    BinaryOpType::JsonExtract,
                    Expr::constant(DataValue::Utf8String("invalid".into())),
                )),
                &HashMap::new(),
            )
            .unwrap(),
            DataValue::JSON(r#"null"#.into())
        );
    }

    #[test]
    fn test_eval_bin_op_json_extract_arr_invalid_key() {
        assert_eq!(
            ExprEvaluator::eval(
                &Expr::BinaryOp(BinaryOp::new(
                    Expr::constant(DataValue::JSON(r#"["a", "b", "c"]"#.into())),
                    BinaryOpType::JsonExtract,
                    Expr::constant(DataValue::UInt32(10)),
                )),
                &HashMap::new(),
            )
            .unwrap(),
            DataValue::JSON(r#"null"#.into())
        );
    }
}

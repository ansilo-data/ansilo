use std::{io, sync::Arc};

use ansilo_core::{
    common::data::{DataType, DataValue},
    err::{bail, Error, Result},
    sqlil,
};

use crate::common::entity::{ConnectorEntityConfig, EntitySource};

use super::{MemoryConnectionConfig, MemoryResultSet};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MemoryQueryExecutor {
    data: Arc<MemoryConnectionConfig>,
    entities: ConnectorEntityConfig<()>,
    query: sqlil::Select,
    params: Vec<DataValue>,
}

impl MemoryQueryExecutor {
    pub(crate) fn new(
        data: Arc<MemoryConnectionConfig>,
        entities: ConnectorEntityConfig<()>,
        query: sqlil::Select,
        params: Vec<DataValue>,
    ) -> Self {
        Self {
            data,
            entities,
            query,
            params,
        }
    }

    pub(crate) fn run(&self) -> Result<MemoryResultSet> {
        let source = self
            .data
            .get_entity_id_data(&self.query.from)
            .ok_or(Error::msg("Could not find entity"))?;

        let mut results = vec![];

        for row in source {
            if self.satisfies_where(row)? {
                results.push(self.project(row)?);
            }
        }

        MemoryResultSet::new(self.cols()?, results)
    }

    fn satisfies_where(&self, row: &[DataValue]) -> Result<bool> {
        let mut res = true;

        for cond in self.query.r#where.iter() {
            let out = match self.evaluate(row, cond)? {
                DataValue::Boolean(out) => out,
                _ => false,
            };

            res = res && out;
        }

        Ok(res)
    }

    fn evaluate(&self, row: &[DataValue], expr: &sqlil::Expr) -> Result<DataValue> {
        Ok(match expr {
            sqlil::Expr::EntityVersion(_) => bail!("Cannot reference entity without attribute"),
            sqlil::Expr::EntityVersionAttribute(a) => {
                let entity = self.get_conf(a)?;
                // TODO: name vs id
                let attr_idx = entity
                    .version()
                    .attributes
                    .iter()
                    .position(|i| i.name == a.attribute_id)
                    .ok_or(Error::msg("Could not find attr"))?;

                row[attr_idx].clone()
            }
            sqlil::Expr::Constant(v) => v.value.clone(),
            sqlil::Expr::Parameter(_) => todo!(),
            sqlil::Expr::UnaryOp(_) => todo!(),
            sqlil::Expr::BinaryOp(op) => {
                let left = self.evaluate(row, &op.left)?;
                let right = self.evaluate(row, &op.right)?;
                match op.r#type {
                    sqlil::BinaryOpType::Add => todo!(),
                    sqlil::BinaryOpType::Subtract => todo!(),
                    sqlil::BinaryOpType::Multiply => todo!(),
                    sqlil::BinaryOpType::Divide => todo!(),
                    sqlil::BinaryOpType::Modulo => todo!(),
                    sqlil::BinaryOpType::Exponent => todo!(),
                    sqlil::BinaryOpType::BitwiseAnd => todo!(),
                    sqlil::BinaryOpType::BitwiseOr => todo!(),
                    sqlil::BinaryOpType::BitwiseXor => todo!(),
                    sqlil::BinaryOpType::BitwiseShiftLeft => todo!(),
                    sqlil::BinaryOpType::BitwiseShiftRight => todo!(),
                    sqlil::BinaryOpType::Concat => todo!(),
                    sqlil::BinaryOpType::Regexp => todo!(),
                    sqlil::BinaryOpType::In => todo!(),
                    sqlil::BinaryOpType::NotIn => todo!(),
                    sqlil::BinaryOpType::Equal => DataValue::Boolean(left == right),
                    sqlil::BinaryOpType::NotEqual => DataValue::Boolean(left != right),
                    sqlil::BinaryOpType::GreaterThan => todo!(),
                    sqlil::BinaryOpType::GreaterThanOrEqual => todo!(),
                    sqlil::BinaryOpType::LessThan => todo!(),
                    sqlil::BinaryOpType::LessThanOrEqual => todo!(),
                }
            }
            sqlil::Expr::FunctionCall(_) => todo!(),
            sqlil::Expr::AggregateCall(_) => todo!(),
        })
    }

    fn cols(&self) -> Result<Vec<(String, DataType)>> {
        self.query
            .cols
            .iter()
            .map(|(s, e)| Ok((s.clone(), self.evaluate_type(e)?)))
            .collect()
    }

    fn evaluate_type(&self, e: &sqlil::Expr) -> Result<DataType> {
        Ok(match e {
            sqlil::Expr::EntityVersion(_) => bail!("Cannot reference entity without attribute"),
            sqlil::Expr::EntityVersionAttribute(a) => {
                let entity = self.get_conf(a)?;
                // TODO: name vs id
                let attr = entity
                    .version()
                    .attributes
                    .iter()
                    .find(|i| i.name == a.attribute_id)
                    .ok_or(Error::msg("Could not find attr"))?;

                attr.r#type.clone()
            }
            sqlil::Expr::Constant(v) => v.value.clone().into(),
            sqlil::Expr::Parameter(p) => p.r#type.clone(),
            sqlil::Expr::UnaryOp(_) => todo!(),
            sqlil::Expr::BinaryOp(op) => {
                let _left = self.evaluate_type(&op.left)?;
                let _right = self.evaluate_type(&op.right)?;

                match op.r#type {
                    sqlil::BinaryOpType::Add => todo!(),
                    sqlil::BinaryOpType::Subtract => todo!(),
                    sqlil::BinaryOpType::Multiply => todo!(),
                    sqlil::BinaryOpType::Divide => todo!(),
                    sqlil::BinaryOpType::Modulo => todo!(),
                    sqlil::BinaryOpType::Exponent => todo!(),
                    sqlil::BinaryOpType::BitwiseAnd => todo!(),
                    sqlil::BinaryOpType::BitwiseOr => todo!(),
                    sqlil::BinaryOpType::BitwiseXor => todo!(),
                    sqlil::BinaryOpType::BitwiseShiftLeft => todo!(),
                    sqlil::BinaryOpType::BitwiseShiftRight => todo!(),
                    sqlil::BinaryOpType::Concat => todo!(),
                    sqlil::BinaryOpType::Regexp => todo!(),
                    sqlil::BinaryOpType::In => todo!(),
                    sqlil::BinaryOpType::NotIn => todo!(),
                    sqlil::BinaryOpType::Equal => DataType::Boolean,
                    sqlil::BinaryOpType::NotEqual => DataType::Boolean,
                    sqlil::BinaryOpType::GreaterThan => todo!(),
                    sqlil::BinaryOpType::GreaterThanOrEqual => todo!(),
                    sqlil::BinaryOpType::LessThan => todo!(),
                    sqlil::BinaryOpType::LessThanOrEqual => todo!(),
                }
            }
            sqlil::Expr::FunctionCall(_) => todo!(),
            sqlil::Expr::AggregateCall(_) => todo!(),
        })
    }

    fn get_conf(&self, a: &sqlil::EntityVersionAttributeIdentifier) -> Result<EntitySource<()>> {
        let entity = self
            .entities
            .find(&a.entity)
            .ok_or(Error::msg("Could not find entity"))?;

        Ok(entity.clone())
    }

    fn project(&self, row: &Vec<DataValue>) -> Result<Vec<DataValue>> {
        let mut res = vec![];

        for (_, expr) in self.query.cols.iter() {
            res.push(self.evaluate(row, expr)?);
        }

        Ok(res)
    }
}

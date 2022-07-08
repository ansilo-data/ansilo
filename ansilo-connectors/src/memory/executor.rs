use std::sync::Arc;

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

        if self.query.row_skip > 0 {
            results = results.into_iter().skip(self.query.row_skip as _).collect();
        }

        if let Some(limit) = self.query.row_limit {
            results = results.into_iter().take(limit as _).collect();
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
                let attr_idx = entity
                    .version()
                    .attributes
                    .iter()
                    .position(|i| i.id == a.attribute_id)
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
                let attr = entity
                    .version()
                    .attributes
                    .iter()
                    .find(|i| i.id == a.attribute_id)
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

#[cfg(test)]
mod tests {
    use ansilo_core::{
        common::data::{EncodingType, VarcharOptions},
        config::{EntityAttributeConfig, EntitySourceConfig, EntityVersionConfig},
    };

    use super::*;

    fn mock_data() -> (ConnectorEntityConfig<()>, MemoryConnectionConfig) {
        let mut conf = MemoryConnectionConfig::new();
        let mut entities = ConnectorEntityConfig::new();

        entities.add(EntitySource::minimal(
            "people",
            EntityVersionConfig::minimal(
                "1.0",
                vec![
                    EntityAttributeConfig::minimal("first_name", DataType::rust_string()),
                    EntityAttributeConfig::minimal("last_name", DataType::rust_string()),
                ],
                EntitySourceConfig::minimal(""),
            ),
            (),
        ));

        conf.set_data(
            "people",
            "1.0",
            vec![
                vec![DataValue::from("Mary"), DataValue::from("Jane")],
                vec![DataValue::from("John"), DataValue::from("Smith")],
            ],
        );

        (entities, conf)
    }

    fn create_executor(query: sqlil::Select, params: Vec<DataValue>) -> MemoryQueryExecutor {
        let (entities, data) = mock_data();

        MemoryQueryExecutor::new(Arc::new(data), entities, query, params)
    }

    #[test]
    fn test_memory_connector_exector_select_all() {
        let mut select = sqlil::Select::new(sqlil::entity("people", "1.0"));
        select.cols.push((
            "first_name".to_string(),
            sqlil::Expr::attr("people", "1.0", "first_name"),
        ));
        select.cols.push((
            "last_name".to_string(),
            sqlil::Expr::attr("people", "1.0", "last_name"),
        ));

        let executor = create_executor(select, vec![]);

        let results = executor.run().unwrap();

        assert_eq!(
            results,
            MemoryResultSet::new(
                vec![
                    (
                        "first_name".to_string(),
                        DataType::Varchar(VarcharOptions::new(None, EncodingType::Utf8))
                    ),
                    (
                        "last_name".to_string(),
                        DataType::Varchar(VarcharOptions::new(None, EncodingType::Utf8))
                    ),
                ],
                vec![
                    vec![
                        DataValue::Varchar("Mary".as_bytes().to_vec()),
                        DataValue::Varchar("Jane".as_bytes().to_vec())
                    ],
                    vec![
                        DataValue::Varchar("John".as_bytes().to_vec()),
                        DataValue::Varchar("Smith".as_bytes().to_vec())
                    ],
                ]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_exector_select_invalid_entity() {
        let select = sqlil::Select::new(sqlil::entity("invalid", "1.0"));

        let executor = create_executor(select, vec![]);

        executor.run().unwrap_err();
    }

    #[test]
    fn test_memory_connector_exector_select_invalid_version() {
        let select = sqlil::Select::new(sqlil::entity("people", "invalid"));

        let executor = create_executor(select, vec![]);

        executor.run().unwrap_err();
    }

    #[test]
    fn test_memory_connector_exector_select_no_cols() {
        let select = sqlil::Select::new(sqlil::entity("people", "1.0"));

        let executor = create_executor(select, vec![]);

        let results = executor.run().unwrap();

        assert_eq!(
            results,
            MemoryResultSet::new(vec![], vec![vec![], vec![]]).unwrap()
        );
    }

    #[test]
    fn test_memory_connector_exector_select_single_column() {
        let mut select = sqlil::Select::new(sqlil::entity("people", "1.0"));
        select.cols.push((
            "alias".to_string(),
            sqlil::Expr::attr("people", "1.0", "first_name"),
        ));

        let executor = create_executor(select, vec![]);

        let results = executor.run().unwrap();

        assert_eq!(
            results,
            MemoryResultSet::new(
                vec![(
                    "alias".to_string(),
                    DataType::Varchar(VarcharOptions::new(None, EncodingType::Utf8))
                ),],
                vec![
                    vec![DataValue::Varchar("Mary".as_bytes().to_vec()),],
                    vec![DataValue::Varchar("John".as_bytes().to_vec()),],
                ]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_exector_select_where_equals() {
        let mut select = sqlil::Select::new(sqlil::entity("people", "1.0"));
        select.cols.push((
            "alias".to_string(),
            sqlil::Expr::attr("people", "1.0", "first_name"),
        ));

        select
            .r#where
            .push(sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::attr("people", "1.0", "first_name"),
                sqlil::BinaryOpType::Equal,
                sqlil::Expr::Constant(sqlil::Constant::new(DataValue::from("Mary"))),
            )));

        let executor = create_executor(select, vec![]);

        let results = executor.run().unwrap();

        assert_eq!(
            results,
            MemoryResultSet::new(
                vec![(
                    "alias".to_string(),
                    DataType::Varchar(VarcharOptions::new(None, EncodingType::Utf8))
                ),],
                vec![vec![DataValue::Varchar("Mary".as_bytes().to_vec()),],]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_exector_select_skip_row() {
        let mut select = sqlil::Select::new(sqlil::entity("people", "1.0"));
        select.cols.push((
            "alias".to_string(),
            sqlil::Expr::EntityVersionAttribute(sqlil::attr("people", "1.0", "first_name")),
        ));

        select.row_skip = 1;

        let executor = create_executor(select, vec![]);
        let results = executor.run().unwrap();

        assert_eq!(
            results,
            MemoryResultSet::new(
                vec![(
                    "alias".to_string(),
                    DataType::Varchar(VarcharOptions::new(None, EncodingType::Utf8))
                ),],
                vec![vec![DataValue::Varchar("John".as_bytes().to_vec()),],]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_exector_select_row_limit() {
        let mut select = sqlil::Select::new(sqlil::entity("people", "1.0"));
        select.cols.push((
            "alias".to_string(),
            sqlil::Expr::EntityVersionAttribute(sqlil::attr("people", "1.0", "first_name")),
        ));

        select.row_limit = Some(1);

        let executor = create_executor(select, vec![]);
        let results = executor.run().unwrap();

        assert_eq!(
            results,
            MemoryResultSet::new(
                vec![(
                    "alias".to_string(),
                    DataType::Varchar(VarcharOptions::new(None, EncodingType::Utf8))
                ),],
                vec![vec![DataValue::Varchar("Mary".as_bytes().to_vec()),],]
            )
            .unwrap()
        )
    }
}

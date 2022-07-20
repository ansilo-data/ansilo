use std::sync::Arc;

use ansilo_core::{
    common::data::{DataType, DataValue},
    err::{bail, Error, Result},
    sqlil,
};
use itertools::Itertools;

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

        let mut filtered = vec![];
        for row in source {
            if self.satisfies_where(row)? {
                filtered.push(row);
            }
        }

        let mut results: Vec<Vec<DataValue>> = if self.is_aggregated() {
            let keys = filtered
                .iter()
                .map(|i| self.grouping_key(i))
                .collect::<Result<Vec<_>>>()?;

            filtered
                .into_iter()
                .zip(keys.iter())
                .group_by(|(_, key)| *key)
                .into_iter()
                .map(|(_, i)| self.project_group(&i.map(|(row, _)| row.clone()).collect()))
                .try_collect()?
        } else {
            filtered
                .into_iter()
                .map(|i| self.project(i))
                .try_collect()?
        };

        if self.query.row_skip > 0 {
            results = results.into_iter().skip(self.query.row_skip as _).collect();
        }

        if let Some(limit) = self.query.row_limit {
            results = results.into_iter().take(limit as _).collect();
        }

        MemoryResultSet::new(self.cols()?, results)
    }

    fn satisfies_where(&self, row: &Vec<DataValue>) -> Result<bool> {
        let mut res = true;

        let row = DataContext::Row(row.clone());
        for cond in self.query.r#where.iter() {
            let out = match self.evaluate(&row, cond)?.as_cell()? {
                DataValue::Boolean(out) => out,
                _ => false,
            };

            res = res && out;
        }

        Ok(res)
    }

    fn project(&self, row: &Vec<DataValue>) -> Result<Vec<DataValue>> {
        self.project_row(row, &self.query.cols.iter().map(|i| i.1.clone()).collect())
    }

    fn project_row(&self, row: &Vec<DataValue>, cols: &Vec<sqlil::Expr>) -> Result<Vec<DataValue>> {
        let mut res = vec![];

        let row = DataContext::Row(row.clone());
        for expr in cols {
            res.push(self.evaluate(&row, expr)?.as_cell()?);
        }

        Ok(res)
    }

    fn is_aggregated(&self) -> bool {
        !self.query.group_bys.is_empty()
            || self
                .query
                .cols
                .iter()
                .any(|(_, i)| i.walk_any(|i| matches!(i, sqlil::Expr::AggregateCall(_))))
    }

    fn grouping_key(&self, row: &Vec<DataValue>) -> Result<Vec<DataValue>> {
        assert!(self.is_aggregated());

        if self.query.group_bys.is_empty() {
            return Ok(vec![DataValue::Boolean(true)]);
        }

        self.project_row(row, &self.query.group_bys)
    }

    fn project_group(&self, group: &Vec<Vec<DataValue>>) -> Result<Vec<DataValue>> {
        let mut res = vec![];

        let group = DataContext::Group(group.clone());
        for (_, expr) in self.query.cols.iter() {
            res.push(self.evaluate(&group, expr)?.as_cell()?);
        }

        Ok(res)
    }

    fn evaluate(&self, data: &DataContext, expr: &sqlil::Expr) -> Result<DataContext> {
        Ok(match expr {
            sqlil::Expr::EntityVersion(_) => bail!("Cannot reference entity without attribute"),
            sqlil::Expr::EntityVersionAttribute(a) => {
                let attr_idx = self.get_attr_index(a)?;

                match data {
                    DataContext::Row(row) => DataContext::Cell(row[attr_idx].clone()),
                    DataContext::Group(group) if self.query.group_bys.contains(expr) => {
                        DataContext::Cell(group.first().unwrap()[attr_idx].clone())
                    }
                    DataContext::Group(group) => {
                        DataContext::Row(group.into_iter().map(|r| r[attr_idx].clone()).collect())
                    }
                    _ => bail!("Unexpected cell"),
                }
            }
            sqlil::Expr::Constant(v) => DataContext::Cell(v.value.clone()),
            sqlil::Expr::Parameter(_) => todo!(),
            sqlil::Expr::UnaryOp(_) => todo!(),
            sqlil::Expr::BinaryOp(op) => {
                let left = self.evaluate(data, &op.left)?.as_cell()?;
                let right = self.evaluate(data, &op.right)?.as_cell()?;

                if op.r#type != sqlil::BinaryOpType::NullSafeEqual
                    && (left.is_null() || right.is_null())
                {
                    return Ok(DataContext::Cell(DataValue::Null));
                }

                DataContext::Cell(match op.r#type {
                    sqlil::BinaryOpType::Add => todo!(),
                    sqlil::BinaryOpType::Subtract => todo!(),
                    sqlil::BinaryOpType::Multiply => todo!(),
                    sqlil::BinaryOpType::Divide => todo!(),
                    sqlil::BinaryOpType::Modulo => todo!(),
                    sqlil::BinaryOpType::Exponent => todo!(),
                    sqlil::BinaryOpType::LogicalAnd => todo!(),
                    sqlil::BinaryOpType::LogicalOr => todo!(),
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
                    sqlil::BinaryOpType::NullSafeEqual => DataValue::Boolean(left == right),
                    sqlil::BinaryOpType::NotEqual => DataValue::Boolean(left != right),
                    sqlil::BinaryOpType::GreaterThan => todo!(),
                    sqlil::BinaryOpType::GreaterThanOrEqual => todo!(),
                    sqlil::BinaryOpType::LessThan => todo!(),
                    sqlil::BinaryOpType::LessThanOrEqual => todo!(),
                })
            }
            sqlil::Expr::Cast(_) => todo!(),
            sqlil::Expr::FunctionCall(_) => todo!(),
            sqlil::Expr::AggregateCall(sqlil::AggregateCall::Count) => {
                DataContext::Cell(DataValue::UInt64(data.as_group_ref()?.len() as _))
            }
            _ => todo!(),
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
                    sqlil::BinaryOpType::LogicalAnd => DataType::Boolean,
                    sqlil::BinaryOpType::LogicalOr => DataType::Boolean,
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
                    sqlil::BinaryOpType::NullSafeEqual => DataType::Boolean,
                    sqlil::BinaryOpType::NotEqual => DataType::Boolean,
                    sqlil::BinaryOpType::GreaterThan => todo!(),
                    sqlil::BinaryOpType::GreaterThanOrEqual => todo!(),
                    sqlil::BinaryOpType::LessThan => todo!(),
                    sqlil::BinaryOpType::LessThanOrEqual => todo!(),
                }
            }
            sqlil::Expr::Cast(_) => todo!(),
            sqlil::Expr::FunctionCall(_) => todo!(),
            sqlil::Expr::AggregateCall(sqlil::AggregateCall::Count) => DataType::UInt64,
            _ => todo!(),
        })
    }

    fn get_conf(&self, a: &sqlil::EntityVersionAttributeIdentifier) -> Result<EntitySource<()>> {
        let entity = self
            .entities
            .find(&a.entity)
            .ok_or(Error::msg("Could not find entity"))?;

        Ok(entity.clone())
    }

    fn get_attr_index(&self, a: &sqlil::EntityVersionAttributeIdentifier) -> Result<usize> {
        let entity = self.get_conf(a)?;
        entity
            .version()
            .attributes
            .iter()
            .position(|i| i.id == a.attribute_id)
            .ok_or(Error::msg("Could not find attr"))
    }
}

enum DataContext {
    Cell(DataValue),
    Row(Vec<DataValue>),
    Group(Vec<Vec<DataValue>>),
}

impl DataContext {
    fn as_cell(self) -> Result<DataValue> {
        if let Self::Cell(v) = self {
            Ok(v)
        } else {
            bail!("Found {} in cell context", self.r#type())
        }
    }

    fn as_row(self) -> Result<Vec<DataValue>> {
        if let Self::Row(v) = self {
            Ok(v)
        } else {
            bail!("Found {} in single row context", self.r#type())
        }
    }

    fn as_group(self) -> Result<Vec<Vec<DataValue>>> {
        if let Self::Group(v) = self {
            Ok(v)
        } else {
            bail!("Found {} in row group context", self.r#type())
        }
    }

    fn as_cell_ref(&self) -> Result<&DataValue> {
        if let Self::Cell(v) = self {
            Ok(v)
        } else {
            bail!("Found {} in cell context", self.r#type())
        }
    }

    fn as_row_ref(&self) -> Result<&Vec<DataValue>> {
        if let Self::Row(v) = self {
            Ok(v)
        } else {
            bail!("Found {} in single row context", self.r#type())
        }
    }

    fn as_group_ref(&self) -> Result<&Vec<Vec<DataValue>>> {
        if let Self::Group(v) = self {
            Ok(v)
        } else {
            bail!("Found {} in row group context", self.r#type())
        }
    }

    fn r#type(&self) -> &'static str {
        match self {
            DataContext::Cell(_) => "cell",
            DataContext::Row(_) => "row",
            DataContext::Group(_) => "row group",
        }
    }
}

#[cfg(test)]
mod tests {
    use ansilo_core::{
        common::data::{EncodingType, VarcharOptions},
        config::{EntityAttributeConfig, EntitySourceConfig, EntityVersionConfig},
        sqlil::AggregateCall,
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
    fn test_memory_connector_executor_select_all() {
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
    fn test_memory_connector_executor_select_invalid_entity() {
        let select = sqlil::Select::new(sqlil::entity("invalid", "1.0"));

        let executor = create_executor(select, vec![]);

        executor.run().unwrap_err();
    }

    #[test]
    fn test_memory_connector_executor_select_invalid_version() {
        let select = sqlil::Select::new(sqlil::entity("people", "invalid"));

        let executor = create_executor(select, vec![]);

        executor.run().unwrap_err();
    }

    #[test]
    fn test_memory_connector_executor_select_no_cols() {
        let select = sqlil::Select::new(sqlil::entity("people", "1.0"));

        let executor = create_executor(select, vec![]);

        let results = executor.run().unwrap();

        assert_eq!(
            results,
            MemoryResultSet::new(vec![], vec![vec![], vec![]]).unwrap()
        );
    }

    #[test]
    fn test_memory_connector_executor_select_single_column() {
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
    fn test_memory_connector_executor_select_where_equals() {
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
    fn test_memory_connector_executor_select_skip_row() {
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
    fn test_memory_connector_executor_select_row_limit() {
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

    #[test]
    fn test_memory_connector_executor_select_group_by_column_key() {
        let mut select = sqlil::Select::new(sqlil::entity("people", "1.0"));
        select.cols.push((
            "alias".to_string(),
            sqlil::Expr::EntityVersionAttribute(sqlil::attr("people", "1.0", "first_name")),
        ));

        select
            .group_bys
            .push(sqlil::Expr::EntityVersionAttribute(sqlil::attr(
                "people",
                "1.0",
                "first_name",
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
                vec![
                    vec![DataValue::Varchar("Mary".as_bytes().to_vec()),],
                    vec![DataValue::Varchar("John".as_bytes().to_vec()),],
                ]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_select_group_by_column_key_with_count() {
        let mut select = sqlil::Select::new(sqlil::entity("people", "1.0"));
        select.cols.push((
            "alias".to_string(),
            sqlil::Expr::EntityVersionAttribute(sqlil::attr("people", "1.0", "first_name")),
        ));
        select.cols.push((
            "count".to_string(),
            sqlil::Expr::AggregateCall(AggregateCall::Count),
        ));

        select
            .group_bys
            .push(sqlil::Expr::EntityVersionAttribute(sqlil::attr(
                "people",
                "1.0",
                "first_name",
            )));

        let executor = create_executor(select, vec![]);
        let results = executor.run().unwrap();

        assert_eq!(
            results,
            MemoryResultSet::new(
                vec![
                    (
                        "alias".to_string(),
                        DataType::Varchar(VarcharOptions::new(None, EncodingType::Utf8))
                    ),
                    ("count".to_string(), DataType::UInt64,)
                ],
                vec![
                    vec![
                        DataValue::Varchar("Mary".as_bytes().to_vec()),
                        DataValue::UInt64(1)
                    ],
                    vec![
                        DataValue::Varchar("John".as_bytes().to_vec()),
                        DataValue::UInt64(1)
                    ],
                ]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_select_count_implicit_group_by() {
        let mut select = sqlil::Select::new(sqlil::entity("people", "1.0"));
        select.cols.push((
            "count".to_string(),
            sqlil::Expr::AggregateCall(AggregateCall::Count),
        ));

        let executor = create_executor(select, vec![]);
        let results = executor.run().unwrap();

        assert_eq!(
            results,
            MemoryResultSet::new(
                vec![(
                    "count".to_string(),
                    DataType::UInt64,
                )],
                vec![vec![DataValue::UInt64(2)],]
            )
            .unwrap()
        )
    }
}

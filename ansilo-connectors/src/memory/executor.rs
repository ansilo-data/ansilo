use std::{
    cmp,
    collections::{HashMap, HashSet},
    iter,
    sync::Arc,
};

use ansilo_core::{
    config::EntityAttributeConfig,
    data::{DataType, DataValue, StringOptions},
    err::{bail, Context, Error, Result},
    sqlil::{self},
};
use itertools::Itertools;

use crate::common::entity::{ConnectorEntityConfig, EntitySource};

use super::{MemoryConnectionConfig, MemoryConnectorEntitySourceConfig, MemoryResultSet};

#[derive(Debug, Clone)]
pub(crate) struct MemoryQueryExecutor {
    data: Arc<MemoryConnectionConfig>,
    entities: ConnectorEntityConfig<MemoryConnectorEntitySourceConfig>,
    query: sqlil::Query,
    params: HashMap<u32, DataValue>,
}

/// This entire implementation is garbage but it doesn't matter as this is used
/// as a testing instrument.
impl MemoryQueryExecutor {
    pub(crate) fn new(
        data: Arc<MemoryConnectionConfig>,
        entities: ConnectorEntityConfig<MemoryConnectorEntitySourceConfig>,
        query: sqlil::Query,
        params: HashMap<u32, DataValue>,
    ) -> Self {
        Self {
            data,
            entities,
            query,
            params,
        }
    }

    pub(crate) fn run(&self) -> Result<MemoryResultSet> {
        match &self.query {
            sqlil::Query::Select(select) => self.run_select(select),
            sqlil::Query::Insert(insert) => self.run_insert(insert),
            sqlil::Query::Update(update) => self.run_update(update),
            sqlil::Query::Delete(delete) => self.run_delete(delete),
        }
    }

    fn run_select(&self, select: &sqlil::Select) -> Result<MemoryResultSet> {
        let mut source = self.get_entity_data(&select.from)?;
        let mut source_entity = &select.from;

        for join in select.joins.iter() {
            let inner = self.get_entity_data(&join.target)?;

            source = self.perform_join(source_entity, join, &source, &inner)?;
            source_entity = &join.target;
        }

        let mut filtered = vec![];
        for row in source {
            if self.satisfies_where(&row)? {
                filtered.push(row);
            }
        }

        let mut results: Vec<Vec<DataValue>> = if self.is_aggregated() {
            let mut groups = self.group(filtered)?;

            groups = self.sort(groups, |r| self.group_sort_key(r))?;

            groups
                .into_iter()
                .map(|g| self.project_group(&g))
                .try_collect()?
        } else {
            filtered = self.sort(filtered, |r| self.sort_key(r))?;

            filtered
                .into_iter()
                .map(|i| self.project(&i))
                .try_collect()?
        };

        if select.row_skip > 0 {
            results = results.into_iter().skip(select.row_skip as _).collect();
        }

        if let Some(limit) = select.row_limit {
            results = results.into_iter().take(limit as _).collect();
        }

        MemoryResultSet::new(self.cols()?, results)
    }

    fn run_insert(&self, insert: &sqlil::Insert) -> Result<MemoryResultSet> {
        self.update_entity_data(&insert.target, |rows| {
            let attrs = self.get_attrs(&insert.target.entity)?;
            let ctx = DataContext::Cell(DataValue::Null);

            let mut row = attrs
                .iter()
                .map(|a| {
                    (
                        &a.r#type,
                        insert.cols.iter().find(|(attr, _)| attr == &a.id),
                    )
                })
                .map(|(t, a)| (t, a.map(|(_, expr)| self.evaluate(&ctx, expr))))
                .map(|(t, a)| (t, a.unwrap_or(Ok(DataContext::Cell(DataValue::Null)))))
                .map(|(t, a)| (t, a.and_then(|a| a.as_cell())))
                .map(|(t, a)| a.and_then(|a| a.try_coerce_into(t)))
                .collect::<Result<Vec<_>>>()?;

            // Append null rowid
            row.push(DataValue::Null);

            rows.push(row);

            Ok(())
        })?;

        Ok(MemoryResultSet::empty())
    }

    fn run_update(&self, update: &sqlil::Update) -> Result<MemoryResultSet> {
        self.update_entity_data(&update.target, |rows| {
            let attrs = self.get_attrs(&update.target.entity)?;

            for row in rows.iter_mut() {
                if !self.satisfies_where(row)? {
                    continue;
                }

                let ctx = DataContext::Row(row.clone());

                for (attr, expr) in update.cols.iter() {
                    let pos = attrs
                        .iter()
                        .position(|a| &a.id == attr)
                        .ok_or(Error::msg("Unknown attr"))?;

                    row[pos] = self
                        .evaluate(&ctx, expr)?
                        .as_cell()?
                        .try_coerce_into(&attrs[pos].r#type)?;
                }
            }

            Ok(())
        })?;

        Ok(MemoryResultSet::empty())
    }

    fn run_delete(&self, delete: &sqlil::Delete) -> Result<MemoryResultSet> {
        self.update_entity_data(&delete.target, |rows| {
            let mut i = 0;

            while i < rows.len() {
                if self.satisfies_where(&rows[i])? {
                    rows.remove(i);
                } else {
                    i += 1;
                }
            }

            Ok(())
        })?;

        Ok(MemoryResultSet::empty())
    }

    fn get_entity_data(&self, s: &sqlil::EntitySource) -> Result<Vec<Vec<DataValue>>> {
        self.data
            .with_data(&s.entity.entity_id, &s.entity.version_id, |rows| {
                Self::with_row_ids(rows.clone())
            })
            .ok_or(Error::msg("Could not find entity"))
    }

    fn update_entity_data(
        &self,
        s: &sqlil::EntitySource,
        cb: impl FnOnce(&mut Vec<Vec<DataValue>>) -> Result<()>,
    ) -> Result<()> {
        self.data
            .with_data_mut(&s.entity.entity_id, &s.entity.version_id, move |rows| {
                let mut rows_with_id = Self::with_row_ids(rows.clone());
                cb(&mut rows_with_id)?;
                *rows = Self::without_row_ids(rows_with_id);

                Ok(())
            })
            .ok_or(Error::msg("Could not find entity"))?
    }

    /// Append a row id (the index of the row) to each row
    fn with_row_ids(mut rows: Vec<Vec<DataValue>>) -> Vec<Vec<DataValue>> {
        for (rowid, row) in rows.iter_mut().enumerate() {
            row.push(DataValue::UInt64(rowid as _));
        }

        rows
    }

    fn without_row_ids(mut rows: Vec<Vec<DataValue>>) -> Vec<Vec<DataValue>> {
        for (_, row) in rows.iter_mut().enumerate() {
            row.remove(row.len() - 1);
        }

        rows
    }

    fn perform_join(
        &self,
        source: &sqlil::EntitySource,
        join: &sqlil::Join,
        outer: &Vec<Vec<DataValue>>,
        inner: &Vec<Vec<DataValue>>,
    ) -> Result<Vec<Vec<DataValue>>> {
        let mut results = vec![];

        let mut outer_joined = HashSet::new();
        let mut inner_joined = HashSet::new();

        for (i, outer_row) in outer.iter().enumerate() {
            for (j, inner_row) in inner.iter().enumerate() {
                let joined_row = outer_row
                    .iter()
                    .chain(inner_row)
                    .cloned()
                    .collect::<Vec<_>>();
                let data = DataContext::Row(joined_row.clone());

                let join_result = join
                    .conds
                    .iter()
                    .map(|cond| {
                        self.evaluate(&data, cond)
                            .and_then(|i| i.as_cell())
                            .and_then(|i| i.try_coerce_into(&DataType::Boolean))
                    })
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .all(|i| matches!(i, DataValue::Boolean(true)));

                if join_result {
                    outer_joined.insert(i);
                    inner_joined.insert(j);
                    results.push(joined_row);
                }
            }
        }

        if join.r#type.is_left() || join.r#type.is_full() {
            let nulls = self.get_attrs(&join.target.entity)?.len() + 1;
            let nulls = iter::repeat(DataValue::Null)
                .take(nulls)
                .collect::<Vec<_>>();

            for (i, outer_row) in outer.iter().enumerate() {
                if !outer_joined.contains(&i) {
                    let joined_row = outer_row.iter().chain(&nulls).cloned().collect::<Vec<_>>();
                    results.push(joined_row);
                }
            }
        }

        if join.r#type.is_right() || join.r#type.is_full() {
            let nulls = self.get_attrs(&source.entity)?.len() + 1;
            let nulls = iter::repeat(DataValue::Null)
                .take(nulls)
                .collect::<Vec<_>>();

            for (i, inner_row) in inner.iter().enumerate() {
                if !inner_joined.contains(&i) {
                    let joined_row = nulls.iter().chain(inner_row).cloned().collect::<Vec<_>>();
                    results.push(joined_row);
                }
            }
        }

        Ok(results)
    }

    fn satisfies_where(&self, row: &Vec<DataValue>) -> Result<bool> {
        let mut res = true;

        let row = DataContext::Row(row.clone());
        for cond in self.query.r#where().iter() {
            let out = match self.evaluate(&row, cond)?.as_cell()? {
                DataValue::Boolean(out) => out,
                _ => false,
            };

            res = res && out;
        }

        Ok(res)
    }

    fn project(&self, row: &Vec<DataValue>) -> Result<Vec<DataValue>> {
        self.project_row(
            row,
            &self
                .query
                .as_select()
                .unwrap()
                .cols
                .iter()
                .map(|i| i.1.clone())
                .collect(),
        )
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
        !self.query.as_select().unwrap().group_bys.is_empty()
            || self
                .query
                .as_select()
                .unwrap()
                .cols
                .iter()
                .any(|(_, i)| i.walk_any(|i| matches!(i, sqlil::Expr::AggregateCall(_))))
    }

    fn grouping_key(&self, row: &Vec<DataValue>) -> Result<Vec<DataValue>> {
        assert!(self.is_aggregated());

        if self.query.as_select().unwrap().group_bys.is_empty() {
            return Ok(vec![DataValue::Boolean(true)]);
        }

        self.project_row(row, &self.query.as_select().unwrap().group_bys)
    }

    fn group(&self, rows: Vec<Vec<DataValue>>) -> Result<Vec<Vec<Vec<DataValue>>>> {
        let mut groups = Vec::<(Vec<DataValue>, Vec<Vec<DataValue>>)>::new();

        for row in rows.into_iter() {
            let key = self.grouping_key(&row)?;
            if let Some((_, group)) = groups.iter_mut().find(|(k, _)| k == &key) {
                group.push(row);
            } else {
                groups.push((key, vec![row]));
            }
        }

        let groups = groups.into_iter().map(|(_, g)| g).collect();

        Ok(groups)
    }

    fn project_group(&self, group_rows: &Vec<Vec<DataValue>>) -> Result<Vec<DataValue>> {
        let mut res = vec![];

        let group = DataContext::Group(group_rows.clone());
        for (_, expr) in self.query.as_select().unwrap().cols.iter() {
            res.push(self.grouping_expr(expr, group_rows, &group)?);
        }

        Ok(res)
    }

    fn grouping_expr(
        &self,
        expr: &sqlil::Expr,
        group_rows: &Vec<Vec<DataValue>>,
        group: &DataContext,
    ) -> Result<DataValue, Error> {
        Ok(
            if self.query.as_select().unwrap().group_bys.contains(expr) {
                self.evaluate(&DataContext::Row(group_rows[0].clone()), expr)?
                    .as_cell()?
            } else {
                self.evaluate(group, expr)?.as_cell()?
            },
        )
    }

    fn sort<R: Clone, K: Fn(&R) -> Result<Vec<Ordered<DataValue>>>>(
        &self,
        rows: Vec<R>,
        key_fn: K,
    ) -> Result<Vec<R>> {
        if self.query.as_select().unwrap().order_bys.is_empty() {
            return Ok(rows.clone());
        }

        let mut to_sort = rows
            .into_iter()
            .map(|i| key_fn(&i).map(|key| (i, key)))
            .collect::<Result<Vec<_>>>()?;

        to_sort.sort_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(cmp::Ordering::Equal));

        Ok(to_sort.into_iter().map(|(row, _)| row).collect())
    }

    fn sort_key(&self, row: &Vec<DataValue>) -> Result<Vec<Ordered<DataValue>>> {
        assert!(!self.query.as_select().unwrap().order_bys.is_empty());

        let row = DataContext::Row(row.clone());
        let mut keys = vec![];

        for ordering in self.query.as_select().unwrap().order_bys.iter() {
            let key = self.evaluate(&row, &ordering.expr)?.as_cell()?;

            keys.push(Ordered::new(ordering.r#type, key));
        }

        Ok(keys)
    }

    fn group_sort_key(&self, group_rows: &Vec<Vec<DataValue>>) -> Result<Vec<Ordered<DataValue>>> {
        assert!(!self.query.as_select().unwrap().order_bys.is_empty());

        let group = DataContext::Group(group_rows.clone());
        let mut keys = vec![];

        for ordering in self.query.as_select().unwrap().order_bys.iter() {
            let key = self.grouping_expr(&ordering.expr, group_rows, &group)?;

            keys.push(Ordered::new(ordering.r#type, key));
        }

        Ok(keys)
    }

    fn evaluate(&self, data: &DataContext, expr: &sqlil::Expr) -> Result<DataContext> {
        Ok(match expr {
            sqlil::Expr::Attribute(a) => {
                let attr_idx = self.get_attr_index(a)?;

                match data {
                    DataContext::Row(row) => DataContext::Cell(row[attr_idx].clone()),
                    DataContext::Group(group) => {
                        DataContext::Row(group.into_iter().map(|r| r[attr_idx].clone()).collect())
                    }
                    _ => bail!("Unexpected cell"),
                }
            }
            sqlil::Expr::Constant(v) => DataContext::Cell(v.value.clone()),
            sqlil::Expr::Parameter(param) => DataContext::Cell(
                self.params
                    .get(&param.id)
                    .context("Unknown parameter id")?
                    .clone(),
            ),
            sqlil::Expr::UnaryOp(_) => todo!(),
            sqlil::Expr::BinaryOp(op) => {
                let left = self.evaluate(data, &op.left)?.as_cell()?;
                let right = self.evaluate(data, &op.right)?.as_cell()?;

                if op.r#type != sqlil::BinaryOpType::NullSafeEqual
                    && (left.is_null() || right.is_null())
                {
                    return Ok(DataContext::Cell(DataValue::Null));
                }

                let (left, right) = if left.r#type() != right.r#type() {
                    if let Ok(coerced) = right.clone().try_coerce_into(&left.r#type()) {
                        (left, coerced)
                    } else if let Ok(coerced) = left.clone().try_coerce_into(&right.r#type()) {
                        (coerced, right)
                    } else {
                        (left, right)
                    }
                } else {
                    (left, right)
                };

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
                    sqlil::BinaryOpType::Concat => {
                        let string = DataType::Utf8String(StringOptions::default());
                        let left = left.try_coerce_into(&string)?;
                        let right = right.try_coerce_into(&string)?;

                        match (left, right) {
                            (DataValue::Utf8String(mut left), DataValue::Utf8String(mut right)) => {
                                left.append(&mut right);
                                DataValue::Utf8String(left)
                            }
                            _ => unreachable!(),
                        }
                    }
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
            .as_select()
            .unwrap()
            .cols
            .iter()
            .map(|(s, e)| Ok((s.clone(), self.evaluate_type(e)?)))
            .collect()
    }

    fn evaluate_type(&self, e: &sqlil::Expr) -> Result<DataType> {
        Ok(match e {
            sqlil::Expr::Attribute(a) => self.get_attr(a)?.r#type.clone(),
            sqlil::Expr::Constant(v) => (&v.value).into(),
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
                    sqlil::BinaryOpType::Concat => DataType::Utf8String(StringOptions::default()),
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

    fn get_conf(
        &self,
        e: &sqlil::EntityVersionIdentifier,
    ) -> Result<&EntitySource<MemoryConnectorEntitySourceConfig>> {
        let entity = self
            .entities
            .find(e)
            .ok_or(Error::msg("Could not find entity"))?;

        Ok(entity)
    }

    fn get_attrs(&self, a: &sqlil::EntityVersionIdentifier) -> Result<&Vec<EntityAttributeConfig>> {
        let entity = self.get_conf(a)?;
        Ok(&entity.version().attributes)
    }

    fn get_attr(&self, a: &sqlil::AttributeIdentifier) -> Result<EntityAttributeConfig> {
        let entity = self.query.get_entity(&a.entity_alias)?;

        if a.attribute_id == "ROWIDX" {
            return Ok(EntityAttributeConfig::minimal("ROWIDX", DataType::UInt64));
        }

        self.get_attrs(entity)?
            .iter()
            .find(|i| i.id == a.attribute_id)
            .cloned()
            .ok_or(Error::msg("Could not find attr"))
    }

    fn get_attr_index(&self, a: &sqlil::AttributeIdentifier) -> Result<usize> {
        let pos: usize = self
            .query
            .get_entity_sources()
            .take_while(|e| e.alias != a.entity_alias)
            // add +1 for the row id appended to each row
            .map(|e| self.get_attrs(&e.entity).unwrap().len() + 1)
            .sum();

        let entity = self.query.get_entity(&a.entity_alias)?;

        if a.attribute_id == "ROWIDX" {
            // Row id is appended to each row
            Ok(pos + self.get_attrs(entity)?.len())
        } else {
            Ok(pos
                + self
                    .get_attrs(entity)?
                    .iter()
                    .position(|i| i.id == a.attribute_id)
                    .ok_or(Error::msg("Could not find attr"))?)
        }
    }
}

enum DataContext {
    Cell(DataValue),
    Row(Vec<DataValue>),
    Group(Vec<Vec<DataValue>>),
}

#[allow(unused)]
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

#[derive(PartialEq, Clone, Debug)]
enum Ordered<T: PartialOrd> {
    Asc(T),
    Desc(T),
}

impl<T: PartialOrd> Ordered<T> {
    pub(crate) fn new(r#type: sqlil::OrderingType, key: T) -> Self {
        match r#type {
            sqlil::OrderingType::Asc => Self::Asc(key),
            sqlil::OrderingType::Desc => Self::Desc(key),
        }
    }
}

impl<T: PartialOrd> PartialOrd for Ordered<T> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        let (a, b, rev) = match (self, other) {
            (Self::Asc(a), Self::Asc(b)) => (a, b, false),
            (Self::Desc(a), Self::Desc(b)) => (a, b, true),
            _ => panic!("Sort ordering mismatch"),
        };

        let cmp = a.partial_cmp(b);

        cmp.map(|cmp| if rev { cmp.reverse() } else { cmp })
    }
}

#[cfg(test)]
mod tests {
    use ansilo_core::{
        config::{EntityAttributeConfig, EntitySourceConfig, EntityVersionConfig},
        data::StringOptions,
        sqlil::{AggregateCall, Ordering},
    };

    use super::*;

    fn mock_data() -> (
        ConnectorEntityConfig<MemoryConnectorEntitySourceConfig>,
        MemoryConnectionConfig,
    ) {
        let conf = MemoryConnectionConfig::new();
        let mut entities = ConnectorEntityConfig::new();

        entities.add(EntitySource::minimal(
            "people",
            EntityVersionConfig::minimal(
                "1.0",
                vec![
                    EntityAttributeConfig::minimal("id", DataType::UInt32),
                    EntityAttributeConfig::minimal("first_name", DataType::rust_string()),
                    EntityAttributeConfig::minimal("last_name", DataType::rust_string()),
                ],
                EntitySourceConfig::minimal(""),
            ),
            MemoryConnectorEntitySourceConfig::default(),
        ));

        entities.add(EntitySource::minimal(
            "pets",
            EntityVersionConfig::minimal(
                "1.0",
                vec![
                    EntityAttributeConfig::minimal("id", DataType::UInt32),
                    EntityAttributeConfig::minimal("owner_id", DataType::UInt32),
                    EntityAttributeConfig::minimal("pet_name", DataType::rust_string()),
                ],
                EntitySourceConfig::minimal(""),
            ),
            MemoryConnectorEntitySourceConfig::default(),
        ));

        conf.set_data(
            "people",
            "1.0",
            vec![
                vec![
                    DataValue::UInt32(1),
                    DataValue::from("Mary"),
                    DataValue::from("Jane"),
                ],
                vec![
                    DataValue::UInt32(2),
                    DataValue::from("John"),
                    DataValue::from("Smith"),
                ],
                vec![
                    DataValue::UInt32(3),
                    DataValue::from("Mary"),
                    DataValue::from("Bennet"),
                ],
            ],
        );

        conf.set_data(
            "pets",
            "1.0",
            vec![
                vec![
                    DataValue::UInt32(1),
                    DataValue::UInt32(1),
                    DataValue::from("Pepper"),
                ],
                vec![
                    DataValue::UInt32(2),
                    DataValue::UInt32(1),
                    DataValue::from("Salt"),
                ],
                vec![
                    DataValue::UInt32(3),
                    DataValue::UInt32(3),
                    DataValue::from("Relish"),
                ],
                vec![
                    DataValue::UInt32(4),
                    DataValue::Null,
                    DataValue::from("Luna"),
                ],
            ],
        );

        (entities, conf)
    }

    fn create_executor(
        query: impl Into<sqlil::Query>,
        params: HashMap<u32, DataValue>,
    ) -> MemoryQueryExecutor {
        let (entities, data) = mock_data();

        MemoryQueryExecutor::new(Arc::new(data), entities, query.into(), params)
    }

    #[test]
    fn test_memory_connector_executor_select_all() {
        let mut select = sqlil::Select::new(sqlil::source("people", "1.0", "people"));
        select.cols.push((
            "first_name".to_string(),
            sqlil::Expr::attr("people", "first_name"),
        ));
        select.cols.push((
            "last_name".to_string(),
            sqlil::Expr::attr("people", "last_name"),
        ));

        let executor = create_executor(select, HashMap::new());

        let results = executor.run().unwrap();

        assert_eq!(
            results,
            MemoryResultSet::new(
                vec![
                    (
                        "first_name".to_string(),
                        DataType::Utf8String(StringOptions::default())
                    ),
                    (
                        "last_name".to_string(),
                        DataType::Utf8String(StringOptions::default())
                    ),
                ],
                vec![
                    vec![
                        DataValue::Utf8String("Mary".as_bytes().to_vec()),
                        DataValue::Utf8String("Jane".as_bytes().to_vec())
                    ],
                    vec![
                        DataValue::Utf8String("John".as_bytes().to_vec()),
                        DataValue::Utf8String("Smith".as_bytes().to_vec())
                    ],
                    vec![
                        DataValue::Utf8String("Mary".as_bytes().to_vec()),
                        DataValue::Utf8String("Bennet".as_bytes().to_vec())
                    ],
                ]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_select_invalid_entity() {
        let select = sqlil::Select::new(sqlil::source("invalid", "1.0", "i"));

        let executor = create_executor(select, HashMap::new());

        executor.run().unwrap_err();
    }

    #[test]
    fn test_memory_connector_executor_select_invalid_version() {
        let select = sqlil::Select::new(sqlil::source("people", "invalid", "i"));

        let executor = create_executor(select, HashMap::new());

        executor.run().unwrap_err();
    }

    #[test]
    fn test_memory_connector_executor_select_no_cols() {
        let select = sqlil::Select::new(sqlil::source("people", "1.0", "people"));

        let executor = create_executor(select, HashMap::new());

        let results = executor.run().unwrap();

        assert_eq!(
            results,
            MemoryResultSet::new(vec![], vec![vec![], vec![], vec![]]).unwrap()
        );
    }

    #[test]
    fn test_memory_connector_executor_select_single_column() {
        let mut select = sqlil::Select::new(sqlil::source("people", "1.0", "people"));
        select.cols.push((
            "alias".to_string(),
            sqlil::Expr::attr("people", "first_name"),
        ));

        let executor = create_executor(select, HashMap::new());

        let results = executor.run().unwrap();

        assert_eq!(
            results,
            MemoryResultSet::new(
                vec![(
                    "alias".to_string(),
                    DataType::Utf8String(StringOptions::default())
                ),],
                vec![
                    vec![DataValue::Utf8String("Mary".as_bytes().to_vec()),],
                    vec![DataValue::Utf8String("John".as_bytes().to_vec()),],
                    vec![DataValue::Utf8String("Mary".as_bytes().to_vec()),],
                ]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_select_where_equals() {
        let mut select = sqlil::Select::new(sqlil::source("people", "1.0", "people"));
        select.cols.push((
            "alias".to_string(),
            sqlil::Expr::attr("people", "first_name"),
        ));

        select
            .r#where
            .push(sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::attr("people", "first_name"),
                sqlil::BinaryOpType::Equal,
                sqlil::Expr::Constant(sqlil::Constant::new(DataValue::from("John"))),
            )));

        let executor = create_executor(select, HashMap::new());

        let results = executor.run().unwrap();

        assert_eq!(
            results,
            MemoryResultSet::new(
                vec![(
                    "alias".to_string(),
                    DataType::Utf8String(StringOptions::default())
                ),],
                vec![vec![DataValue::Utf8String("John".as_bytes().to_vec()),],]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_select_skip_row() {
        let mut select = sqlil::Select::new(sqlil::source("people", "1.0", "people"));
        select.cols.push((
            "alias".to_string(),
            sqlil::Expr::attr("people", "first_name"),
        ));

        select.row_skip = 1;

        let executor = create_executor(select, HashMap::new());
        let results = executor.run().unwrap();

        assert_eq!(
            results,
            MemoryResultSet::new(
                vec![(
                    "alias".to_string(),
                    DataType::Utf8String(StringOptions::default())
                ),],
                vec![
                    vec![DataValue::Utf8String("John".as_bytes().to_vec()),],
                    vec![DataValue::Utf8String("Mary".as_bytes().to_vec()),],
                ]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_select_row_limit() {
        let mut select = sqlil::Select::new(sqlil::source("people", "1.0", "people"));
        select.cols.push((
            "alias".to_string(),
            sqlil::Expr::attr("people", "first_name"),
        ));

        select.row_limit = Some(1);

        let executor = create_executor(select, HashMap::new());
        let results = executor.run().unwrap();

        assert_eq!(
            results,
            MemoryResultSet::new(
                vec![(
                    "alias".to_string(),
                    DataType::Utf8String(StringOptions::default())
                ),],
                vec![vec![DataValue::Utf8String("Mary".as_bytes().to_vec()),],]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_select_group_by_column_key() {
        let mut select = sqlil::Select::new(sqlil::source("people", "1.0", "people"));
        select.cols.push((
            "alias".to_string(),
            sqlil::Expr::attr("people", "first_name"),
        ));

        select
            .group_bys
            .push(sqlil::Expr::Attribute(sqlil::attr("people", "first_name")));

        let executor = create_executor(select, HashMap::new());
        let results = executor.run().unwrap();

        assert_eq!(
            results,
            MemoryResultSet::new(
                vec![(
                    "alias".to_string(),
                    DataType::Utf8String(StringOptions::default())
                ),],
                vec![
                    vec![DataValue::Utf8String("Mary".as_bytes().to_vec()),],
                    vec![DataValue::Utf8String("John".as_bytes().to_vec()),],
                ]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_select_group_by_column_key_with_count() {
        let mut select = sqlil::Select::new(sqlil::source("people", "1.0", "people"));
        select.cols.push((
            "alias".to_string(),
            sqlil::Expr::attr("people", "first_name"),
        ));
        select.cols.push((
            "count".to_string(),
            sqlil::Expr::AggregateCall(AggregateCall::Count),
        ));

        select
            .group_bys
            .push(sqlil::Expr::Attribute(sqlil::attr("people", "first_name")));

        let executor = create_executor(select, HashMap::new());
        let results = executor.run().unwrap();

        assert_eq!(
            results,
            MemoryResultSet::new(
                vec![
                    (
                        "alias".to_string(),
                        DataType::Utf8String(StringOptions::default())
                    ),
                    ("count".to_string(), DataType::UInt64,)
                ],
                vec![
                    vec![
                        DataValue::Utf8String("Mary".as_bytes().to_vec()),
                        DataValue::UInt64(2)
                    ],
                    vec![
                        DataValue::Utf8String("John".as_bytes().to_vec()),
                        DataValue::UInt64(1)
                    ],
                ]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_select_count_implicit_group_by() {
        let mut select = sqlil::Select::new(sqlil::source("people", "1.0", "people"));
        select.cols.push((
            "count".to_string(),
            sqlil::Expr::AggregateCall(AggregateCall::Count),
        ));

        let executor = create_executor(select, HashMap::new());
        let results = executor.run().unwrap();

        assert_eq!(
            results,
            MemoryResultSet::new(
                vec![("count".to_string(), DataType::UInt64,)],
                vec![vec![DataValue::UInt64(3)],]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_select_bin_op_concat() {
        let mut select = sqlil::Select::new(sqlil::source("people", "1.0", "people"));
        select.cols.push((
            "full_name".to_string(),
            sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::attr("people", "first_name"),
                sqlil::BinaryOpType::Concat,
                sqlil::Expr::attr("people", "last_name"),
            )),
        ));

        let executor = create_executor(select, HashMap::new());
        let results = executor.run().unwrap();

        assert_eq!(
            results,
            MemoryResultSet::new(
                vec![(
                    "full_name".to_string(),
                    DataType::Utf8String(StringOptions::default()),
                )],
                vec![
                    vec![DataValue::Utf8String("MaryJane".as_bytes().to_vec())],
                    vec![DataValue::Utf8String("JohnSmith".as_bytes().to_vec())],
                    vec![DataValue::Utf8String("MaryBennet".as_bytes().to_vec())]
                ]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_select_group_by_expr_key_with_count() {
        let mut select = sqlil::Select::new(sqlil::source("people", "1.0", "people"));
        let full_name = sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
            sqlil::Expr::attr("people", "first_name"),
            sqlil::BinaryOpType::Concat,
            sqlil::Expr::attr("people", "last_name"),
        ));
        select
            .cols
            .push(("full_name".to_string(), full_name.clone()));
        select.cols.push((
            "count".to_string(),
            sqlil::Expr::AggregateCall(AggregateCall::Count),
        ));

        select.group_bys.push(full_name);

        let executor = create_executor(select, HashMap::new());
        let results = executor.run().unwrap();

        assert_eq!(
            results,
            MemoryResultSet::new(
                vec![
                    (
                        "full_name".to_string(),
                        DataType::Utf8String(StringOptions::default())
                    ),
                    ("count".to_string(), DataType::UInt64,)
                ],
                vec![
                    vec![
                        DataValue::Utf8String("MaryJane".as_bytes().to_vec()),
                        DataValue::UInt64(1)
                    ],
                    vec![
                        DataValue::Utf8String("JohnSmith".as_bytes().to_vec()),
                        DataValue::UInt64(1)
                    ],
                    vec![
                        DataValue::Utf8String("MaryBennet".as_bytes().to_vec()),
                        DataValue::UInt64(1)
                    ],
                ]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_select_order_by_single() {
        let mut select = sqlil::Select::new(sqlil::source("people", "1.0", "people"));
        select.cols.push((
            "first_name".to_string(),
            sqlil::Expr::attr("people", "first_name"),
        ));
        select.cols.push((
            "last_name".to_string(),
            sqlil::Expr::attr("people", "last_name"),
        ));

        select
            .order_bys
            .push(Ordering::asc(sqlil::Expr::attr("people", "first_name")));

        let executor = create_executor(select, HashMap::new());
        let results = executor.run().unwrap();

        assert_eq!(
            results,
            MemoryResultSet::new(
                vec![
                    (
                        "first_name".to_string(),
                        DataType::Utf8String(StringOptions::default())
                    ),
                    (
                        "last_name".to_string(),
                        DataType::Utf8String(StringOptions::default())
                    )
                ],
                vec![
                    vec![
                        DataValue::Utf8String("John".as_bytes().to_vec()),
                        DataValue::Utf8String("Smith".as_bytes().to_vec())
                    ],
                    vec![
                        DataValue::Utf8String("Mary".as_bytes().to_vec()),
                        DataValue::Utf8String("Jane".as_bytes().to_vec())
                    ],
                    vec![
                        DataValue::Utf8String("Mary".as_bytes().to_vec()),
                        DataValue::Utf8String("Bennet".as_bytes().to_vec())
                    ],
                ]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_select_order_by_single_desc() {
        let mut select = sqlil::Select::new(sqlil::source("people", "1.0", "people"));
        select.cols.push((
            "first_name".to_string(),
            sqlil::Expr::attr("people", "first_name"),
        ));
        select.cols.push((
            "last_name".to_string(),
            sqlil::Expr::attr("people", "last_name"),
        ));

        select
            .order_bys
            .push(Ordering::desc(sqlil::Expr::attr("people", "first_name")));

        let executor = create_executor(select, HashMap::new());
        let results = executor.run().unwrap();

        assert_eq!(
            results,
            MemoryResultSet::new(
                vec![
                    (
                        "first_name".to_string(),
                        DataType::Utf8String(StringOptions::default())
                    ),
                    (
                        "last_name".to_string(),
                        DataType::Utf8String(StringOptions::default())
                    )
                ],
                vec![
                    vec![
                        DataValue::Utf8String("Mary".as_bytes().to_vec()),
                        DataValue::Utf8String("Jane".as_bytes().to_vec())
                    ],
                    vec![
                        DataValue::Utf8String("Mary".as_bytes().to_vec()),
                        DataValue::Utf8String("Bennet".as_bytes().to_vec())
                    ],
                    vec![
                        DataValue::Utf8String("John".as_bytes().to_vec()),
                        DataValue::Utf8String("Smith".as_bytes().to_vec())
                    ],
                ]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_select_order_by_multiple() {
        let mut select = sqlil::Select::new(sqlil::source("people", "1.0", "people"));
        select.cols.push((
            "first_name".to_string(),
            sqlil::Expr::attr("people", "first_name"),
        ));
        select.cols.push((
            "last_name".to_string(),
            sqlil::Expr::attr("people", "last_name"),
        ));

        select
            .order_bys
            .push(Ordering::asc(sqlil::Expr::attr("people", "first_name")));
        select
            .order_bys
            .push(Ordering::desc(sqlil::Expr::attr("people", "last_name")));

        let executor = create_executor(select, HashMap::new());
        let results = executor.run().unwrap();

        assert_eq!(
            results,
            MemoryResultSet::new(
                vec![
                    (
                        "first_name".to_string(),
                        DataType::Utf8String(StringOptions::default())
                    ),
                    (
                        "last_name".to_string(),
                        DataType::Utf8String(StringOptions::default())
                    )
                ],
                vec![
                    vec![
                        DataValue::Utf8String("John".as_bytes().to_vec()),
                        DataValue::Utf8String("Smith".as_bytes().to_vec())
                    ],
                    vec![
                        DataValue::Utf8String("Mary".as_bytes().to_vec()),
                        DataValue::Utf8String("Jane".as_bytes().to_vec())
                    ],
                    vec![
                        DataValue::Utf8String("Mary".as_bytes().to_vec()),
                        DataValue::Utf8String("Bennet".as_bytes().to_vec())
                    ],
                ]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_select_inner_join() {
        let mut select = sqlil::Select::new(sqlil::source("people", "1.0", "people"));

        select.joins.push(sqlil::Join::new(
            sqlil::JoinType::Inner,
            sqlil::source("pets", "1.0", "pets"),
            vec![sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::attr("people", "id"),
                sqlil::BinaryOpType::Equal,
                sqlil::Expr::attr("pets", "owner_id"),
            ))],
        ));

        select.cols.push((
            "owner_first_name".to_string(),
            sqlil::Expr::attr("people", "first_name"),
        ));
        select.cols.push((
            "owner_last_name".to_string(),
            sqlil::Expr::attr("people", "last_name"),
        ));
        select.cols.push((
            "pet_name".to_string(),
            sqlil::Expr::attr("pets", "pet_name"),
        ));

        let executor = create_executor(select, HashMap::new());
        let results = executor.run().unwrap();

        assert_eq!(
            results,
            MemoryResultSet::new(
                vec![
                    (
                        "owner_first_name".to_string(),
                        DataType::Utf8String(StringOptions::default())
                    ),
                    (
                        "owner_last_name".to_string(),
                        DataType::Utf8String(StringOptions::default())
                    ),
                    (
                        "pet_name".to_string(),
                        DataType::Utf8String(StringOptions::default())
                    )
                ],
                vec![
                    vec![
                        DataValue::Utf8String("Mary".as_bytes().to_vec()),
                        DataValue::Utf8String("Jane".as_bytes().to_vec()),
                        DataValue::Utf8String("Pepper".as_bytes().to_vec())
                    ],
                    vec![
                        DataValue::Utf8String("Mary".as_bytes().to_vec()),
                        DataValue::Utf8String("Jane".as_bytes().to_vec()),
                        DataValue::Utf8String("Salt".as_bytes().to_vec())
                    ],
                    vec![
                        DataValue::Utf8String("Mary".as_bytes().to_vec()),
                        DataValue::Utf8String("Bennet".as_bytes().to_vec()),
                        DataValue::Utf8String("Relish".as_bytes().to_vec())
                    ],
                ]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_select_left_join() {
        let mut select = sqlil::Select::new(sqlil::source("people", "1.0", "people"));

        select.joins.push(sqlil::Join::new(
            sqlil::JoinType::Left,
            sqlil::source("pets", "1.0", "pets"),
            vec![sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::attr("people", "id"),
                sqlil::BinaryOpType::Equal,
                sqlil::Expr::attr("pets", "owner_id"),
            ))],
        ));

        select.cols.push((
            "owner_first_name".to_string(),
            sqlil::Expr::attr("people", "first_name"),
        ));
        select.cols.push((
            "owner_last_name".to_string(),
            sqlil::Expr::attr("people", "last_name"),
        ));
        select.cols.push((
            "pet_name".to_string(),
            sqlil::Expr::attr("pets", "pet_name"),
        ));

        let executor = create_executor(select, HashMap::new());
        let results = executor.run().unwrap();

        assert_eq!(
            results,
            MemoryResultSet::new(
                vec![
                    (
                        "owner_first_name".to_string(),
                        DataType::Utf8String(StringOptions::default())
                    ),
                    (
                        "owner_last_name".to_string(),
                        DataType::Utf8String(StringOptions::default())
                    ),
                    (
                        "pet_name".to_string(),
                        DataType::Utf8String(StringOptions::default())
                    )
                ],
                vec![
                    vec![
                        DataValue::Utf8String("Mary".as_bytes().to_vec()),
                        DataValue::Utf8String("Jane".as_bytes().to_vec()),
                        DataValue::Utf8String("Pepper".as_bytes().to_vec())
                    ],
                    vec![
                        DataValue::Utf8String("Mary".as_bytes().to_vec()),
                        DataValue::Utf8String("Jane".as_bytes().to_vec()),
                        DataValue::Utf8String("Salt".as_bytes().to_vec())
                    ],
                    vec![
                        DataValue::Utf8String("Mary".as_bytes().to_vec()),
                        DataValue::Utf8String("Bennet".as_bytes().to_vec()),
                        DataValue::Utf8String("Relish".as_bytes().to_vec())
                    ],
                    vec![
                        DataValue::Utf8String("John".as_bytes().to_vec()),
                        DataValue::Utf8String("Smith".as_bytes().to_vec()),
                        DataValue::Null,
                    ],
                ]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_select_right_join() {
        let mut select = sqlil::Select::new(sqlil::source("people", "1.0", "people"));

        select.joins.push(sqlil::Join::new(
            sqlil::JoinType::Right,
            sqlil::source("pets", "1.0", "pets"),
            vec![sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::attr("people", "id"),
                sqlil::BinaryOpType::Equal,
                sqlil::Expr::attr("pets", "owner_id"),
            ))],
        ));

        select.cols.push((
            "owner_first_name".to_string(),
            sqlil::Expr::attr("people", "first_name"),
        ));
        select.cols.push((
            "owner_last_name".to_string(),
            sqlil::Expr::attr("people", "last_name"),
        ));
        select.cols.push((
            "pet_name".to_string(),
            sqlil::Expr::attr("pets", "pet_name"),
        ));

        let executor = create_executor(select, HashMap::new());
        let results = executor.run().unwrap();

        assert_eq!(
            results,
            MemoryResultSet::new(
                vec![
                    (
                        "owner_first_name".to_string(),
                        DataType::Utf8String(StringOptions::default())
                    ),
                    (
                        "owner_last_name".to_string(),
                        DataType::Utf8String(StringOptions::default())
                    ),
                    (
                        "pet_name".to_string(),
                        DataType::Utf8String(StringOptions::default())
                    )
                ],
                vec![
                    vec![
                        DataValue::Utf8String("Mary".as_bytes().to_vec()),
                        DataValue::Utf8String("Jane".as_bytes().to_vec()),
                        DataValue::Utf8String("Pepper".as_bytes().to_vec())
                    ],
                    vec![
                        DataValue::Utf8String("Mary".as_bytes().to_vec()),
                        DataValue::Utf8String("Jane".as_bytes().to_vec()),
                        DataValue::Utf8String("Salt".as_bytes().to_vec())
                    ],
                    vec![
                        DataValue::Utf8String("Mary".as_bytes().to_vec()),
                        DataValue::Utf8String("Bennet".as_bytes().to_vec()),
                        DataValue::Utf8String("Relish".as_bytes().to_vec())
                    ],
                    vec![
                        DataValue::Null,
                        DataValue::Null,
                        DataValue::Utf8String("Luna".as_bytes().to_vec()),
                    ],
                ]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_select_full_join() {
        let mut select = sqlil::Select::new(sqlil::source("people", "1.0", "people"));

        select.joins.push(sqlil::Join::new(
            sqlil::JoinType::Full,
            sqlil::source("pets", "1.0", "pets"),
            vec![sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::attr("people", "id"),
                sqlil::BinaryOpType::Equal,
                sqlil::Expr::attr("pets", "owner_id"),
            ))],
        ));

        select.cols.push((
            "owner_first_name".to_string(),
            sqlil::Expr::attr("people", "first_name"),
        ));
        select.cols.push((
            "owner_last_name".to_string(),
            sqlil::Expr::attr("people", "last_name"),
        ));
        select.cols.push((
            "pet_name".to_string(),
            sqlil::Expr::attr("pets", "pet_name"),
        ));

        let executor = create_executor(select, HashMap::new());
        let results = executor.run().unwrap();

        assert_eq!(
            results,
            MemoryResultSet::new(
                vec![
                    (
                        "owner_first_name".to_string(),
                        DataType::Utf8String(StringOptions::default())
                    ),
                    (
                        "owner_last_name".to_string(),
                        DataType::Utf8String(StringOptions::default())
                    ),
                    (
                        "pet_name".to_string(),
                        DataType::Utf8String(StringOptions::default())
                    )
                ],
                vec![
                    vec![
                        DataValue::Utf8String("Mary".as_bytes().to_vec()),
                        DataValue::Utf8String("Jane".as_bytes().to_vec()),
                        DataValue::Utf8String("Pepper".as_bytes().to_vec())
                    ],
                    vec![
                        DataValue::Utf8String("Mary".as_bytes().to_vec()),
                        DataValue::Utf8String("Jane".as_bytes().to_vec()),
                        DataValue::Utf8String("Salt".as_bytes().to_vec())
                    ],
                    vec![
                        DataValue::Utf8String("Mary".as_bytes().to_vec()),
                        DataValue::Utf8String("Bennet".as_bytes().to_vec()),
                        DataValue::Utf8String("Relish".as_bytes().to_vec())
                    ],
                    vec![
                        DataValue::Utf8String("John".as_bytes().to_vec()),
                        DataValue::Utf8String("Smith".as_bytes().to_vec()),
                        DataValue::Null,
                    ],
                    vec![
                        DataValue::Null,
                        DataValue::Null,
                        DataValue::Utf8String("Luna".as_bytes().to_vec()),
                    ],
                ]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_select_where_parameter() {
        let mut select = sqlil::Select::new(sqlil::source("people", "1.0", "people"));
        select.cols.push((
            "first_name".to_string(),
            sqlil::Expr::attr("people", "first_name"),
        ));
        select.cols.push((
            "last_name".to_string(),
            sqlil::Expr::attr("people", "last_name"),
        ));

        select
            .r#where
            .push(sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::attr("people", "first_name"),
                sqlil::BinaryOpType::Equal,
                sqlil::Expr::Parameter(sqlil::Parameter::new(
                    DataType::Utf8String(StringOptions::default()),
                    1,
                )),
            )));

        let executor = create_executor(
            select,
            [(1, DataValue::Utf8String("John".as_bytes().to_vec()))]
                .into_iter()
                .collect(),
        );

        let results = executor.run().unwrap();

        assert_eq!(
            results,
            MemoryResultSet::new(
                vec![
                    (
                        "first_name".to_string(),
                        DataType::Utf8String(StringOptions::default())
                    ),
                    (
                        "last_name".to_string(),
                        DataType::Utf8String(StringOptions::default())
                    ),
                ],
                vec![vec![
                    DataValue::Utf8String("John".as_bytes().to_vec()),
                    DataValue::Utf8String("Smith".as_bytes().to_vec())
                ],]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_insert_empty_row() {
        let insert = sqlil::Insert::new(sqlil::source("people", "1.0", "people"));

        let executor = create_executor(insert, HashMap::new());

        let results = executor.run().unwrap();

        assert_eq!(results, MemoryResultSet::empty());

        executor
            .data
            .with_data("people", "1.0", |data| {
                assert_eq!(
                    data.iter().last().unwrap(),
                    &vec![DataValue::Null, DataValue::Null, DataValue::Null]
                );
            })
            .unwrap();
    }

    #[test]
    fn test_memory_connector_executor_insert_row_with_values_row() {
        let mut insert = sqlil::Insert::new(sqlil::source("people", "1.0", "people"));

        insert
            .cols
            .push(("id".into(), sqlil::Expr::constant(DataValue::UInt32(123))));
        insert.cols.push((
            "first_name".into(),
            sqlil::Expr::constant(DataValue::from("New")),
        ));
        insert.cols.push((
            "last_name".into(),
            sqlil::Expr::constant(DataValue::from("Man")),
        ));

        let executor = create_executor(insert, HashMap::new());

        let results = executor.run().unwrap();

        assert_eq!(results, MemoryResultSet::empty());

        executor
            .data
            .with_data("people", "1.0", |data| {
                assert_eq!(
                    data.iter().last().unwrap(),
                    &vec![
                        DataValue::UInt32(123),
                        DataValue::from("New"),
                        DataValue::from("Man")
                    ]
                );
            })
            .unwrap();
    }

    #[test]
    fn test_memory_connector_executor_update_no_set() {
        let update = sqlil::Update::new(sqlil::source("people", "1.0", "people"));

        let executor = create_executor(update, HashMap::new());
        let orig_data = executor
            .data
            .with_data("people", "1.0", |data| data.clone())
            .unwrap();

        let results = executor.run().unwrap();

        assert_eq!(results, MemoryResultSet::empty());

        executor
            .data
            .with_data("people", "1.0", |data| {
                assert_eq!(data, &orig_data);
            })
            .unwrap();
    }

    #[test]
    fn test_memory_connector_executor_update_all_rows() {
        let mut update = sqlil::Update::new(sqlil::source("people", "1.0", "people"));

        update.cols.push((
            "first_name".into(),
            sqlil::Expr::constant(DataValue::from("New")),
        ));

        let executor = create_executor(update, HashMap::new());

        let results = executor.run().unwrap();

        assert_eq!(results, MemoryResultSet::empty());

        executor
            .data
            .with_data("people", "1.0", |data| {
                assert!(data.into_iter().all(|r| r[1] == DataValue::from("New")))
            })
            .unwrap();
    }

    #[test]
    fn test_memory_connector_executor_update_where() {
        let mut update = sqlil::Update::new(sqlil::source("people", "1.0", "people"));

        update.cols.push((
            "first_name".into(),
            sqlil::Expr::constant(DataValue::from("New")),
        ));

        update
            .r#where
            .push(sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::attr("people", "id"),
                sqlil::BinaryOpType::Equal,
                sqlil::Expr::constant(DataValue::UInt32(1)),
            )));

        let executor = create_executor(update, HashMap::new());

        let results = executor.run().unwrap();

        assert_eq!(results, MemoryResultSet::empty());

        executor
            .data
            .with_data("people", "1.0", |data| {
                assert_eq!(
                    data.into_iter()
                        .map(|row| row[1].clone())
                        .collect::<Vec<_>>(),
                    vec![
                        DataValue::from("New"),
                        DataValue::from("John"),
                        DataValue::from("Mary"),
                    ]
                );
            })
            .unwrap();
    }

    #[test]
    fn test_memory_connector_executor_delete_all() {
        let delete = sqlil::Delete::new(sqlil::source("people", "1.0", "people"));

        let executor = create_executor(delete, HashMap::new());

        let results = executor.run().unwrap();

        assert_eq!(results, MemoryResultSet::empty());

        executor
            .data
            .with_data("people", "1.0", |data| {
                assert_eq!(data, &Vec::<Vec<_>>::new());
            })
            .unwrap();
    }

    #[test]
    fn test_memory_connector_executor_delete_where() {
        let mut delete = sqlil::Delete::new(sqlil::source("people", "1.0", "people"));

        delete
            .r#where
            .push(sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::attr("people", "id"),
                sqlil::BinaryOpType::Equal,
                sqlil::Expr::constant(DataValue::UInt32(1)),
            )));

        let executor = create_executor(delete, HashMap::new());

        let results = executor.run().unwrap();

        assert_eq!(results, MemoryResultSet::empty());

        executor
            .data
            .with_data("people", "1.0", |data| {
                assert_eq!(
                    data.into_iter().map(|r| r[0].clone()).collect::<Vec<_>>(),
                    vec![DataValue::UInt32(2), DataValue::UInt32(3)]
                )
            })
            .unwrap();
    }

    #[test]
    fn test_memory_connector_executor_select_row_id() {
        let mut select = sqlil::Select::new(sqlil::source("people", "1.0", "people"));
        select
            .cols
            .push(("row_id".to_string(), sqlil::Expr::attr("people", "ROWIDX")));
        select.cols.push((
            "first_name".to_string(),
            sqlil::Expr::attr("people", "first_name"),
        ));
        select.cols.push((
            "last_name".to_string(),
            sqlil::Expr::attr("people", "last_name"),
        ));

        let executor = create_executor(select, HashMap::new());
        let results = executor.run().unwrap();

        assert_eq!(
            results,
            MemoryResultSet::new(
                vec![
                    ("row_id".to_string(), DataType::UInt64),
                    (
                        "first_name".to_string(),
                        DataType::Utf8String(StringOptions::default())
                    ),
                    (
                        "last_name".to_string(),
                        DataType::Utf8String(StringOptions::default())
                    )
                ],
                vec![
                    vec![
                        DataValue::UInt64(0),
                        DataValue::from("Mary"),
                        DataValue::from("Jane"),
                    ],
                    vec![
                        DataValue::UInt64(1),
                        DataValue::from("John"),
                        DataValue::from("Smith"),
                    ],
                    vec![
                        DataValue::UInt64(2),
                        DataValue::from("Mary"),
                        DataValue::from("Bennet"),
                    ],
                ]
            )
            .unwrap()
        )
    }
}

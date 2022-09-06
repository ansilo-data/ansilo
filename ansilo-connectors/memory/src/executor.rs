use std::{
    cmp,
    collections::{HashMap, HashSet},
    iter,
    sync::Arc,
};

use ansilo_core::{
    config::EntityAttributeConfig,
    data::{
        rust_decimal::{prelude::FromPrimitive, Decimal, MathematicalOps},
        uuid::Uuid,
        DataType, DataValue, DecimalOptions, StringOptions,
    },
    err::{bail, Context, Error, Result},
    sqlil::{self},
};
use itertools::Itertools;

use ansilo_connectors_base::common::entity::{ConnectorEntityConfig, EntitySource};

use super::{MemoryConnectorEntitySourceConfig, MemoryDatabase, MemoryResultSet};

#[derive(Debug, Clone)]
pub(crate) struct MemoryQueryExecutor {
    data: Arc<MemoryDatabase>,
    entities: ConnectorEntityConfig<MemoryConnectorEntitySourceConfig>,
    query: sqlil::Query,
    params: HashMap<u32, DataValue>,
}

/// This entire implementation is garbage but it doesn't matter as this is used
/// as a testing instrument.
impl MemoryQueryExecutor {
    pub(crate) fn new(
        data: Arc<MemoryDatabase>,
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
            sqlil::Query::BulkInsert(bulk_insert) => self.run_bulk_insert(bulk_insert),
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

            self.data
                .append_row_ids(&insert.target.entity.entity_id, &mut [&mut row]);
            rows.push(row);

            Ok(())
        })?;

        Ok(MemoryResultSet::empty())
    }

    fn run_bulk_insert(&self, bulk_insert: &sqlil::BulkInsert) -> Result<MemoryResultSet> {
        self.update_entity_data(&bulk_insert.target, |rows| {
            let attrs = self.get_attrs(&bulk_insert.target.entity)?;
            let ctx = DataContext::Cell(DataValue::Null);

            for values in bulk_insert.rows().into_iter() {
                let values = values.collect_vec();

                let mut row = attrs
                    .iter()
                    .map(|a| {
                        (
                            &a.r#type,
                            bulk_insert.cols.iter().position(|attr| attr == &a.id),
                        )
                    })
                    .map(|(t, a)| (t, a.map(|idx| self.evaluate(&ctx, values[idx]))))
                    .map(|(t, a)| (t, a.unwrap_or(Ok(DataContext::Cell(DataValue::Null)))))
                    .map(|(t, a)| (t, a.and_then(|a| a.as_cell())))
                    .map(|(t, a)| a.and_then(|a| a.try_coerce_into(t)))
                    .collect::<Result<Vec<_>>>()?;

                self.data
                    .append_row_ids(&bulk_insert.target.entity.entity_id, &mut [&mut row]);
                rows.push(row);
            }

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
            let mut retained = vec![];

            for row in rows.iter() {
                if !self.satisfies_where(row)? {
                    retained.push(row.clone());
                }
            }

            *rows = retained;

            Ok(())
        })?;

        Ok(MemoryResultSet::empty())
    }

    fn get_entity_data(&self, s: &sqlil::EntitySource) -> Result<Vec<Vec<DataValue>>> {
        self.data
            .with_data(&s.entity.entity_id, |rows| rows.clone())
            .ok_or(Error::msg("Could not find entity"))
    }

    fn update_entity_data(
        &self,
        s: &sqlil::EntitySource,
        cb: impl FnOnce(&mut Vec<Vec<DataValue>>) -> Result<()>,
    ) -> Result<()> {
        self.data
            .with_data_mut(&s.entity.entity_id, move |rows| cb(rows))
            .ok_or(Error::msg("Could not find entity"))?
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
            sqlil::Expr::UnaryOp(op) => {
                let arg = self.evaluate(data, &op.expr)?.as_cell()?;

                DataContext::Cell(match op.r#type {
                    sqlil::UnaryOpType::LogicalNot => {
                        match arg.try_coerce_into(&DataType::Boolean)? {
                            DataValue::Boolean(v) => DataValue::Boolean(!v),
                            _ => unreachable!(),
                        }
                    }
                    sqlil::UnaryOpType::Negate => match arg {
                        DataValue::Int8(v) => DataValue::Int8(-v),
                        DataValue::Int16(v) => DataValue::Int16(-v),
                        DataValue::Int32(v) => DataValue::Int32(-v),
                        DataValue::Int64(v) => DataValue::Int64(-v),
                        DataValue::Float32(v) => DataValue::Float32(-v),
                        DataValue::Float64(v) => DataValue::Float64(-v),
                        DataValue::Decimal(v) => DataValue::Decimal(-v),
                        _ => bail!("Cannot negate type: {:?}", arg.r#type()),
                    },
                    sqlil::UnaryOpType::BitwiseNot => match arg {
                        DataValue::Int8(v) => DataValue::Int8(!v),
                        DataValue::UInt8(v) => DataValue::UInt8(!v),
                        DataValue::Int16(v) => DataValue::Int16(!v),
                        DataValue::UInt16(v) => DataValue::UInt16(!v),
                        DataValue::Int32(v) => DataValue::Int32(!v),
                        DataValue::UInt32(v) => DataValue::UInt32(!v),
                        DataValue::Int64(v) => DataValue::Int64(!v),
                        DataValue::UInt64(v) => DataValue::UInt64(!v),
                        _ => bail!("Cannot bit-invert type: {:?}", arg.r#type()),
                    },
                    sqlil::UnaryOpType::IsNull => DataValue::Boolean(arg.is_null()),
                    sqlil::UnaryOpType::IsNotNull => DataValue::Boolean(!arg.is_null()),
                })
            }
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

                DataContext::Cell(match &op.r#type {
                    sqlil::BinaryOpType::Add => match (left, right) {
                        (DataValue::Int8(l), DataValue::Int8(r)) => {
                            DataValue::Int16(l as i16 + r as i16)
                        }
                        (DataValue::UInt8(l), DataValue::UInt8(r)) => {
                            DataValue::UInt16(l as u16 + r as u16)
                        }
                        (DataValue::Int16(l), DataValue::Int16(r)) => {
                            DataValue::Int32(l as i32 + r as i32)
                        }
                        (DataValue::UInt16(l), DataValue::UInt16(r)) => {
                            DataValue::UInt32(l as u32 + r as u32)
                        }
                        (DataValue::Int32(l), DataValue::Int32(r)) => {
                            DataValue::Int64(l as i64 + r as i64)
                        }
                        (DataValue::UInt32(l), DataValue::UInt32(r)) => {
                            DataValue::UInt64(l as u64 + r as u64)
                        }
                        (DataValue::Int64(l), DataValue::Int64(r)) => DataValue::Decimal(
                            Decimal::from_i64(l).unwrap() + Decimal::from_i64(r).unwrap(),
                        ),
                        (DataValue::UInt64(l), DataValue::UInt64(r)) => DataValue::Decimal(
                            Decimal::from_u64(l).unwrap() + Decimal::from_u64(r).unwrap(),
                        ),
                        (DataValue::Float32(l), DataValue::Float32(r)) => {
                            DataValue::Float64(l as f64 + r as f64)
                        }
                        (DataValue::Float64(l), DataValue::Float64(r)) => DataValue::Float64(l + r),
                        (DataValue::Decimal(l), DataValue::Decimal(r)) => DataValue::Decimal(l + r),
                        (l, r) => bail!("Cannot add pair ({:?}, {:?})", l, r),
                    },
                    sqlil::BinaryOpType::Subtract => match (left, right) {
                        (DataValue::Int8(l), DataValue::Int8(r)) => {
                            DataValue::Int16(l as i16 - r as i16)
                        }
                        (DataValue::UInt8(l), DataValue::UInt8(r)) => {
                            DataValue::Int16(l as i16 - r as i16)
                        }
                        (DataValue::Int16(l), DataValue::Int16(r)) => {
                            DataValue::Int32(l as i32 - r as i32)
                        }
                        (DataValue::UInt16(l), DataValue::UInt16(r)) => {
                            DataValue::Int32(l as i32 - r as i32)
                        }
                        (DataValue::Int32(l), DataValue::Int32(r)) => {
                            DataValue::Int64(l as i64 - r as i64)
                        }
                        (DataValue::UInt32(l), DataValue::UInt32(r)) => {
                            DataValue::Int64(l as i64 - r as i64)
                        }
                        (DataValue::Int64(l), DataValue::Int64(r)) => DataValue::Decimal(
                            Decimal::from_i64(l).unwrap() - Decimal::from_i64(r).unwrap(),
                        ),
                        (DataValue::UInt64(l), DataValue::UInt64(r)) => DataValue::Decimal(
                            Decimal::from_u64(l).unwrap() - Decimal::from_u64(r).unwrap(),
                        ),
                        (DataValue::Float32(l), DataValue::Float32(r)) => {
                            DataValue::Float64(l as f64 - r as f64)
                        }
                        (DataValue::Float64(l), DataValue::Float64(r)) => DataValue::Float64(l - r),
                        (DataValue::Decimal(l), DataValue::Decimal(r)) => DataValue::Decimal(l - r),
                        (l, r) => bail!("Cannot subtract pair ({:?}, {:?})", l, r),
                    },
                    sqlil::BinaryOpType::Multiply => match (left, right) {
                        (DataValue::Int8(l), DataValue::Int8(r)) => DataValue::Decimal(
                            Decimal::from_i8(l).unwrap() * Decimal::from_i8(r).unwrap(),
                        ),
                        (DataValue::UInt8(l), DataValue::UInt8(r)) => DataValue::Decimal(
                            Decimal::from_u8(l).unwrap() * Decimal::from_u8(r).unwrap(),
                        ),
                        (DataValue::Int16(l), DataValue::Int16(r)) => DataValue::Decimal(
                            Decimal::from_i16(l).unwrap() * Decimal::from_i16(r).unwrap(),
                        ),
                        (DataValue::UInt16(l), DataValue::UInt16(r)) => DataValue::Decimal(
                            Decimal::from_u16(l).unwrap() * Decimal::from_u16(r).unwrap(),
                        ),
                        (DataValue::Int32(l), DataValue::Int32(r)) => DataValue::Decimal(
                            Decimal::from_i32(l).unwrap() * Decimal::from_i32(r).unwrap(),
                        ),
                        (DataValue::UInt32(l), DataValue::UInt32(r)) => DataValue::Decimal(
                            Decimal::from_u32(l).unwrap() * Decimal::from_u32(r).unwrap(),
                        ),
                        (DataValue::Int64(l), DataValue::Int64(r)) => DataValue::Decimal(
                            Decimal::from_i64(l).unwrap() * Decimal::from_i64(r).unwrap(),
                        ),
                        (DataValue::UInt64(l), DataValue::UInt64(r)) => DataValue::Decimal(
                            Decimal::from_u64(l).unwrap() * Decimal::from_u64(r).unwrap(),
                        ),
                        (DataValue::Float32(l), DataValue::Float32(r)) => {
                            DataValue::Float64(l as f64 * r as f64)
                        }
                        (DataValue::Float64(l), DataValue::Float64(r)) => DataValue::Float64(l * r),
                        (DataValue::Decimal(l), DataValue::Decimal(r)) => DataValue::Decimal(l * r),
                        (l, r) => bail!("Cannot multiply pair ({:?}, {:?})", l, r),
                    },
                    sqlil::BinaryOpType::Divide => match (left, right) {
                        (DataValue::Int8(l), DataValue::Int8(r)) => DataValue::Decimal(
                            Decimal::from_i8(l).unwrap() / Decimal::from_i8(r).unwrap(),
                        ),
                        (DataValue::UInt8(l), DataValue::UInt8(r)) => DataValue::Decimal(
                            Decimal::from_u8(l).unwrap() / Decimal::from_u8(r).unwrap(),
                        ),
                        (DataValue::Int16(l), DataValue::Int16(r)) => DataValue::Decimal(
                            Decimal::from_i16(l).unwrap() / Decimal::from_i16(r).unwrap(),
                        ),
                        (DataValue::UInt16(l), DataValue::UInt16(r)) => DataValue::Decimal(
                            Decimal::from_u16(l).unwrap() / Decimal::from_u16(r).unwrap(),
                        ),
                        (DataValue::Int32(l), DataValue::Int32(r)) => DataValue::Decimal(
                            Decimal::from_i32(l).unwrap() / Decimal::from_i32(r).unwrap(),
                        ),
                        (DataValue::UInt32(l), DataValue::UInt32(r)) => DataValue::Decimal(
                            Decimal::from_u32(l).unwrap() / Decimal::from_u32(r).unwrap(),
                        ),
                        (DataValue::Int64(l), DataValue::Int64(r)) => DataValue::Decimal(
                            Decimal::from_i64(l).unwrap() / Decimal::from_i64(r).unwrap(),
                        ),
                        (DataValue::UInt64(l), DataValue::UInt64(r)) => DataValue::Decimal(
                            Decimal::from_u64(l).unwrap() / Decimal::from_u64(r).unwrap(),
                        ),
                        (DataValue::Float32(l), DataValue::Float32(r)) => {
                            DataValue::Float64(l as f64 / r as f64)
                        }
                        (DataValue::Float64(l), DataValue::Float64(r)) => DataValue::Float64(l / r),
                        (DataValue::Decimal(l), DataValue::Decimal(r)) => DataValue::Decimal(l / r),
                        (l, r) => bail!("Cannot divide pair ({:?}, {:?})", l, r),
                    },
                    sqlil::BinaryOpType::Modulo => match (left, right) {
                        (DataValue::Int8(l), DataValue::Int8(r)) => DataValue::Decimal(
                            Decimal::from_i8(l).unwrap() % Decimal::from_i8(r).unwrap(),
                        ),
                        (DataValue::UInt8(l), DataValue::UInt8(r)) => DataValue::Decimal(
                            Decimal::from_u8(l).unwrap() % Decimal::from_u8(r).unwrap(),
                        ),
                        (DataValue::Int16(l), DataValue::Int16(r)) => DataValue::Decimal(
                            Decimal::from_i16(l).unwrap() % Decimal::from_i16(r).unwrap(),
                        ),
                        (DataValue::UInt16(l), DataValue::UInt16(r)) => DataValue::Decimal(
                            Decimal::from_u16(l).unwrap() % Decimal::from_u16(r).unwrap(),
                        ),
                        (DataValue::Int32(l), DataValue::Int32(r)) => DataValue::Decimal(
                            Decimal::from_i32(l).unwrap() % Decimal::from_i32(r).unwrap(),
                        ),
                        (DataValue::UInt32(l), DataValue::UInt32(r)) => DataValue::Decimal(
                            Decimal::from_u32(l).unwrap() % Decimal::from_u32(r).unwrap(),
                        ),
                        (DataValue::Int64(l), DataValue::Int64(r)) => DataValue::Decimal(
                            Decimal::from_i64(l).unwrap() % Decimal::from_i64(r).unwrap(),
                        ),
                        (DataValue::UInt64(l), DataValue::UInt64(r)) => DataValue::Decimal(
                            Decimal::from_u64(l).unwrap() % Decimal::from_u64(r).unwrap(),
                        ),
                        (DataValue::Float32(l), DataValue::Float32(r)) => {
                            DataValue::Float64(l as f64 % r as f64)
                        }
                        (DataValue::Float64(l), DataValue::Float64(r)) => DataValue::Float64(l % r),
                        (DataValue::Decimal(l), DataValue::Decimal(r)) => DataValue::Decimal(l % r),
                        (l, r) => bail!("Cannot mod pair ({:?}, {:?})", l, r),
                    },
                    sqlil::BinaryOpType::Exponent => match (left, right) {
                        (DataValue::Int8(l), DataValue::Int8(r)) => DataValue::Decimal(
                            Decimal::from_i8(l)
                                .unwrap()
                                .powd(Decimal::from_i8(r).unwrap()),
                        ),
                        (DataValue::UInt8(l), DataValue::UInt8(r)) => DataValue::Decimal(
                            Decimal::from_u8(l)
                                .unwrap()
                                .powd(Decimal::from_u8(r).unwrap()),
                        ),
                        (DataValue::Int16(l), DataValue::Int16(r)) => DataValue::Decimal(
                            Decimal::from_i16(l)
                                .unwrap()
                                .powd(Decimal::from_i16(r).unwrap()),
                        ),
                        (DataValue::UInt16(l), DataValue::UInt16(r)) => DataValue::Decimal(
                            Decimal::from_u16(l)
                                .unwrap()
                                .powd(Decimal::from_u16(r).unwrap()),
                        ),
                        (DataValue::Int32(l), DataValue::Int32(r)) => DataValue::Decimal(
                            Decimal::from_i32(l)
                                .unwrap()
                                .powd(Decimal::from_i32(r).unwrap()),
                        ),
                        (DataValue::UInt32(l), DataValue::UInt32(r)) => DataValue::Decimal(
                            Decimal::from_u32(l)
                                .unwrap()
                                .powd(Decimal::from_u32(r).unwrap()),
                        ),
                        (DataValue::Int64(l), DataValue::Int64(r)) => DataValue::Decimal(
                            Decimal::from_i64(l)
                                .unwrap()
                                .powd(Decimal::from_i64(r).unwrap()),
                        ),
                        (DataValue::UInt64(l), DataValue::UInt64(r)) => DataValue::Decimal(
                            Decimal::from_u64(l)
                                .unwrap()
                                .powd(Decimal::from_u64(r).unwrap()),
                        ),
                        (DataValue::Float32(l), DataValue::Float32(r)) => {
                            DataValue::Float64((l as f64).powf(r as f64))
                        }
                        (DataValue::Float64(l), DataValue::Float64(r)) => {
                            DataValue::Float64(l.powf(r))
                        }
                        (DataValue::Decimal(l), DataValue::Decimal(r)) => {
                            DataValue::Decimal(l.powd(r))
                        }
                        (l, r) => bail!("Cannot exponent pair ({:?}, {:?})", l, r),
                    },
                    r#type @ sqlil::BinaryOpType::LogicalAnd
                    | r#type @ sqlil::BinaryOpType::LogicalOr => {
                        match (
                            left.clone().try_coerce_into(&DataType::Boolean),
                            right.clone().try_coerce_into(&DataType::Boolean),
                        ) {
                            (Ok(DataValue::Boolean(l)), Ok(DataValue::Boolean(r))) => {
                                DataValue::Boolean(match r#type {
                                    sqlil::BinaryOpType::LogicalAnd => l && r,
                                    sqlil::BinaryOpType::LogicalOr => l || r,
                                    _ => unreachable!(),
                                })
                            }
                            _ => bail!("Could not logical and pair ({:?}, {:?})", left, right),
                        }
                    }
                    sqlil::BinaryOpType::BitwiseAnd => match (left, right) {
                        (DataValue::Int8(l), DataValue::Int8(r)) => DataValue::Int8(l & r),
                        (DataValue::UInt8(l), DataValue::UInt8(r)) => DataValue::UInt8(l & r),
                        (DataValue::Int16(l), DataValue::Int16(r)) => DataValue::Int16(l & r),
                        (DataValue::UInt16(l), DataValue::UInt16(r)) => DataValue::UInt16(l & r),
                        (DataValue::Int32(l), DataValue::Int32(r)) => DataValue::Int32(l & r),
                        (DataValue::UInt32(l), DataValue::UInt32(r)) => DataValue::UInt32(l & r),
                        (DataValue::Int64(l), DataValue::Int64(r)) => DataValue::Int64(l & r),
                        (DataValue::UInt64(l), DataValue::UInt64(r)) => DataValue::UInt64(l & r),
                        (l, r) => bail!("Cannot bitwise-and pair ({:?}, {:?})", l, r),
                    },
                    sqlil::BinaryOpType::BitwiseOr => match (left, right) {
                        (DataValue::Int8(l), DataValue::Int8(r)) => DataValue::Int8(l | r),
                        (DataValue::UInt8(l), DataValue::UInt8(r)) => DataValue::UInt8(l | r),
                        (DataValue::Int16(l), DataValue::Int16(r)) => DataValue::Int16(l | r),
                        (DataValue::UInt16(l), DataValue::UInt16(r)) => DataValue::UInt16(l | r),
                        (DataValue::Int32(l), DataValue::Int32(r)) => DataValue::Int32(l | r),
                        (DataValue::UInt32(l), DataValue::UInt32(r)) => DataValue::UInt32(l | r),
                        (DataValue::Int64(l), DataValue::Int64(r)) => DataValue::Int64(l | r),
                        (DataValue::UInt64(l), DataValue::UInt64(r)) => DataValue::UInt64(l | r),
                        (l, r) => bail!("Cannot bitwise-or pair ({:?}, {:?})", l, r),
                    },
                    sqlil::BinaryOpType::BitwiseXor => match (left, right) {
                        (DataValue::Int8(l), DataValue::Int8(r)) => DataValue::Int8(l ^ r),
                        (DataValue::UInt8(l), DataValue::UInt8(r)) => DataValue::UInt8(l ^ r),
                        (DataValue::Int16(l), DataValue::Int16(r)) => DataValue::Int16(l ^ r),
                        (DataValue::UInt16(l), DataValue::UInt16(r)) => DataValue::UInt16(l ^ r),
                        (DataValue::Int32(l), DataValue::Int32(r)) => DataValue::Int32(l ^ r),
                        (DataValue::UInt32(l), DataValue::UInt32(r)) => DataValue::UInt32(l ^ r),
                        (DataValue::Int64(l), DataValue::Int64(r)) => DataValue::Int64(l ^ r),
                        (DataValue::UInt64(l), DataValue::UInt64(r)) => DataValue::UInt64(l ^ r),
                        (l, r) => bail!("Cannot bitwise-xor pair ({:?}, {:?})", l, r),
                    },
                    sqlil::BinaryOpType::BitwiseShiftLeft => match (left, right) {
                        (DataValue::Int8(l), DataValue::Int8(r)) => DataValue::Int8(l << r),
                        (DataValue::UInt8(l), DataValue::UInt8(r)) => DataValue::UInt8(l << r),
                        (DataValue::Int16(l), DataValue::Int16(r)) => DataValue::Int16(l << r),
                        (DataValue::UInt16(l), DataValue::UInt16(r)) => DataValue::UInt16(l << r),
                        (DataValue::Int32(l), DataValue::Int32(r)) => DataValue::Int32(l << r),
                        (DataValue::UInt32(l), DataValue::UInt32(r)) => DataValue::UInt32(l << r),
                        (DataValue::Int64(l), DataValue::Int64(r)) => DataValue::Int64(l << r),
                        (DataValue::UInt64(l), DataValue::UInt64(r)) => DataValue::UInt64(l << r),
                        (l, r) => bail!("Cannot bitshift-left pair ({:?}, {:?})", l, r),
                    },
                    sqlil::BinaryOpType::BitwiseShiftRight => match (left, right) {
                        (DataValue::Int8(l), DataValue::Int8(r)) => DataValue::Int8(l >> r),
                        (DataValue::UInt8(l), DataValue::UInt8(r)) => DataValue::UInt8(l >> r),
                        (DataValue::Int16(l), DataValue::Int16(r)) => DataValue::Int16(l >> r),
                        (DataValue::UInt16(l), DataValue::UInt16(r)) => DataValue::UInt16(l >> r),
                        (DataValue::Int32(l), DataValue::Int32(r)) => DataValue::Int32(l >> r),
                        (DataValue::UInt32(l), DataValue::UInt32(r)) => DataValue::UInt32(l >> r),
                        (DataValue::Int64(l), DataValue::Int64(r)) => DataValue::Int64(l >> r),
                        (DataValue::UInt64(l), DataValue::UInt64(r)) => DataValue::UInt64(l >> r),
                        (l, r) => bail!("Cannot bitshift-right pair ({:?}, {:?})", l, r),
                    },
                    sqlil::BinaryOpType::Concat => {
                        let string = DataType::Utf8String(StringOptions::default());
                        let left = left.try_coerce_into(&string)?;
                        let right = right.try_coerce_into(&string)?;

                        match (left, right) {
                            (DataValue::Utf8String(left), DataValue::Utf8String(right)) => {
                                DataValue::Utf8String(left + &right)
                            }
                            _ => unreachable!(),
                        }
                    }
                    sqlil::BinaryOpType::Regexp => todo!(),
                    sqlil::BinaryOpType::Equal => DataValue::Boolean(left == right),
                    sqlil::BinaryOpType::NullSafeEqual => DataValue::Boolean(left == right),
                    sqlil::BinaryOpType::NotEqual => DataValue::Boolean(left != right),
                    sqlil::BinaryOpType::GreaterThan => match (left, right) {
                        (DataValue::Int8(l), DataValue::Int8(r)) => DataValue::Boolean(l > r),
                        (DataValue::UInt8(l), DataValue::UInt8(r)) => DataValue::Boolean(l > r),
                        (DataValue::Int16(l), DataValue::Int16(r)) => DataValue::Boolean(l > r),
                        (DataValue::UInt16(l), DataValue::UInt16(r)) => DataValue::Boolean(l > r),
                        (DataValue::Int32(l), DataValue::Int32(r)) => DataValue::Boolean(l > r),
                        (DataValue::UInt32(l), DataValue::UInt32(r)) => DataValue::Boolean(l > r),
                        (DataValue::Int64(l), DataValue::Int64(r)) => DataValue::Boolean(l > r),
                        (DataValue::UInt64(l), DataValue::UInt64(r)) => DataValue::Boolean(l > r),
                        (DataValue::Float32(l), DataValue::Float32(r)) => DataValue::Boolean(l > r),
                        (DataValue::Float64(l), DataValue::Float64(r)) => DataValue::Boolean(l > r),
                        (DataValue::Decimal(l), DataValue::Decimal(r)) => DataValue::Boolean(l > r),
                        (l, r) => bail!("Cannot compare pair ({:?}, {:?})", l, r),
                    },
                    sqlil::BinaryOpType::GreaterThanOrEqual => match (left, right) {
                        (DataValue::Int8(l), DataValue::Int8(r)) => DataValue::Boolean(l >= r),
                        (DataValue::UInt8(l), DataValue::UInt8(r)) => DataValue::Boolean(l >= r),
                        (DataValue::Int16(l), DataValue::Int16(r)) => DataValue::Boolean(l >= r),
                        (DataValue::UInt16(l), DataValue::UInt16(r)) => DataValue::Boolean(l >= r),
                        (DataValue::Int32(l), DataValue::Int32(r)) => DataValue::Boolean(l >= r),
                        (DataValue::UInt32(l), DataValue::UInt32(r)) => DataValue::Boolean(l >= r),
                        (DataValue::Int64(l), DataValue::Int64(r)) => DataValue::Boolean(l >= r),
                        (DataValue::UInt64(l), DataValue::UInt64(r)) => DataValue::Boolean(l >= r),
                        (DataValue::Float32(l), DataValue::Float32(r)) => {
                            DataValue::Boolean(l >= r)
                        }
                        (DataValue::Float64(l), DataValue::Float64(r)) => {
                            DataValue::Boolean(l >= r)
                        }
                        (DataValue::Decimal(l), DataValue::Decimal(r)) => {
                            DataValue::Boolean(l >= r)
                        }
                        (l, r) => bail!("Cannot compare pair ({:?}, {:?})", l, r),
                    },
                    sqlil::BinaryOpType::LessThan => match (left, right) {
                        (DataValue::Int8(l), DataValue::Int8(r)) => DataValue::Boolean(l < r),
                        (DataValue::UInt8(l), DataValue::UInt8(r)) => DataValue::Boolean(l < r),
                        (DataValue::Int16(l), DataValue::Int16(r)) => DataValue::Boolean(l < r),
                        (DataValue::UInt16(l), DataValue::UInt16(r)) => DataValue::Boolean(l < r),
                        (DataValue::Int32(l), DataValue::Int32(r)) => DataValue::Boolean(l < r),
                        (DataValue::UInt32(l), DataValue::UInt32(r)) => DataValue::Boolean(l < r),
                        (DataValue::Int64(l), DataValue::Int64(r)) => DataValue::Boolean(l < r),
                        (DataValue::UInt64(l), DataValue::UInt64(r)) => DataValue::Boolean(l < r),
                        (DataValue::Float32(l), DataValue::Float32(r)) => DataValue::Boolean(l < r),
                        (DataValue::Float64(l), DataValue::Float64(r)) => DataValue::Boolean(l < r),
                        (DataValue::Decimal(l), DataValue::Decimal(r)) => DataValue::Boolean(l < r),
                        (l, r) => bail!("Cannot compare pair ({:?}, {:?})", l, r),
                    },
                    sqlil::BinaryOpType::LessThanOrEqual => match (left, right) {
                        (DataValue::Int8(l), DataValue::Int8(r)) => DataValue::Boolean(l <= r),
                        (DataValue::UInt8(l), DataValue::UInt8(r)) => DataValue::Boolean(l <= r),
                        (DataValue::Int16(l), DataValue::Int16(r)) => DataValue::Boolean(l <= r),
                        (DataValue::UInt16(l), DataValue::UInt16(r)) => DataValue::Boolean(l <= r),
                        (DataValue::Int32(l), DataValue::Int32(r)) => DataValue::Boolean(l <= r),
                        (DataValue::UInt32(l), DataValue::UInt32(r)) => DataValue::Boolean(l <= r),
                        (DataValue::Int64(l), DataValue::Int64(r)) => DataValue::Boolean(l <= r),
                        (DataValue::UInt64(l), DataValue::UInt64(r)) => DataValue::Boolean(l <= r),
                        (DataValue::Float32(l), DataValue::Float32(r)) => {
                            DataValue::Boolean(l <= r)
                        }
                        (DataValue::Float64(l), DataValue::Float64(r)) => {
                            DataValue::Boolean(l <= r)
                        }
                        (DataValue::Decimal(l), DataValue::Decimal(r)) => {
                            DataValue::Boolean(l <= r)
                        }
                        (l, r) => bail!("Cannot compare pair ({:?}, {:?})", l, r),
                    },
                })
            }
            sqlil::Expr::Cast(cast) => {
                let val = self.evaluate(data, &cast.expr)?.as_cell()?;
                DataContext::Cell(val.try_coerce_into(&cast.r#type)?)
            }
            sqlil::Expr::FunctionCall(call) => self.evaluate_func_call(data, call)?,
            sqlil::Expr::AggregateCall(call) => self.evaluate_agg_call(data, call)?,
        })
    }

    fn evaluate_func_call(
        &self,
        data: &DataContext,
        call: &sqlil::FunctionCall,
    ) -> Result<DataContext> {
        Ok(DataContext::Cell(match call {
            sqlil::FunctionCall::Abs(arg) => match self.evaluate(data, arg)?.as_cell()? {
                DataValue::Int8(v) => DataValue::Int8(v.abs()),
                DataValue::UInt8(v) => DataValue::UInt8(v),
                DataValue::Int16(v) => DataValue::Int16(v.abs()),
                DataValue::UInt16(v) => DataValue::UInt16(v),
                DataValue::Int32(v) => DataValue::Int32(v.abs()),
                DataValue::UInt32(v) => DataValue::UInt32(v),
                DataValue::Int64(v) => DataValue::Int64(v.abs()),
                DataValue::UInt64(v) => DataValue::UInt64(v),
                DataValue::Float32(v) => DataValue::Float32(v.abs()),
                DataValue::Float64(v) => DataValue::Float64(v.abs()),
                DataValue::Decimal(v) => DataValue::Decimal(v.abs()),
                val => bail!("Cannot abs val: {:?}", val),
            },
            sqlil::FunctionCall::Length(arg) => match self
                .evaluate(data, arg)?
                .as_cell()?
                .try_coerce_into(&DataType::Utf8String(StringOptions::default()))?
            {
                DataValue::Utf8String(data) => DataValue::UInt32(data.len() as _),
                _ => unreachable!(),
            },
            sqlil::FunctionCall::Uppercase(arg) => match self
                .evaluate(data, arg)?
                .as_cell()?
                .try_coerce_into(&DataType::Utf8String(StringOptions::default()))?
            {
                DataValue::Utf8String(data) => DataValue::Utf8String(data.to_uppercase()),
                _ => unreachable!(),
            },
            sqlil::FunctionCall::Lowercase(arg) => match self
                .evaluate(data, arg)?
                .as_cell()?
                .try_coerce_into(&DataType::Utf8String(StringOptions::default()))?
            {
                DataValue::Utf8String(data) => DataValue::Utf8String(data.to_lowercase()),
                _ => unreachable!(),
            },
            sqlil::FunctionCall::Substring(call) => {
                let string = self
                    .evaluate(data, &call.string)?
                    .as_cell()?
                    .try_coerce_into(&DataType::Utf8String(StringOptions::default()))?;
                let start = self
                    .evaluate(data, &call.start)?
                    .as_cell()?
                    .try_coerce_into(&DataType::UInt64)?;
                let len = self
                    .evaluate(data, &call.len)?
                    .as_cell()?
                    .try_coerce_into(&DataType::UInt64)?;

                match (string, start, len) {
                    (
                        DataValue::Utf8String(data),
                        DataValue::UInt64(start),
                        DataValue::UInt64(len),
                    ) => DataValue::Utf8String(data[(start as usize - 1)..(len as usize)].into()),
                    _ => unreachable!(),
                }
            }
            sqlil::FunctionCall::Uuid => DataValue::Uuid(Uuid::new_v4()),
            sqlil::FunctionCall::Coalesce(args) => {
                for arg in args {
                    let arg = self.evaluate(data, arg)?.as_cell()?;

                    if !arg.is_null() {
                        return Ok(DataContext::Cell(arg));
                    }
                }

                DataValue::Null
            }
        }))
    }

    fn evaluate_agg_call(
        &self,
        data: &DataContext,
        call: &sqlil::AggregateCall,
    ) -> Result<DataContext> {
        Ok(DataContext::Cell(match call {
            sqlil::AggregateCall::Sum(arg) => self
                .evaluate_group(data, arg)
                .and_then(|group| {
                    self.agg_reduce(group, |a, b| {
                        sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                            sqlil::Expr::constant(a),
                            sqlil::BinaryOpType::Add,
                            sqlil::Expr::constant(b),
                        ))
                    })
                })
                .and_then(|res| {
                    res.try_coerce_into(&DataType::Decimal(DecimalOptions::default()))
                })?,
            sqlil::AggregateCall::Count => DataValue::UInt64(data.as_group_ref()?.len() as _),
            sqlil::AggregateCall::CountDistinct(arg) => {
                self.evaluate_group(data, arg).map(|group| {
                    DataValue::UInt64(group.into_iter().unique().collect::<Vec<_>>().len() as _)
                })?
            }
            sqlil::AggregateCall::Max(arg) => self.evaluate_group(data, arg).and_then(|group| {
                Ok(group
                    .into_iter()
                    .map(|v| v.try_coerce_into(&DataType::Decimal(DecimalOptions::default())))
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .max_by_key(|a| match a {
                        DataValue::Decimal(v) => v.clone(),
                        _ => unreachable!(),
                    })
                    .unwrap_or(DataValue::Null))
            })?,
            sqlil::AggregateCall::Min(arg) => self.evaluate_group(data, arg).and_then(|group| {
                Ok(group
                    .into_iter()
                    .map(|v| v.try_coerce_into(&DataType::Decimal(DecimalOptions::default())))
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .min_by_key(|a| match a {
                        DataValue::Decimal(v) => v.clone(),
                        _ => unreachable!(),
                    })
                    .unwrap_or(DataValue::Null))
            })?,
            sqlil::AggregateCall::Average(arg) => {
                self.evaluate_group(data, arg).and_then(|group| {
                    let len = group.len();
                    Ok(DataValue::Decimal(
                        group
                            .into_iter()
                            .map(|v| {
                                v.try_coerce_into(&DataType::Decimal(DecimalOptions::default()))
                            })
                            .collect::<Result<Vec<_>>>()?
                            .into_iter()
                            .map(|a| match a {
                                DataValue::Decimal(v) => v.clone(),
                                _ => unreachable!(),
                            })
                            .sum::<Decimal>()
                            / Decimal::from_u64(len as _).unwrap(),
                    ))
                })?
            }
            sqlil::AggregateCall::StringAgg(call) => {
                self.evaluate_group(data, &call.expr).and_then(|group| {
                    Ok(DataValue::Utf8String(
                        group
                            .into_iter()
                            .map(|v| {
                                v.try_coerce_into(&DataType::Utf8String(StringOptions::default()))
                            })
                            .collect::<Result<Vec<_>>>()?
                            .into_iter()
                            .map(|v| match v {
                                DataValue::Utf8String(s) => s,
                                _ => unreachable!(),
                            })
                            .join(&call.separator),
                    ))
                })?
            }
        }))
    }

    fn evaluate_group(&self, data: &DataContext, expr: &sqlil::Expr) -> Result<Vec<DataValue>> {
        data.as_group_ref()?
            .clone()
            .into_iter()
            .map(|row| {
                let ctx = DataContext::Row(row);
                self.evaluate(&ctx, expr).and_then(|res| res.as_cell())
            })
            .collect::<Result<Vec<_>>>()
    }

    fn agg_reduce(
        &self,
        vals: Vec<DataValue>,
        cb: impl Fn(DataValue, DataValue) -> sqlil::Expr,
    ) -> Result<DataValue> {
        if vals.is_empty() {
            return Ok(DataValue::Null);
        }

        let mut iter = vals.into_iter();
        let mut curr = iter.next().unwrap();

        for next in iter {
            let expr = cb(curr, next);
            curr = self
                .evaluate(&DataContext::Cell(DataValue::Null), &expr)?
                .as_cell()?;
        }

        Ok(curr)
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
            sqlil::Expr::UnaryOp(op) => {
                let arg = self.evaluate_type(&op.expr)?;

                match &op.r#type {
                    sqlil::UnaryOpType::LogicalNot => DataType::Boolean,
                    sqlil::UnaryOpType::Negate => match &arg {
                        DataType::Int8 => DataType::Int8,
                        DataType::Int16 => DataType::Int16,
                        DataType::Int32 => DataType::Int32,
                        DataType::Int64 => DataType::Int64,
                        DataType::Float32 => DataType::Float32,
                        DataType::Float64 => DataType::Float64,
                        DataType::Decimal(_) => DataType::Decimal(DecimalOptions::default()),
                        _ => bail!("Cannot negate type: {:?}", arg),
                    },
                    sqlil::UnaryOpType::BitwiseNot => match &arg {
                        DataType::Int8 => DataType::Int8,
                        DataType::UInt8 => DataType::UInt8,
                        DataType::Int16 => DataType::Int16,
                        DataType::UInt16 => DataType::UInt16,
                        DataType::Int32 => DataType::Int32,
                        DataType::UInt32 => DataType::UInt32,
                        DataType::Int64 => DataType::Int64,
                        DataType::UInt64 => DataType::UInt64,
                        _ => bail!("Cannot bitwise not type: {:?}", arg),
                    },
                    sqlil::UnaryOpType::IsNull => DataType::Boolean,
                    sqlil::UnaryOpType::IsNotNull => DataType::Boolean,
                }
            }
            sqlil::Expr::BinaryOp(op) => {
                let left = self.evaluate_type(&op.left)?;
                let right = self.evaluate_type(&op.right)?;

                match op.r#type {
                    sqlil::BinaryOpType::Add => match &left {
                        DataType::Int8 => DataType::Int16,
                        DataType::UInt8 => DataType::UInt16,
                        DataType::Int16 => DataType::Int32,
                        DataType::UInt16 => DataType::UInt32,
                        DataType::Int32 => DataType::Int64,
                        DataType::UInt32 => DataType::UInt64,
                        DataType::Int64 => DataType::Decimal(DecimalOptions::default()),
                        DataType::UInt64 => DataType::Decimal(DecimalOptions::default()),
                        DataType::Float32 => DataType::Float64,
                        DataType::Float64 => DataType::Float64,
                        DataType::Decimal(_) => DataType::Decimal(DecimalOptions::default()),
                        _ => bail!("Cannot add types ({:?}, {:?}", left, right),
                    },
                    sqlil::BinaryOpType::Subtract => match &left {
                        DataType::Int8 => DataType::Int16,
                        DataType::UInt8 => DataType::Int16,
                        DataType::Int16 => DataType::Int32,
                        DataType::UInt16 => DataType::Int32,
                        DataType::Int32 => DataType::Int64,
                        DataType::UInt32 => DataType::Int64,
                        DataType::Int64 => DataType::Decimal(DecimalOptions::default()),
                        DataType::UInt64 => DataType::Decimal(DecimalOptions::default()),
                        DataType::Float32 => DataType::Float64,
                        DataType::Float64 => DataType::Float64,
                        DataType::Decimal(_) => DataType::Decimal(DecimalOptions::default()),
                        _ => bail!("Cannot subtract types ({:?}, {:?}", left, right),
                    },
                    sqlil::BinaryOpType::Multiply
                    | sqlil::BinaryOpType::Divide
                    | sqlil::BinaryOpType::Modulo
                    | sqlil::BinaryOpType::Exponent => match &left {
                        DataType::Int8
                        | DataType::UInt8
                        | DataType::Int16
                        | DataType::UInt16
                        | DataType::Int32
                        | DataType::UInt32
                        | DataType::Int64
                        | DataType::UInt64
                        | DataType::Float32
                        | DataType::Float64
                        | DataType::Decimal(_) => DataType::Decimal(DecimalOptions::default()),
                        _ => bail!(
                            "Cannot divide/multiply/mod/exp types ({:?}, {:?}",
                            left,
                            right
                        ),
                    },
                    sqlil::BinaryOpType::LogicalAnd => DataType::Boolean,
                    sqlil::BinaryOpType::LogicalOr => DataType::Boolean,
                    sqlil::BinaryOpType::BitwiseAnd
                    | sqlil::BinaryOpType::BitwiseOr
                    | sqlil::BinaryOpType::BitwiseXor
                    | sqlil::BinaryOpType::BitwiseShiftLeft
                    | sqlil::BinaryOpType::BitwiseShiftRight => match &left {
                        DataType::Int8 => DataType::Int8,
                        DataType::UInt8 => DataType::UInt8,
                        DataType::Int16 => DataType::Int16,
                        DataType::UInt16 => DataType::UInt16,
                        DataType::Int32 => DataType::Int32,
                        DataType::UInt32 => DataType::UInt32,
                        DataType::Int64 => DataType::Int64,
                        DataType::UInt64 => DataType::UInt64,
                        _ => bail!(
                            "Cannot bitwise-(and/or/xor/shift) pair ({:?}, {:?})",
                            left,
                            right
                        ),
                    },
                    sqlil::BinaryOpType::Concat => DataType::Utf8String(StringOptions::default()),
                    sqlil::BinaryOpType::Regexp => DataType::Boolean,
                    sqlil::BinaryOpType::Equal => DataType::Boolean,
                    sqlil::BinaryOpType::NullSafeEqual => DataType::Boolean,
                    sqlil::BinaryOpType::NotEqual => DataType::Boolean,
                    sqlil::BinaryOpType::GreaterThan => DataType::Boolean,
                    sqlil::BinaryOpType::GreaterThanOrEqual => DataType::Boolean,
                    sqlil::BinaryOpType::LessThan => DataType::Boolean,
                    sqlil::BinaryOpType::LessThanOrEqual => DataType::Boolean,
                }
            }
            sqlil::Expr::Cast(cast) => cast.r#type.clone(),
            sqlil::Expr::FunctionCall(call) => match call {
                sqlil::FunctionCall::Abs(arg) => self.evaluate_type(arg)?,
                sqlil::FunctionCall::Length(_) => DataType::Int32,
                sqlil::FunctionCall::Uppercase(_) => DataType::Utf8String(StringOptions::default()),
                sqlil::FunctionCall::Lowercase(_) => DataType::Utf8String(StringOptions::default()),
                sqlil::FunctionCall::Substring(_) => DataType::Utf8String(StringOptions::default()),
                sqlil::FunctionCall::Uuid => DataType::Uuid,
                sqlil::FunctionCall::Coalesce(args) => self.evaluate_type(&args[0])?,
            },
            sqlil::Expr::AggregateCall(call) => match call {
                sqlil::AggregateCall::Sum(_) => DataType::Decimal(DecimalOptions::default()),
                sqlil::AggregateCall::Count => DataType::UInt64,
                sqlil::AggregateCall::CountDistinct(_) => DataType::UInt64,
                sqlil::AggregateCall::Max(_) => DataType::Decimal(DecimalOptions::default()),
                sqlil::AggregateCall::Min(_) => DataType::Decimal(DecimalOptions::default()),
                sqlil::AggregateCall::Average(_) => DataType::Decimal(DecimalOptions::default()),
                sqlil::AggregateCall::StringAgg(_) => {
                    DataType::Utf8String(StringOptions::default())
                }
            },
        })
    }

    fn get_conf(
        &self,
        e: &sqlil::EntityId,
    ) -> Result<&EntitySource<MemoryConnectorEntitySourceConfig>> {
        let entity = self.entities.get(e)?;

        Ok(entity)
    }

    fn get_attrs(&self, a: &sqlil::EntityId) -> Result<&Vec<EntityAttributeConfig>> {
        let entity = self.get_conf(a)?;
        Ok(&entity.conf.attributes)
    }

    fn get_attr(&self, a: &sqlil::AttributeId) -> Result<EntityAttributeConfig> {
        let entity = self.query.get_entity(&a.entity_alias)?;

        if a.attribute_id == "ROWIDX" {
            return Ok(EntityAttributeConfig::minimal("ROWIDX", DataType::UInt64));
        }

        self.get_attrs(entity)?
            .iter()
            .find(|i| i.id == a.attribute_id)
            .cloned()
            .ok_or_else(|| Error::msg(format!("Could not find attr: {:?}", a)))
    }

    fn get_attr_index(&self, a: &sqlil::AttributeId) -> Result<usize> {
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
                    .ok_or_else(|| Error::msg(format!("Could not find attr: {:?}", a)))?)
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
        config::{EntityAttributeConfig, EntityConfig, EntitySourceConfig},
        data::StringOptions,
        sqlil::{AggregateCall, Ordering},
    };

    use super::*;

    fn mock_data() -> (
        ConnectorEntityConfig<MemoryConnectorEntitySourceConfig>,
        MemoryDatabase,
    ) {
        let data = MemoryDatabase::new();
        let mut conf = ConnectorEntityConfig::new();

        conf.add(EntitySource::new(
            EntityConfig::minimal(
                "people",
                vec![
                    EntityAttributeConfig::minimal("id", DataType::UInt32),
                    EntityAttributeConfig::minimal("first_name", DataType::rust_string()),
                    EntityAttributeConfig::minimal("last_name", DataType::rust_string()),
                ],
                EntitySourceConfig::minimal(""),
            ),
            MemoryConnectorEntitySourceConfig::default(),
        ));

        conf.add(EntitySource::new(
            EntityConfig::minimal(
                "pets",
                vec![
                    EntityAttributeConfig::minimal("id", DataType::UInt32),
                    EntityAttributeConfig::minimal("owner_id", DataType::UInt32),
                    EntityAttributeConfig::minimal("pet_name", DataType::rust_string()),
                ],
                EntitySourceConfig::minimal(""),
            ),
            MemoryConnectorEntitySourceConfig::default(),
        ));

        data.set_data(
            "people",
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

        data.set_data(
            "pets",
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

        (conf, data)
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
        let mut select = sqlil::Select::new(sqlil::source("people", "people"));
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
                        DataValue::Utf8String("Mary".into()),
                        DataValue::Utf8String("Jane".into())
                    ],
                    vec![
                        DataValue::Utf8String("John".into()),
                        DataValue::Utf8String("Smith".into())
                    ],
                    vec![
                        DataValue::Utf8String("Mary".into()),
                        DataValue::Utf8String("Bennet".into())
                    ],
                ]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_select_invalid_entity() {
        let select = sqlil::Select::new(sqlil::source("invalid", "i"));

        let executor = create_executor(select, HashMap::new());

        executor.run().unwrap_err();
    }

    #[test]
    fn test_memory_connector_executor_select_no_cols() {
        let select = sqlil::Select::new(sqlil::source("people", "people"));

        let executor = create_executor(select, HashMap::new());

        let results = executor.run().unwrap();

        assert_eq!(
            results,
            MemoryResultSet::new(vec![], vec![vec![], vec![], vec![]]).unwrap()
        );
    }

    #[test]
    fn test_memory_connector_executor_select_single_column() {
        let mut select = sqlil::Select::new(sqlil::source("people", "people"));
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
                    vec![DataValue::Utf8String("Mary".into()),],
                    vec![DataValue::Utf8String("John".into()),],
                    vec![DataValue::Utf8String("Mary".into()),],
                ]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_select_where_equals() {
        let mut select = sqlil::Select::new(sqlil::source("people", "people"));
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
                vec![vec![DataValue::Utf8String("John".into()),],]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_select_skip_row() {
        let mut select = sqlil::Select::new(sqlil::source("people", "people"));
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
                    vec![DataValue::Utf8String("John".into()),],
                    vec![DataValue::Utf8String("Mary".into()),],
                ]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_select_row_limit() {
        let mut select = sqlil::Select::new(sqlil::source("people", "people"));
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
                vec![vec![DataValue::Utf8String("Mary".into()),],]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_select_group_by_column_key() {
        let mut select = sqlil::Select::new(sqlil::source("people", "people"));
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
                    vec![DataValue::Utf8String("Mary".into()),],
                    vec![DataValue::Utf8String("John".into()),],
                ]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_select_group_by_column_key_with_count() {
        let mut select = sqlil::Select::new(sqlil::source("people", "people"));
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
                    vec![DataValue::Utf8String("Mary".into()), DataValue::UInt64(2)],
                    vec![DataValue::Utf8String("John".into()), DataValue::UInt64(1)],
                ]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_select_count_implicit_group_by() {
        let mut select = sqlil::Select::new(sqlil::source("people", "people"));
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
        let mut select = sqlil::Select::new(sqlil::source("people", "people"));
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
                    vec![DataValue::Utf8String("MaryJane".into())],
                    vec![DataValue::Utf8String("JohnSmith".into())],
                    vec![DataValue::Utf8String("MaryBennet".into())]
                ]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_select_group_by_expr_key_with_count() {
        let mut select = sqlil::Select::new(sqlil::source("people", "people"));
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
                        DataValue::Utf8String("MaryJane".into()),
                        DataValue::UInt64(1)
                    ],
                    vec![
                        DataValue::Utf8String("JohnSmith".into()),
                        DataValue::UInt64(1)
                    ],
                    vec![
                        DataValue::Utf8String("MaryBennet".into()),
                        DataValue::UInt64(1)
                    ],
                ]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_select_order_by_single() {
        let mut select = sqlil::Select::new(sqlil::source("people", "people"));
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
                        DataValue::Utf8String("John".into()),
                        DataValue::Utf8String("Smith".into())
                    ],
                    vec![
                        DataValue::Utf8String("Mary".into()),
                        DataValue::Utf8String("Jane".into())
                    ],
                    vec![
                        DataValue::Utf8String("Mary".into()),
                        DataValue::Utf8String("Bennet".into())
                    ],
                ]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_select_order_by_single_desc() {
        let mut select = sqlil::Select::new(sqlil::source("people", "people"));
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
                        DataValue::Utf8String("Mary".into()),
                        DataValue::Utf8String("Jane".into())
                    ],
                    vec![
                        DataValue::Utf8String("Mary".into()),
                        DataValue::Utf8String("Bennet".into())
                    ],
                    vec![
                        DataValue::Utf8String("John".into()),
                        DataValue::Utf8String("Smith".into())
                    ],
                ]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_select_order_by_multiple() {
        let mut select = sqlil::Select::new(sqlil::source("people", "people"));
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
                        DataValue::Utf8String("John".into()),
                        DataValue::Utf8String("Smith".into())
                    ],
                    vec![
                        DataValue::Utf8String("Mary".into()),
                        DataValue::Utf8String("Jane".into())
                    ],
                    vec![
                        DataValue::Utf8String("Mary".into()),
                        DataValue::Utf8String("Bennet".into())
                    ],
                ]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_select_inner_join() {
        let mut select = sqlil::Select::new(sqlil::source("people", "people"));

        select.joins.push(sqlil::Join::new(
            sqlil::JoinType::Inner,
            sqlil::source("pets", "pets"),
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
                        DataValue::Utf8String("Mary".into()),
                        DataValue::Utf8String("Jane".into()),
                        DataValue::Utf8String("Pepper".into())
                    ],
                    vec![
                        DataValue::Utf8String("Mary".into()),
                        DataValue::Utf8String("Jane".into()),
                        DataValue::Utf8String("Salt".into())
                    ],
                    vec![
                        DataValue::Utf8String("Mary".into()),
                        DataValue::Utf8String("Bennet".into()),
                        DataValue::Utf8String("Relish".into())
                    ],
                ]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_select_left_join() {
        let mut select = sqlil::Select::new(sqlil::source("people", "people"));

        select.joins.push(sqlil::Join::new(
            sqlil::JoinType::Left,
            sqlil::source("pets", "pets"),
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
                        DataValue::Utf8String("Mary".into()),
                        DataValue::Utf8String("Jane".into()),
                        DataValue::Utf8String("Pepper".into())
                    ],
                    vec![
                        DataValue::Utf8String("Mary".into()),
                        DataValue::Utf8String("Jane".into()),
                        DataValue::Utf8String("Salt".into())
                    ],
                    vec![
                        DataValue::Utf8String("Mary".into()),
                        DataValue::Utf8String("Bennet".into()),
                        DataValue::Utf8String("Relish".into())
                    ],
                    vec![
                        DataValue::Utf8String("John".into()),
                        DataValue::Utf8String("Smith".into()),
                        DataValue::Null,
                    ],
                ]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_select_right_join() {
        let mut select = sqlil::Select::new(sqlil::source("people", "people"));

        select.joins.push(sqlil::Join::new(
            sqlil::JoinType::Right,
            sqlil::source("pets", "pets"),
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
                        DataValue::Utf8String("Mary".into()),
                        DataValue::Utf8String("Jane".into()),
                        DataValue::Utf8String("Pepper".into())
                    ],
                    vec![
                        DataValue::Utf8String("Mary".into()),
                        DataValue::Utf8String("Jane".into()),
                        DataValue::Utf8String("Salt".into())
                    ],
                    vec![
                        DataValue::Utf8String("Mary".into()),
                        DataValue::Utf8String("Bennet".into()),
                        DataValue::Utf8String("Relish".into())
                    ],
                    vec![
                        DataValue::Null,
                        DataValue::Null,
                        DataValue::Utf8String("Luna".into()),
                    ],
                ]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_select_full_join() {
        let mut select = sqlil::Select::new(sqlil::source("people", "people"));

        select.joins.push(sqlil::Join::new(
            sqlil::JoinType::Full,
            sqlil::source("pets", "pets"),
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
                        DataValue::Utf8String("Mary".into()),
                        DataValue::Utf8String("Jane".into()),
                        DataValue::Utf8String("Pepper".into())
                    ],
                    vec![
                        DataValue::Utf8String("Mary".into()),
                        DataValue::Utf8String("Jane".into()),
                        DataValue::Utf8String("Salt".into())
                    ],
                    vec![
                        DataValue::Utf8String("Mary".into()),
                        DataValue::Utf8String("Bennet".into()),
                        DataValue::Utf8String("Relish".into())
                    ],
                    vec![
                        DataValue::Utf8String("John".into()),
                        DataValue::Utf8String("Smith".into()),
                        DataValue::Null,
                    ],
                    vec![
                        DataValue::Null,
                        DataValue::Null,
                        DataValue::Utf8String("Luna".into()),
                    ],
                ]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_select_where_parameter() {
        let mut select = sqlil::Select::new(sqlil::source("people", "people"));
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
            [(1, DataValue::Utf8String("John".into()))]
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
                    DataValue::Utf8String("John".into()),
                    DataValue::Utf8String("Smith".into())
                ],]
            )
            .unwrap()
        )
    }

    #[test]
    fn test_memory_connector_executor_insert_empty_row() {
        let insert = sqlil::Insert::new(sqlil::source("people", "people"));

        let executor = create_executor(insert, HashMap::new());

        let results = executor.run().unwrap();

        assert_eq!(results, MemoryResultSet::empty());

        executor
            .data
            .with_data("people", |data| {
                assert_eq!(
                    data.iter().last().unwrap(),
                    &vec![
                        DataValue::Null,
                        DataValue::Null,
                        DataValue::Null,
                        DataValue::UInt64(3)
                    ]
                );
            })
            .unwrap();
    }

    #[test]
    fn test_memory_connector_executor_insert_row_with_values_row() {
        let mut insert = sqlil::Insert::new(sqlil::source("people", "people"));

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
            .with_data("people", |data| {
                assert_eq!(
                    data.iter().last().unwrap(),
                    &vec![
                        DataValue::UInt32(123),
                        DataValue::from("New"),
                        DataValue::from("Man"),
                        DataValue::UInt64(3)
                    ]
                );
            })
            .unwrap();
    }

    #[test]
    fn test_memory_connector_executor_bulk_insert_rows() {
        let mut insert = sqlil::BulkInsert::new(sqlil::source("people", "people"));
        insert.cols = vec!["id".into(), "first_name".into(), "last_name".into()];

        insert.values = vec![
            sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::UInt32, 1)),
            sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::rust_string(), 2)),
            sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::rust_string(), 3)),
            //
            sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::UInt32, 4)),
            sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::rust_string(), 5)),
            sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::rust_string(), 6)),
            //
            sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::UInt32, 7)),
            sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::rust_string(), 8)),
            sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::rust_string(), 9)),
        ];

        let executor = create_executor(
            insert,
            [
                (1, DataValue::UInt32(1)),
                (2, DataValue::from("First")),
                (3, DataValue::from("Row")),
                (4, DataValue::UInt32(2)),
                (5, DataValue::from("Second")),
                (6, DataValue::from("Record")),
                (7, DataValue::UInt32(3)),
                (8, DataValue::from("Third")),
                (9, DataValue::from("Entity")),
            ]
            .into_iter()
            .collect(),
        );

        let results = executor.run().unwrap();

        assert_eq!(results, MemoryResultSet::empty());

        executor
            .data
            .with_data("people", |data| {
                assert_eq!(
                    data[data.len() - 3..],
                    [
                        vec![
                            DataValue::UInt32(1),
                            DataValue::from("First"),
                            DataValue::from("Row"),
                            DataValue::UInt64(3)
                        ],
                        vec![
                            DataValue::UInt32(2),
                            DataValue::from("Second"),
                            DataValue::from("Record"),
                            DataValue::UInt64(4)
                        ],
                        vec![
                            DataValue::UInt32(3),
                            DataValue::from("Third"),
                            DataValue::from("Entity"),
                            DataValue::UInt64(5)
                        ]
                    ]
                );
            })
            .unwrap();
    }

    #[test]
    fn test_memory_connector_executor_update_no_set() {
        let update = sqlil::Update::new(sqlil::source("people", "people"));

        let executor = create_executor(update, HashMap::new());
        let orig_data = executor
            .data
            .with_data("people", |data| data.clone())
            .unwrap();

        let results = executor.run().unwrap();

        assert_eq!(results, MemoryResultSet::empty());

        executor
            .data
            .with_data("people", |data| {
                assert_eq!(data, &orig_data);
            })
            .unwrap();
    }

    #[test]
    fn test_memory_connector_executor_update_all_rows() {
        let mut update = sqlil::Update::new(sqlil::source("people", "people"));

        update.cols.push((
            "first_name".into(),
            sqlil::Expr::constant(DataValue::from("New")),
        ));

        let executor = create_executor(update, HashMap::new());

        let results = executor.run().unwrap();

        assert_eq!(results, MemoryResultSet::empty());

        executor
            .data
            .with_data("people", |data| {
                assert!(data.into_iter().all(|r| r[1] == DataValue::from("New")))
            })
            .unwrap();
    }

    #[test]
    fn test_memory_connector_executor_update_where() {
        let mut update = sqlil::Update::new(sqlil::source("people", "people"));

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
            .with_data("people", |data| {
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
        let delete = sqlil::Delete::new(sqlil::source("people", "people"));

        let executor = create_executor(delete, HashMap::new());

        let results = executor.run().unwrap();

        assert_eq!(results, MemoryResultSet::empty());

        executor
            .data
            .with_data("people", |data| {
                assert_eq!(data, &Vec::<Vec<_>>::new());
            })
            .unwrap();
    }

    #[test]
    fn test_memory_connector_executor_delete_where() {
        let mut delete = sqlil::Delete::new(sqlil::source("people", "people"));

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
            .with_data("people", |data| {
                assert_eq!(
                    data.into_iter().map(|r| r[0].clone()).collect::<Vec<_>>(),
                    vec![DataValue::UInt32(2), DataValue::UInt32(3)]
                )
            })
            .unwrap();
    }

    #[test]
    fn test_memory_connector_executor_select_row_id() {
        let mut select = sqlil::Select::new(sqlil::source("people", "people"));
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

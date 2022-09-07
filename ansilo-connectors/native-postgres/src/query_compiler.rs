use std::{marker::PhantomData, ops::DerefMut};

use ansilo_core::{
    data::DataValue,
    err::{bail, Context, Result},
    sqlil as sql,
};

use ansilo_connectors_base::{common::query::QueryParam, interface::QueryCompiler};
use ansilo_util_pg::query::pg_quote_identifier;
use tokio_postgres::Client;

use crate::{to_pg_type, PostgresConnection, PostgresQuery};

use super::{PostgresConnectorEntityConfig, PostgresEntitySourceConfig, PostgresTableOptions};

/// Query compiler for Postgres driver
pub struct PostgresQueryCompiler<T> {
    _data: PhantomData<T>,
}

impl<T: DerefMut<Target = Client>> QueryCompiler for PostgresQueryCompiler<T> {
    type TConnection = PostgresConnection<T>;
    type TQuery = PostgresQuery;
    type TEntitySourceConfig = PostgresEntitySourceConfig;

    fn compile_query(
        _con: &mut Self::TConnection,
        conf: &PostgresConnectorEntityConfig,
        query: sql::Query,
    ) -> Result<PostgresQuery> {
        match &query {
            sql::Query::Select(select) => Self::compile_select_query(conf, &query, select),
            sql::Query::Insert(insert) => Self::compile_insert_query(conf, &query, insert),
            sql::Query::BulkInsert(insert) => Self::compile_bulk_insert_query(conf, &query, insert),
            sql::Query::Update(update) => Self::compile_update_query(conf, &query, update),
            sql::Query::Delete(delete) => Self::compile_delete_query(conf, &query, delete),
        }
    }
}

impl<T: DerefMut<Target = Client>> PostgresQueryCompiler<T> {
    fn compile_select_query(
        conf: &PostgresConnectorEntityConfig,
        query: &sql::Query,
        select: &sql::Select,
    ) -> Result<PostgresQuery> {
        let mut params = Vec::<QueryParam>::new();

        let query = [
            "SELECT".to_string(),
            Self::compile_select_cols(conf, query, &select.cols, &mut params)?,
            format!(
                "FROM {}",
                Self::compile_entity_source(conf, &select.from, true)?
            ),
            Self::compile_select_joins(conf, query, &select.joins, &mut params)?,
            Self::compile_where(conf, query, &select.r#where, &mut params)?,
            Self::compile_select_group_by(conf, query, &select.group_bys, &mut params)?,
            Self::compile_order_by(conf, query, &select.order_bys, &mut params)?,
            Self::compile_offet_limit(select.row_skip, select.row_limit)?,
            Self::compile_select_lock_clause(select.row_lock)?,
        ]
        .into_iter()
        .filter(|i| !i.is_empty())
        .collect::<Vec<String>>()
        .join(" ");

        Ok(PostgresQuery::new(query, params))
    }

    fn compile_insert_query(
        conf: &PostgresConnectorEntityConfig,
        query: &sql::Query,
        insert: &sql::Insert,
    ) -> Result<PostgresQuery> {
        let mut params = Vec::<QueryParam>::new();

        let query = [
            "INSERT INTO".to_string(),
            Self::compile_entity_source(conf, &insert.target, false)?,
            format!(
                "({})",
                insert
                    .cols
                    .iter()
                    .map(|(col, _)| Self::compile_attribute_identifier(
                        conf,
                        query,
                        &sql::AttributeId::new(&insert.target.alias, col),
                        false
                    ))
                    .collect::<Result<Vec<_>>>()?
                    .join(", "),
            ),
            "VALUES".to_string(),
            format!(
                "({})",
                insert
                    .cols
                    .iter()
                    .map(|(_, e)| Self::compile_expr(conf, query, e, &mut params))
                    .collect::<Result<Vec<_>>>()?
                    .join(", ")
            ),
        ]
        .into_iter()
        .collect::<Vec<String>>()
        .join(" ");

        Ok(PostgresQuery::new(query, params))
    }

    fn compile_bulk_insert_query(
        conf: &PostgresConnectorEntityConfig,
        query: &sql::Query,
        insert: &sql::BulkInsert,
    ) -> Result<PostgresQuery> {
        let mut params = Vec::<QueryParam>::new();

        let query = [
            "INSERT INTO".to_string(),
            Self::compile_entity_source(conf, &insert.target, false)?,
            format!(
                "({})",
                insert
                    .cols
                    .iter()
                    .map(|col| Self::compile_attribute_identifier(
                        conf,
                        query,
                        &sql::AttributeId::new(&insert.target.alias, col),
                        false
                    ))
                    .collect::<Result<Vec<_>>>()?
                    .join(", "),
            ),
            "VALUES".to_string(),
            insert
                .rows()
                .into_iter()
                .map(|row| {
                    Ok(format!(
                        "({})",
                        row.map(|e| Self::compile_expr(conf, query, e, &mut params))
                            .collect::<Result<Vec<_>>>()?
                            .join(", ")
                    ))
                })
                .collect::<Result<Vec<_>>>()?
                .join(", "),
        ]
        .into_iter()
        .collect::<Vec<String>>()
        .join(" ");

        Ok(PostgresQuery::new(query, params))
    }

    fn compile_update_query(
        conf: &PostgresConnectorEntityConfig,
        query: &sql::Query,
        update: &sql::Update,
    ) -> Result<PostgresQuery> {
        let mut params = Vec::<QueryParam>::new();

        let query = [
            "UPDATE".to_string(),
            Self::compile_entity_source(conf, &update.target, false)?,
            "SET".to_string(),
            update
                .cols
                .iter()
                .map(|(col, expr)| {
                    Ok(format!(
                        "{} = {}",
                        Self::compile_attribute_identifier(
                            conf,
                            query,
                            &sql::AttributeId::new(&update.target.alias, col),
                            false
                        )?,
                        Self::compile_expr(conf, query, expr, &mut params)?
                    ))
                })
                .collect::<Result<Vec<_>>>()?
                .join(", "),
            Self::compile_where(conf, query, &update.r#where, &mut params)?,
        ]
        .into_iter()
        .filter(|i| !i.is_empty())
        .collect::<Vec<String>>()
        .join(" ");

        Ok(PostgresQuery::new(query, params))
    }

    fn compile_delete_query(
        conf: &PostgresConnectorEntityConfig,
        query: &sql::Query,
        delete: &sql::Delete,
    ) -> Result<PostgresQuery> {
        let mut params = Vec::<QueryParam>::new();

        let query = [
            "DELETE FROM".to_string(),
            Self::compile_entity_source(conf, &delete.target, false)?,
            Self::compile_where(conf, query, &delete.r#where, &mut params)?,
        ]
        .into_iter()
        .filter(|i| !i.is_empty())
        .collect::<Vec<String>>()
        .join(" ");

        Ok(PostgresQuery::new(query, params))
    }

    fn compile_select_cols(
        conf: &PostgresConnectorEntityConfig,
        query: &sql::Query,
        cols: &Vec<(String, sql::Expr)>,
        params: &mut Vec<QueryParam>,
    ) -> Result<String> {
        Ok(cols
            .into_iter()
            .map(|i| {
                Ok(format!(
                    "{} AS {}",
                    Self::compile_expr(conf, query, &i.1, params)?,
                    Self::compile_identifier(i.0.clone())?
                ))
            })
            .collect::<Result<Vec<String>>>()?
            .join(", "))
    }

    fn compile_select_joins(
        conf: &PostgresConnectorEntityConfig,
        query: &sql::Query,
        joins: &Vec<sql::Join>,
        params: &mut Vec<QueryParam>,
    ) -> Result<String> {
        Ok(joins
            .into_iter()
            .map(|j| Ok(Self::compile_select_join(conf, query, j, params)?))
            .collect::<Result<Vec<String>>>()?
            .join(", "))
    }

    fn compile_select_join(
        conf: &PostgresConnectorEntityConfig,
        query: &sql::Query,
        join: &sql::Join,
        params: &mut Vec<QueryParam>,
    ) -> Result<String> {
        let target = Self::compile_entity_source(conf, &join.target, true)?;
        let cond = if join.conds.is_empty() {
            "1=1".to_string()
        } else {
            format!(
                "({})",
                join.conds
                    .iter()
                    .map(|e| Ok(Self::compile_expr(conf, query, e, params)?))
                    .collect::<Result<Vec<String>>>()?
                    .join(") AND (")
            )
        };

        Ok(match join.r#type {
            sql::JoinType::Inner => format!("INNER JOIN {} ON {}", target, cond),
            sql::JoinType::Left => format!("LEFT JOIN {} ON {}", target, cond),
            sql::JoinType::Right => format!("RIGHT JOIN {} ON {}", target, cond),
            sql::JoinType::Full => format!("FULL JOIN {} ON {}", target, cond),
        })
    }

    fn compile_where(
        conf: &PostgresConnectorEntityConfig,
        query: &sql::Query,
        r#where: &Vec<sql::Expr>,
        params: &mut Vec<QueryParam>,
    ) -> Result<String> {
        if r#where.is_empty() {
            return Ok("".to_string());
        }

        let clauses = r#where
            .into_iter()
            .map(|e| Ok(Self::compile_expr(conf, query, e, params)?))
            .collect::<Result<Vec<String>>>()?
            .join(") AND (");

        Ok(format!("WHERE ({})", clauses))
    }

    fn compile_select_group_by(
        conf: &PostgresConnectorEntityConfig,
        query: &sql::Query,
        group_bys: &Vec<sql::Expr>,
        params: &mut Vec<QueryParam>,
    ) -> Result<String> {
        if group_bys.is_empty() {
            return Ok("".to_string());
        }

        let clauses = group_bys
            .into_iter()
            .map(|e| Ok(Self::compile_expr(conf, query, e, params)?))
            .collect::<Result<Vec<String>>>()?
            .join(", ");

        Ok(format!("GROUP BY {}", clauses))
    }

    fn compile_order_by(
        conf: &PostgresConnectorEntityConfig,
        query: &sql::Query,
        order_bys: &Vec<sql::Ordering>,
        params: &mut Vec<QueryParam>,
    ) -> Result<String> {
        if order_bys.is_empty() {
            return Ok("".to_string());
        }

        let clauses = order_bys
            .into_iter()
            .map(|i| {
                Ok(format!(
                    "{} {}",
                    Self::compile_expr(conf, query, &i.expr, params)?,
                    match i.r#type {
                        sql::OrderingType::Asc => "ASC",
                        sql::OrderingType::Desc => "DESC",
                    }
                ))
            })
            .collect::<Result<Vec<String>>>()?
            .join(", ");

        Ok(format!("ORDER BY {}", clauses))
    }

    fn compile_offet_limit(row_skip: u64, row_limit: Option<u64>) -> Result<String> {
        let mut parts = vec![];

        if let Some(lim) = row_limit {
            parts.push(format!("LIMIT {}", lim));
        }

        if row_skip > 0 {
            parts.push(format!("OFFSET {}", row_skip));
        }

        Ok(parts.join(" "))
    }

    fn compile_select_lock_clause(mode: sql::SelectRowLockMode) -> Result<String> {
        Ok(match mode {
            sql::SelectRowLockMode::None => "",
            sql::SelectRowLockMode::ForUpdate => "FOR UPDATE",
        }
        .into())
    }

    fn compile_expr(
        conf: &PostgresConnectorEntityConfig,
        query: &sql::Query,
        expr: &sql::Expr,
        params: &mut Vec<QueryParam>,
    ) -> Result<String> {
        let sql = match expr {
            sql::Expr::Attribute(eva) => {
                Self::compile_attribute_identifier(conf, query, eva, true)?
            }
            sql::Expr::Constant(c) => Self::compile_constant(c, params)?,
            sql::Expr::Parameter(p) => Self::compile_param(p, params)?,
            sql::Expr::UnaryOp(o) => Self::compile_unary_op(conf, query, o, params)?,
            sql::Expr::BinaryOp(b) => Self::compile_binary_op(conf, query, b, params)?,
            sql::Expr::Cast(c) => Self::compile_cast(conf, query, c, params)?,
            sql::Expr::FunctionCall(f) => Self::compile_function_call(conf, query, f, params)?,
            sql::Expr::AggregateCall(a) => Self::compile_aggregate_call(conf, query, a, params)?,
        };

        Ok(sql)
    }

    pub fn compile_identifier(id: String) -> Result<String> {
        // @see https://dev.postgres.com/doc/refman/8.0/en/identifiers.html#:~:text=An%20identifier%20may%20be%20quoted,it%20need%20not%20be%20quoted.)
        if id.contains("\0") {
            bail!("Invalid identifier: \"{id}\", cannot contain '\\0' chars");
        }

        Ok(pg_quote_identifier(&id))
    }

    pub fn compile_entity_source(
        conf: &PostgresConnectorEntityConfig,
        source: &sql::EntitySource,
        include_alias: bool,
    ) -> Result<String> {
        let entity = conf
            .get(&source.entity)
            .with_context(|| format!("Failed to find entity {:?}", source.entity.clone()))?;

        let id = Self::compile_source_identifier(&entity.source)?;

        Ok(if include_alias {
            let alias = Self::compile_identifier(source.alias.clone())?;

            format!("{id} AS {alias}")
        } else {
            id
        })
    }

    pub fn compile_source_identifier(source: &PostgresEntitySourceConfig) -> Result<String> {
        Ok(match &source {
            PostgresEntitySourceConfig::Table(PostgresTableOptions {
                schema_name: Some(schema),
                table_name: table,
                ..
            }) => format!(
                "{}.{}",
                Self::compile_identifier(schema.clone())?,
                Self::compile_identifier(table.clone())?
            ),
            PostgresEntitySourceConfig::Table(PostgresTableOptions {
                schema_name: None,
                table_name: table,
                ..
            }) => Self::compile_identifier(table.clone())?,
        })
    }

    fn compile_attribute_identifier(
        conf: &PostgresConnectorEntityConfig,
        query: &sql::Query,
        eva: &sql::AttributeId,
        include_table: bool,
    ) -> Result<String> {
        let source = query.get_entity_source(&eva.entity_alias)?;
        let entity = conf
            .get(&source.entity)
            .with_context(|| format!("Failed to find entity {:?}", source.entity.clone()))?;

        let table = match &entity.source {
            PostgresEntitySourceConfig::Table(table) => table,
        };

        let column = table
            .attribute_column_map
            .get(&eva.attribute_id)
            .unwrap_or(&eva.attribute_id);

        let table_alias = if query.as_select().is_some() {
            eva.entity_alias.clone()
        } else {
            table.table_name.clone()
        };

        Ok(if include_table {
            vec![
                Self::compile_identifier(table_alias)?,
                Self::compile_identifier(column.clone())?,
            ]
            .join(".")
        } else {
            Self::compile_identifier(column.clone())?
        })
    }

    fn compile_constant(c: &sql::Constant, params: &mut Vec<QueryParam>) -> Result<String> {
        params.push(QueryParam::Constant(c.value.clone()));
        Ok(format!("${}", params.len()))
    }

    fn compile_param(p: &sql::Parameter, params: &mut Vec<QueryParam>) -> Result<String> {
        params.push(QueryParam::Dynamic(p.clone()));
        Ok(format!("${}", params.len()))
    }

    fn compile_unary_op(
        conf: &PostgresConnectorEntityConfig,
        query: &sql::Query,
        op: &sql::UnaryOp,
        params: &mut Vec<QueryParam>,
    ) -> Result<String> {
        let inner = Self::compile_expr(conf, query, &*op.expr, params)?;

        Ok(match op.r#type {
            sql::UnaryOpType::LogicalNot => format!("!({})", inner),
            sql::UnaryOpType::Negate => format!("-({})", inner),
            sql::UnaryOpType::BitwiseNot => format!("~({})", inner),
            sql::UnaryOpType::IsNull => format!("({}) IS NULL", inner),
            sql::UnaryOpType::IsNotNull => format!("({}) IS NOT NULL", inner),
        })
    }

    fn compile_binary_op(
        conf: &PostgresConnectorEntityConfig,
        query: &sql::Query,
        op: &sql::BinaryOp,
        params: &mut Vec<QueryParam>,
    ) -> Result<String> {
        let l = Self::compile_expr(conf, query, &*op.left, params)?;
        let r = Self::compile_expr(conf, query, &*op.right, params)?;

        Ok(match op.r#type {
            sql::BinaryOpType::Add => format!("({}) + ({})", l, r),
            sql::BinaryOpType::Subtract => format!("({}) - ({})", l, r),
            sql::BinaryOpType::Multiply => format!("({}) * ({})", l, r),
            sql::BinaryOpType::Divide => format!("({}) / ({})", l, r),
            sql::BinaryOpType::LogicalAnd => format!("({}) AND ({})", l, r),
            sql::BinaryOpType::LogicalOr => format!("({}) OR ({})", l, r),
            sql::BinaryOpType::Modulo => format!("({}) % ({})", l, r),
            sql::BinaryOpType::Exponent => format!("pow({}, {})", l, r),
            sql::BinaryOpType::BitwiseAnd => format!("({}) & ({})", l, r),
            sql::BinaryOpType::BitwiseOr => format!("({}) | ({})", l, r),
            sql::BinaryOpType::BitwiseXor => format!("({}) ^ ({})", l, r),
            sql::BinaryOpType::BitwiseShiftLeft => format!("({}) << ({})", l, r),
            sql::BinaryOpType::BitwiseShiftRight => format!("({}) >> ({})", l, r),
            sql::BinaryOpType::Concat => format!("({}) || ({})", l, r),
            sql::BinaryOpType::Regexp => format!("({}) ~ ({})", l, r),
            sql::BinaryOpType::Equal => format!("({}) = ({})", l, r),
            sql::BinaryOpType::NullSafeEqual => format!("({}) IS DISTINCT FROM ({})", l, r),
            sql::BinaryOpType::NotEqual => format!("({}) != ({})", l, r),
            sql::BinaryOpType::GreaterThan => format!("({}) > ({})", l, r),
            sql::BinaryOpType::GreaterThanOrEqual => format!("({}) >= ({})", l, r),
            sql::BinaryOpType::LessThan => format!("({}) < ({})", l, r),
            sql::BinaryOpType::LessThanOrEqual => format!("({}) <= ({})", l, r),
        })
    }

    fn compile_cast(
        conf: &PostgresConnectorEntityConfig,
        query: &sql::Query,
        cast: &sql::Cast,
        params: &mut Vec<QueryParam>,
    ) -> Result<String> {
        let arg = Self::compile_expr(conf, query, &cast.expr, params)?;

        Ok(format!("({})::{}", arg, to_pg_type(&cast.r#type).name()))
    }

    fn compile_function_call(
        conf: &PostgresConnectorEntityConfig,
        query: &sql::Query,
        func: &sql::FunctionCall,
        params: &mut Vec<QueryParam>,
    ) -> Result<String> {
        Ok(match func {
            sql::FunctionCall::Length(arg) => {
                format!(
                    "length({})",
                    Self::compile_expr(conf, query, &*arg, params)?
                )
            }
            sql::FunctionCall::Abs(arg) => {
                format!("abs({})", Self::compile_expr(conf, query, &*arg, params)?)
            }
            sql::FunctionCall::Uppercase(arg) => {
                format!("upper({})", Self::compile_expr(conf, query, &*arg, params)?)
            }
            sql::FunctionCall::Lowercase(arg) => {
                format!("lower({})", Self::compile_expr(conf, query, &*arg, params)?)
            }
            sql::FunctionCall::Substring(call) => format!(
                "substring({} FROM {} FOR {})",
                Self::compile_expr(conf, query, &*call.string, params)?,
                Self::compile_expr(conf, query, &*call.start, params)?,
                Self::compile_expr(conf, query, &*call.len, params)?
            ),
            sql::FunctionCall::Uuid => "gen_random_uuid()".into(),
            sql::FunctionCall::Coalesce(args) => format!(
                "coalecse({})",
                args.iter()
                    .map(|arg| Self::compile_expr(conf, query, &**arg, params))
                    .collect::<Result<Vec<_>>>()?
                    .join(", ")
            ),
        })
    }

    fn compile_aggregate_call(
        conf: &PostgresConnectorEntityConfig,
        query: &sql::Query,
        agg: &sql::AggregateCall,
        params: &mut Vec<QueryParam>,
    ) -> Result<String> {
        Ok(match agg {
            sql::AggregateCall::Sum(arg) => {
                format!("sum({})", Self::compile_expr(conf, query, &*arg, params)?)
            }
            sql::AggregateCall::Count => "count(*)".into(),
            sql::AggregateCall::CountDistinct(arg) => format!(
                "count(distinct {})",
                Self::compile_expr(conf, query, &*arg, params)?
            ),
            sql::AggregateCall::Max(arg) => {
                format!("max({})", Self::compile_expr(conf, query, &*arg, params)?)
            }
            sql::AggregateCall::Min(arg) => {
                format!("min({})", Self::compile_expr(conf, query, &*arg, params)?)
            }
            sql::AggregateCall::Average(arg) => {
                format!("avg({})", Self::compile_expr(conf, query, &*arg, params)?)
            }
            sql::AggregateCall::StringAgg(call) => {
                params.push(QueryParam::Constant(DataValue::Utf8String(
                    call.separator.clone(),
                )));
                format!(
                    "string_agg({}, {})",
                    Self::compile_expr(conf, query, &call.expr, params)?,
                    params.len()
                )
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use ansilo_core::{
        config::{EntityConfig, EntitySourceConfig},
        data::{DataType, DataValue},
    };

    use ansilo_connectors_base::common::entity::EntitySource;
    use pretty_assertions::assert_eq;

    use crate::PooledClient;

    use super::*;

    fn compile_select(select: sql::Select, conf: PostgresConnectorEntityConfig) -> PostgresQuery {
        let query = sql::Query::Select(select);
        PostgresQueryCompiler::<PooledClient>::compile_select_query(
            &conf,
            &query,
            query.as_select().unwrap(),
        )
        .unwrap()
    }

    fn compile_insert(insert: sql::Insert, conf: PostgresConnectorEntityConfig) -> PostgresQuery {
        let query = sql::Query::Insert(insert);
        PostgresQueryCompiler::<PooledClient>::compile_insert_query(
            &conf,
            &query,
            query.as_insert().unwrap(),
        )
        .unwrap()
    }

    fn compile_bulk_insert(
        bulk_insert: sql::BulkInsert,
        conf: PostgresConnectorEntityConfig,
    ) -> PostgresQuery {
        let query = sql::Query::BulkInsert(bulk_insert);
        PostgresQueryCompiler::<PooledClient>::compile_bulk_insert_query(
            &conf,
            &query,
            query.as_bulk_insert().unwrap(),
        )
        .unwrap()
    }

    fn compile_update(update: sql::Update, conf: PostgresConnectorEntityConfig) -> PostgresQuery {
        let query = sql::Query::Update(update);
        PostgresQueryCompiler::<PooledClient>::compile_update_query(
            &conf,
            &query,
            query.as_update().unwrap(),
        )
        .unwrap()
    }

    fn compile_delete(delete: sql::Delete, conf: PostgresConnectorEntityConfig) -> PostgresQuery {
        let query = sql::Query::Delete(delete);
        PostgresQueryCompiler::<PooledClient>::compile_delete_query(
            &conf,
            &query,
            query.as_delete().unwrap(),
        )
        .unwrap()
    }

    fn create_entity_config(
        id: &str,
        source: PostgresEntitySourceConfig,
    ) -> EntitySource<PostgresEntitySourceConfig> {
        EntitySource::new(
            EntityConfig::minimal(id, vec![], EntitySourceConfig::minimal("")),
            source,
        )
    }

    fn mock_entity_table() -> PostgresConnectorEntityConfig {
        let mut conf = PostgresConnectorEntityConfig::new();

        conf.add(create_entity_config(
            "entity",
            PostgresEntitySourceConfig::Table(PostgresTableOptions::new(
                None,
                "table".to_string(),
                HashMap::from([("attr1".to_string(), "col1".to_string())]),
            )),
        ));
        conf.add(create_entity_config(
            "other",
            PostgresEntitySourceConfig::Table(PostgresTableOptions::new(
                None,
                "other".to_string(),
                HashMap::from([("otherattr1".to_string(), "othercol1".to_string())]),
            )),
        ));

        conf
    }

    #[test]
    fn test_postgres_compile_select() {
        let mut select = sql::Select::new(sql::source("entity", "entity"));
        select
            .cols
            .push(("COL".to_string(), sql::Expr::attr("entity", "attr1")));
        let compiled = compile_select(select, mock_entity_table());

        assert_eq!(
            compiled,
            PostgresQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" AS "entity""#,
                vec![]
            )
        );
    }

    #[test]
    fn test_postgres_compile_select_where() {
        let mut select = sql::Select::new(sql::source("entity", "entity"));
        select
            .cols
            .push(("COL".to_string(), sql::Expr::attr("entity", "attr1")));
        select.r#where.push(sql::Expr::BinaryOp(sql::BinaryOp::new(
            sql::Expr::attr("entity", "attr1"),
            sql::BinaryOpType::Equal,
            sql::Expr::Parameter(sql::Parameter::new(DataType::Int32, 1)),
        )));
        let compiled = compile_select(select, mock_entity_table());

        assert_eq!(
            compiled,
            PostgresQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" AS "entity" WHERE (("entity"."col1") = ($1))"#,
                vec![QueryParam::Dynamic(sql::Parameter::new(DataType::Int32, 1))]
            )
        );
    }

    #[test]
    fn test_postgres_compile_select_inner_join() {
        let mut select = sql::Select::new(sql::source("entity", "entity"));
        select
            .cols
            .push(("COL".to_string(), sql::Expr::attr("entity", "attr1")));
        select.joins.push(sql::Join::new(
            sql::JoinType::Inner,
            sql::source("other", "other"),
            vec![sql::Expr::BinaryOp(sql::BinaryOp::new(
                sql::Expr::attr("entity", "attr1"),
                sql::BinaryOpType::Equal,
                sql::Expr::attr("other", "otherattr1"),
            ))],
        ));
        let compiled = compile_select(select, mock_entity_table());

        assert_eq!(
            compiled,
            PostgresQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" AS "entity" INNER JOIN "other" AS "other" ON (("entity"."col1") = ("other"."othercol1"))"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_postgres_compile_select_left_join() {
        let mut select = sql::Select::new(sql::source("entity", "entity"));
        select
            .cols
            .push(("COL".to_string(), sql::Expr::attr("entity", "attr1")));
        select.joins.push(sql::Join::new(
            sql::JoinType::Left,
            sql::source("other", "other"),
            vec![sql::Expr::BinaryOp(sql::BinaryOp::new(
                sql::Expr::attr("entity", "attr1"),
                sql::BinaryOpType::Equal,
                sql::Expr::attr("other", "otherattr1"),
            ))],
        ));
        let compiled = compile_select(select, mock_entity_table());

        assert_eq!(
            compiled,
            PostgresQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" AS "entity" LEFT JOIN "other" AS "other" ON (("entity"."col1") = ("other"."othercol1"))"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_postgres_compile_select_right_join() {
        let mut select = sql::Select::new(sql::source("entity", "entity"));
        select
            .cols
            .push(("COL".to_string(), sql::Expr::attr("entity", "attr1")));
        select.joins.push(sql::Join::new(
            sql::JoinType::Right,
            sql::source("other", "other"),
            vec![sql::Expr::BinaryOp(sql::BinaryOp::new(
                sql::Expr::attr("entity", "attr1"),
                sql::BinaryOpType::Equal,
                sql::Expr::attr("other", "otherattr1"),
            ))],
        ));
        let compiled = compile_select(select, mock_entity_table());

        assert_eq!(
            compiled,
            PostgresQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" AS "entity" RIGHT JOIN "other" AS "other" ON (("entity"."col1") = ("other"."othercol1"))"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_postgres_compile_select_group_by() {
        let mut select = sql::Select::new(sql::source("entity", "entity"));
        select
            .cols
            .push(("COL".to_string(), sql::Expr::attr("entity", "attr1")));
        select.group_bys.push(sql::Expr::attr("entity", "attr1"));
        select
            .group_bys
            .push(sql::Expr::Constant(sql::Constant::new(DataValue::Int32(1))));
        let compiled = compile_select(select, mock_entity_table());

        assert_eq!(
            compiled,
            PostgresQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" AS "entity" GROUP BY "entity"."col1", $1"#,
                vec![QueryParam::Constant(DataValue::Int32(1))]
            )
        );
    }

    #[test]
    fn test_postgres_compile_select_order_by() {
        let mut select = sql::Select::new(sql::source("entity", "entity"));
        select
            .cols
            .push(("COL".to_string(), sql::Expr::attr("entity", "attr1")));
        select.order_bys.push(sql::Ordering::new(
            sql::OrderingType::Asc,
            sql::Expr::attr("entity", "attr1"),
        ));
        select.order_bys.push(sql::Ordering::new(
            sql::OrderingType::Desc,
            sql::Expr::Constant(sql::Constant::new(DataValue::Int32(1))),
        ));
        let compiled = compile_select(select, mock_entity_table());

        assert_eq!(
            compiled,
            PostgresQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" AS "entity" ORDER BY "entity"."col1" ASC, $1 DESC"#,
                vec![QueryParam::Constant(DataValue::Int32(1))]
            )
        );
    }

    #[test]
    fn test_postgres_compile_select_row_skip_and_limit() {
        let mut select = sql::Select::new(sql::source("entity", "entity"));
        select
            .cols
            .push(("COL".to_string(), sql::Expr::attr("entity", "attr1")));
        select.row_skip = 10;
        select.row_limit = Some(20);
        let compiled = compile_select(select, mock_entity_table());

        assert_eq!(
            compiled,
            PostgresQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" AS "entity" LIMIT 20 OFFSET 10"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_postgres_compile_select_row_skip() {
        let mut select = sql::Select::new(sql::source("entity", "entity"));
        select
            .cols
            .push(("COL".to_string(), sql::Expr::attr("entity", "attr1")));
        select.row_skip = 10;
        let compiled = compile_select(select, mock_entity_table());

        assert_eq!(
            compiled,
            PostgresQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" AS "entity" OFFSET 10"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_postgres_compile_select_row_limit() {
        let mut select = sql::Select::new(sql::source("entity", "entity"));
        select
            .cols
            .push(("COL".to_string(), sql::Expr::attr("entity", "attr1")));
        select.row_limit = Some(20);
        let compiled = compile_select(select, mock_entity_table());

        assert_eq!(
            compiled,
            PostgresQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" AS "entity" LIMIT 20"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_postgres_compile_select_function_call() {
        let mut select = sql::Select::new(sql::source("entity", "entity"));
        select.cols.push((
            "COL".to_string(),
            sql::Expr::FunctionCall(sql::FunctionCall::Length(Box::new(sql::Expr::attr(
                "entity", "attr1",
            )))),
        ));
        select.row_skip = 10;
        let compiled = compile_select(select, mock_entity_table());

        assert_eq!(
            compiled,
            PostgresQuery::new(
                r#"SELECT length("entity"."col1") AS "COL" FROM "table" AS "entity" OFFSET 10"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_postgres_compile_select_aggregate_call() {
        let mut select = sql::Select::new(sql::source("entity", "entity"));
        select.cols.push((
            "COL".to_string(),
            sql::Expr::AggregateCall(sql::AggregateCall::Sum(Box::new(sql::Expr::attr(
                "entity", "attr1",
            )))),
        ));
        select.row_skip = 10;
        let compiled = compile_select(select, mock_entity_table());

        assert_eq!(
            compiled,
            PostgresQuery::new(
                r#"SELECT sum("entity"."col1") AS "COL" FROM "table" AS "entity" OFFSET 10"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_postgres_compile_select_for_update() {
        let mut select = sql::Select::new(sql::source("entity", "entity"));
        select.cols.push((
            "COL".to_string(),
            sql::Expr::AggregateCall(sql::AggregateCall::Sum(Box::new(sql::Expr::attr(
                "entity", "attr1",
            )))),
        ));
        select.row_lock = sql::SelectRowLockMode::ForUpdate;
        let compiled = compile_select(select, mock_entity_table());

        assert_eq!(
            compiled,
            PostgresQuery::new(
                r#"SELECT sum("entity"."col1") AS "COL" FROM "table" AS "entity" FOR UPDATE"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_postgres_compile_insert_query() {
        let mut insert = sql::Insert::new(sql::source("entity", "entity"));
        insert.cols.push((
            "attr1".to_string(),
            sql::Expr::Parameter(sql::Parameter::new(DataType::Int8, 1)),
        ));

        let compiled = compile_insert(insert, mock_entity_table());

        assert_eq!(
            compiled,
            PostgresQuery::new(
                r#"INSERT INTO "table" ("col1") VALUES ($1)"#,
                vec![QueryParam::Dynamic(sql::Parameter::new(DataType::Int8, 1))]
            )
        );
    }

    #[test]
    fn test_postgres_compile_bulk_insert_query() {
        let mut bulk_insert = sql::BulkInsert::new(sql::source("entity", "entity"));
        bulk_insert.cols.push("attr1".into());
        bulk_insert.values = vec![
            sql::Expr::Parameter(sql::Parameter::new(DataType::Int8, 1)),
            sql::Expr::Parameter(sql::Parameter::new(DataType::Int8, 2)),
            sql::Expr::Parameter(sql::Parameter::new(DataType::Int8, 3)),
        ];

        let compiled = compile_bulk_insert(bulk_insert, mock_entity_table());

        assert_eq!(
            compiled,
            PostgresQuery::new(
                r#"INSERT INTO "table" ("col1") VALUES ($1), ($2), ($3)"#,
                vec![
                    QueryParam::Dynamic(sql::Parameter::new(DataType::Int8, 1)),
                    QueryParam::Dynamic(sql::Parameter::new(DataType::Int8, 2)),
                    QueryParam::Dynamic(sql::Parameter::new(DataType::Int8, 3))
                ]
            )
        );
    }

    #[test]
    fn test_postgres_compile_update_query() {
        let mut update = sql::Update::new(sql::source("entity", "entity"));
        update
            .cols
            .push(("attr1".to_string(), sql::Expr::constant(DataValue::Int8(1))));

        let compiled = compile_update(update, mock_entity_table());

        assert_eq!(
            compiled,
            PostgresQuery::new(
                r#"UPDATE "table" SET "col1" = $1"#,
                vec![QueryParam::Constant(DataValue::Int8(1))]
            )
        );
    }

    #[test]
    fn test_postgres_compile_update_where_query() {
        let mut update = sql::Update::new(sql::source("entity", "entity"));
        update
            .cols
            .push(("attr1".to_string(), sql::Expr::constant(DataValue::Int8(1))));

        update.r#where.push(sql::Expr::BinaryOp(sql::BinaryOp::new(
            sql::Expr::attr("entity", "attr1"),
            sql::BinaryOpType::Equal,
            sql::Expr::Parameter(sql::Parameter::new(DataType::Int32, 1)),
        )));

        let compiled = compile_update(update, mock_entity_table());

        assert_eq!(
            compiled,
            PostgresQuery::new(
                r#"UPDATE "table" SET "col1" = $1 WHERE (("table"."col1") = ($2))"#,
                vec![
                    QueryParam::Constant(DataValue::Int8(1)),
                    QueryParam::Dynamic(sql::Parameter::new(DataType::Int32, 1))
                ]
            )
        );
    }

    #[test]
    fn test_postgres_compile_delete_query() {
        let delete = sql::Delete::new(sql::source("entity", "entity"));
        let compiled = compile_delete(delete, mock_entity_table());

        assert_eq!(
            compiled,
            PostgresQuery::new(r#"DELETE FROM "table""#, vec![])
        );
    }

    #[test]
    fn test_postgres_compile_delete_where_query() {
        let mut delete = sql::Delete::new(sql::source("entity", "entity"));

        delete.r#where.push(sql::Expr::BinaryOp(sql::BinaryOp::new(
            sql::Expr::attr("entity", "attr1"),
            sql::BinaryOpType::Equal,
            sql::Expr::Parameter(sql::Parameter::new(DataType::Int32, 1)),
        )));

        let compiled = compile_delete(delete, mock_entity_table());

        assert_eq!(
            compiled,
            PostgresQuery::new(
                r#"DELETE FROM "table" WHERE (("table"."col1") = ($1))"#,
                vec![QueryParam::Dynamic(sql::Parameter::new(DataType::Int32, 1))]
            )
        );
    }
}

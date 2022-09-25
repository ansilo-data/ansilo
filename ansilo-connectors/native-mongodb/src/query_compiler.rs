use ansilo_core::{
    data::DataValue,
    err::{bail, Context, Result},
    sqlil as sql,
};

use ansilo_connectors_base::{common::query::QueryParam, interface::QueryCompiler};
use mongodb::bson::{doc, Document};

use crate::{to_mongodb_type, FindQuery, MongodbConnection, MongodbQuery, MongodbQueryType};

use super::{MongodbCollectionOptions, MongodbConnectorEntityConfig, MongodbEntitySourceConfig};

/// Query compiler for Mongodb driver
pub struct MongodbQueryCompiler {}

impl QueryCompiler for MongodbQueryCompiler {
    type TConnection = MongodbConnection;
    type TQuery = MongodbQuery;
    type TEntitySourceConfig = MongodbEntitySourceConfig;

    fn compile_query(
        _con: &mut Self::TConnection,
        conf: &MongodbConnectorEntityConfig,
        query: sql::Query,
    ) -> Result<MongodbQuery> {
        match &query {
            sql::Query::Select(select) => Self::compile_select_query(conf, &query, select),
            sql::Query::Insert(insert) => Self::compile_insert_query(conf, &query, insert),
            sql::Query::BulkInsert(insert) => Self::compile_bulk_insert_query(conf, &query, insert),
            sql::Query::Update(update) => Self::compile_update_query(conf, &query, update),
            sql::Query::Delete(delete) => Self::compile_delete_query(conf, &query, delete),
        }
    }
}

impl MongodbQueryCompiler {
    fn compile_select_query(
        conf: &MongodbConnectorEntityConfig,
        query: &sql::Query,
        select: &sql::Select,
    ) -> Result<MongodbQuery> {
        let (db, col) = Self::get_collection(conf, &select.from)?;

        Ok(MongodbQuery::new(
            query,
            params,
            MongodbQueryType::Find(FindQuery {
                filter: if select.r#where.is_empty() {
                    None
                } else {
                    Some({
                        let mut doc = Document::new();
                        doc.insert(
                            "$and",
                            select
                                .r#where
                                .iter()
                                .map(|e| Self::compile_expr(e)?)
                                .collect::<Result<Vec<Bson>>>()?,
                        );
                        doc
                    })
                },
                sort: todo!(),
                skip: todo!(),
                limit: todo!(),
            }),
        ))
    }

    fn compile_insert_query(
        conf: &MongodbConnectorEntityConfig,
        query: &sql::Query,
        insert: &sql::Insert,
    ) -> Result<MongodbQuery> {
        let mut params = Vec::<QueryParam>::new();

        let query = [
            "INSERT INTO".to_string(),
            Self::get_collection(conf, &insert.target, false)?,
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

        Ok(MongodbQuery::new(query, params))
    }

    fn compile_bulk_insert_query(
        conf: &MongodbConnectorEntityConfig,
        query: &sql::Query,
        insert: &sql::BulkInsert,
    ) -> Result<MongodbQuery> {
        let mut params = Vec::<QueryParam>::new();

        let query = [
            "INSERT INTO".to_string(),
            Self::get_collection(conf, &insert.target, false)?,
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

        Ok(MongodbQuery::new(query, params))
    }

    fn compile_update_query(
        conf: &MongodbConnectorEntityConfig,
        query: &sql::Query,
        update: &sql::Update,
    ) -> Result<MongodbQuery> {
        let mut params = Vec::<QueryParam>::new();

        let query = [
            "UPDATE".to_string(),
            Self::get_collection(conf, &update.target, false)?,
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

        Ok(MongodbQuery::new(query, params))
    }

    fn compile_delete_query(
        conf: &MongodbConnectorEntityConfig,
        query: &sql::Query,
        delete: &sql::Delete,
    ) -> Result<MongodbQuery> {
        let mut params = Vec::<QueryParam>::new();

        let query = [
            "DELETE FROM".to_string(),
            Self::get_collection(conf, &delete.target, false)?,
            Self::compile_where(conf, query, &delete.r#where, &mut params)?,
        ]
        .into_iter()
        .filter(|i| !i.is_empty())
        .collect::<Vec<String>>()
        .join(" ");

        Ok(MongodbQuery::new(query, params))
    }

    fn compile_select_cols(
        conf: &MongodbConnectorEntityConfig,
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
        conf: &MongodbConnectorEntityConfig,
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
        conf: &MongodbConnectorEntityConfig,
        query: &sql::Query,
        join: &sql::Join,
        params: &mut Vec<QueryParam>,
    ) -> Result<String> {
        let target = Self::get_collection(conf, &join.target, true)?;
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
        conf: &MongodbConnectorEntityConfig,
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
        conf: &MongodbConnectorEntityConfig,
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
        conf: &MongodbConnectorEntityConfig,
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

    fn compile_expr(
        conf: &MongodbConnectorEntityConfig,
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
        if id.contains("\0") {
            bail!("Invalid identifier: \"{id}\", cannot contain '\\0' chars");
        }

        Ok(ansilo_util_pg::query::pg_quote_identifier(&id))
    }

    pub fn get_collection(
        conf: &MongodbConnectorEntityConfig,
        source: &sql::EntitySource,
    ) -> Result<(String, String)> {
        let entity = conf
            .get(&source.entity)
            .with_context(|| format!("Failed to find entity {:?}", source.entity.clone()))?;

        let col = match &source {
            MongodbEntitySourceConfig::Collection(col) => col,
        };

        Ok((col.database_name.clone(), col.collection_name.clone()))
    }

    fn compile_constant(c: &sql::Constant, params: &mut Vec<QueryParam>) -> Result<String> {
        params.push(QueryParam::Constant(c.value.clone()));
        Ok(format!("?{}", params.len()))
    }

    fn compile_param(p: &sql::Parameter, params: &mut Vec<QueryParam>) -> Result<String> {
        params.push(QueryParam::Dynamic(p.clone()));
        Ok(format!("?{}", params.len()))
    }

    fn compile_unary_op(
        conf: &MongodbConnectorEntityConfig,
        query: &sql::Query,
        op: &sql::UnaryOp,
        params: &mut Vec<QueryParam>,
    ) -> Result<String> {
        let inner = Self::compile_expr(conf, query, &*op.expr, params)?;

        Ok(match op.r#type {
            sql::UnaryOpType::LogicalNot => format!("NOT ({})", inner),
            sql::UnaryOpType::Negate => format!("-({})", inner),
            sql::UnaryOpType::BitwiseNot => format!("~({})", inner),
            sql::UnaryOpType::IsNull => format!("({}) ISNULL", inner),
            sql::UnaryOpType::IsNotNull => format!("({}) NOTNULL", inner),
        })
    }

    fn compile_binary_op(
        conf: &MongodbConnectorEntityConfig,
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
        conf: &MongodbConnectorEntityConfig,
        query: &sql::Query,
        cast: &sql::Cast,
        params: &mut Vec<QueryParam>,
    ) -> Result<String> {
        let arg = Self::compile_expr(conf, query, &cast.expr, params)?;

        Ok(format!(
            "CAST({} AS {})",
            arg,
            to_mongodb_type(&cast.r#type)
        ))
    }

    fn compile_function_call(
        conf: &MongodbConnectorEntityConfig,
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
                "substring({}, {}, {})",
                Self::compile_expr(conf, query, &*call.string, params)?,
                Self::compile_expr(conf, query, &*call.start, params)?,
                Self::compile_expr(conf, query, &*call.len, params)?
            ),
            sql::FunctionCall::Uuid => "lower(
                hex(randomblob(4)) || '-' || hex(randomblob(2)) || '-' || '4' || 
                substr(hex( randomblob(2)), 2) || '-' || 
                substr('AB89', 1 + (abs(random()) % 4) , 1)  ||
                substr(hex(randomblob(2)), 2) || '-' || 
                hex(randomblob(6))
              )"
            .into(),
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
        conf: &MongodbConnectorEntityConfig,
        query: &sql::Query,
        agg: &sql::AggregateCall,
        params: &mut Vec<QueryParam>,
    ) -> Result<String> {
        Ok(match agg {
            sql::AggregateCall::Sum(arg) => {
                format!("SUM({})", Self::compile_expr(conf, query, &*arg, params)?)
            }
            sql::AggregateCall::Count => "COUNT(*)".into(),
            sql::AggregateCall::CountDistinct(arg) => format!(
                "COUNT(DISTINCT {})",
                Self::compile_expr(conf, query, &*arg, params)?
            ),
            sql::AggregateCall::Max(arg) => {
                format!("MAX({})", Self::compile_expr(conf, query, &*arg, params)?)
            }
            sql::AggregateCall::Min(arg) => {
                format!("MIN({})", Self::compile_expr(conf, query, &*arg, params)?)
            }
            sql::AggregateCall::Average(arg) => {
                format!("AVG({})", Self::compile_expr(conf, query, &*arg, params)?)
            }
            sql::AggregateCall::StringAgg(call) => {
                params.push(QueryParam::Constant(DataValue::Utf8String(
                    call.separator.clone(),
                )));
                format!(
                    "GROUP_CONCAT({}, {})",
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

    use super::*;

    fn compile_select(select: sql::Select, conf: MongodbConnectorEntityConfig) -> MongodbQuery {
        let query = sql::Query::Select(select);
        MongodbQueryCompiler::compile_select_query(&conf, &query, query.as_select().unwrap())
            .unwrap()
    }

    fn compile_insert(insert: sql::Insert, conf: MongodbConnectorEntityConfig) -> MongodbQuery {
        let query = sql::Query::Insert(insert);
        MongodbQueryCompiler::compile_insert_query(&conf, &query, query.as_insert().unwrap())
            .unwrap()
    }

    fn compile_bulk_insert(
        bulk_insert: sql::BulkInsert,
        conf: MongodbConnectorEntityConfig,
    ) -> MongodbQuery {
        let query = sql::Query::BulkInsert(bulk_insert);
        MongodbQueryCompiler::compile_bulk_insert_query(
            &conf,
            &query,
            query.as_bulk_insert().unwrap(),
        )
        .unwrap()
    }

    fn compile_update(update: sql::Update, conf: MongodbConnectorEntityConfig) -> MongodbQuery {
        let query = sql::Query::Update(update);
        MongodbQueryCompiler::compile_update_query(&conf, &query, query.as_update().unwrap())
            .unwrap()
    }

    fn compile_delete(delete: sql::Delete, conf: MongodbConnectorEntityConfig) -> MongodbQuery {
        let query = sql::Query::Delete(delete);
        MongodbQueryCompiler::compile_delete_query(&conf, &query, query.as_delete().unwrap())
            .unwrap()
    }

    fn create_entity_config(
        id: &str,
        source: MongodbEntitySourceConfig,
    ) -> EntitySource<MongodbEntitySourceConfig> {
        EntitySource::new(
            EntityConfig::minimal(id, vec![], EntitySourceConfig::minimal("")),
            source,
        )
    }

    fn mock_entity_table() -> MongodbConnectorEntityConfig {
        let mut conf = MongodbConnectorEntityConfig::new();

        conf.add(create_entity_config(
            "entity",
            MongodbEntitySourceConfig::Collection(MongodbCollectionOptions::new(
                "table".to_string(),
                HashMap::from([("attr1".to_string(), "col1".to_string())]),
            )),
        ));
        conf.add(create_entity_config(
            "other",
            MongodbEntitySourceConfig::Collection(MongodbCollectionOptions::new(
                "other".to_string(),
                HashMap::from([("otherattr1".to_string(), "othercol1".to_string())]),
            )),
        ));

        conf
    }

    #[test]
    fn test_mongodb_compile_select() {
        let mut select = sql::Select::new(sql::source("entity", "entity"));
        select
            .cols
            .push(("COL".to_string(), sql::Expr::attr("entity", "attr1")));
        let compiled = compile_select(select, mock_entity_table());

        assert_eq!(
            compiled,
            MongodbQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" AS "entity""#,
                vec![]
            )
        );
    }

    #[test]
    fn test_mongodb_compile_select_where() {
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
            MongodbQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" AS "entity" WHERE (("entity"."col1") = (?1))"#,
                vec![QueryParam::Dynamic(sql::Parameter::new(DataType::Int32, 1))]
            )
        );
    }

    #[test]
    fn test_mongodb_compile_select_inner_join() {
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
            MongodbQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" AS "entity" INNER JOIN "other" AS "other" ON (("entity"."col1") = ("other"."othercol1"))"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_mongodb_compile_select_left_join() {
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
            MongodbQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" AS "entity" LEFT JOIN "other" AS "other" ON (("entity"."col1") = ("other"."othercol1"))"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_mongodb_compile_select_right_join() {
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
            MongodbQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" AS "entity" RIGHT JOIN "other" AS "other" ON (("entity"."col1") = ("other"."othercol1"))"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_mongodb_compile_select_group_by() {
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
            MongodbQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" AS "entity" GROUP BY "entity"."col1", ?1"#,
                vec![QueryParam::Constant(DataValue::Int32(1))]
            )
        );
    }

    #[test]
    fn test_mongodb_compile_select_order_by() {
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
            MongodbQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" AS "entity" ORDER BY "entity"."col1" ASC, ?1 DESC"#,
                vec![QueryParam::Constant(DataValue::Int32(1))]
            )
        );
    }

    #[test]
    fn test_mongodb_compile_select_row_skip_and_limit() {
        let mut select = sql::Select::new(sql::source("entity", "entity"));
        select
            .cols
            .push(("COL".to_string(), sql::Expr::attr("entity", "attr1")));
        select.row_skip = 10;
        select.row_limit = Some(20);
        let compiled = compile_select(select, mock_entity_table());

        assert_eq!(
            compiled,
            MongodbQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" AS "entity" LIMIT 20 OFFSET 10"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_mongodb_compile_select_row_skip() {
        let mut select = sql::Select::new(sql::source("entity", "entity"));
        select
            .cols
            .push(("COL".to_string(), sql::Expr::attr("entity", "attr1")));
        select.row_skip = 10;
        let compiled = compile_select(select, mock_entity_table());

        assert_eq!(
            compiled,
            MongodbQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" AS "entity" OFFSET 10"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_mongodb_compile_select_row_limit() {
        let mut select = sql::Select::new(sql::source("entity", "entity"));
        select
            .cols
            .push(("COL".to_string(), sql::Expr::attr("entity", "attr1")));
        select.row_limit = Some(20);
        let compiled = compile_select(select, mock_entity_table());

        assert_eq!(
            compiled,
            MongodbQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" AS "entity" LIMIT 20"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_mongodb_compile_select_function_call() {
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
            MongodbQuery::new(
                r#"SELECT length("entity"."col1") AS "COL" FROM "table" AS "entity" OFFSET 10"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_mongodb_compile_select_aggregate_call() {
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
            MongodbQuery::new(
                r#"SELECT SUM("entity"."col1") AS "COL" FROM "table" AS "entity" OFFSET 10"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_mongodb_compile_insert_query() {
        let mut insert = sql::Insert::new(sql::source("entity", "entity"));
        insert.cols.push((
            "attr1".to_string(),
            sql::Expr::Parameter(sql::Parameter::new(DataType::Int8, 1)),
        ));

        let compiled = compile_insert(insert, mock_entity_table());

        assert_eq!(
            compiled,
            MongodbQuery::new(
                r#"INSERT INTO "table" ("col1") VALUES (?1)"#,
                vec![QueryParam::Dynamic(sql::Parameter::new(DataType::Int8, 1))]
            )
        );
    }

    #[test]
    fn test_mongodb_compile_bulk_insert_query() {
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
            MongodbQuery::new(
                r#"INSERT INTO "table" ("col1") VALUES (?1), (?2), (?3)"#,
                vec![
                    QueryParam::Dynamic(sql::Parameter::new(DataType::Int8, 1)),
                    QueryParam::Dynamic(sql::Parameter::new(DataType::Int8, 2)),
                    QueryParam::Dynamic(sql::Parameter::new(DataType::Int8, 3))
                ]
            )
        );
    }

    #[test]
    fn test_mongodb_compile_update_query() {
        let mut update = sql::Update::new(sql::source("entity", "entity"));
        update
            .cols
            .push(("attr1".to_string(), sql::Expr::constant(DataValue::Int8(1))));

        let compiled = compile_update(update, mock_entity_table());

        assert_eq!(
            compiled,
            MongodbQuery::new(
                r#"UPDATE "table" SET "col1" = ?1"#,
                vec![QueryParam::Constant(DataValue::Int8(1))]
            )
        );
    }

    #[test]
    fn test_mongodb_compile_update_where_query() {
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
            MongodbQuery::new(
                r#"UPDATE "table" SET "col1" = ?1 WHERE (("table"."col1") = (?2))"#,
                vec![
                    QueryParam::Constant(DataValue::Int8(1)),
                    QueryParam::Dynamic(sql::Parameter::new(DataType::Int32, 1))
                ]
            )
        );
    }

    #[test]
    fn test_mongodb_compile_delete_query() {
        let delete = sql::Delete::new(sql::source("entity", "entity"));
        let compiled = compile_delete(delete, mock_entity_table());

        assert_eq!(
            compiled,
            MongodbQuery::new(r#"DELETE FROM "table""#, vec![])
        );
    }

    #[test]
    fn test_mongodb_compile_delete_where_query() {
        let mut delete = sql::Delete::new(sql::source("entity", "entity"));

        delete.r#where.push(sql::Expr::BinaryOp(sql::BinaryOp::new(
            sql::Expr::attr("entity", "attr1"),
            sql::BinaryOpType::Equal,
            sql::Expr::Parameter(sql::Parameter::new(DataType::Int32, 1)),
        )));

        let compiled = compile_delete(delete, mock_entity_table());

        assert_eq!(
            compiled,
            MongodbQuery::new(
                r#"DELETE FROM "table" WHERE (("table"."col1") = (?1))"#,
                vec![QueryParam::Dynamic(sql::Parameter::new(DataType::Int32, 1))]
            )
        );
    }
}

use ansilo_core::{
    err::{bail, Context, Result},
    sqlil as sql,
};

use crate::{
    interface::QueryCompiler,
    jdbc::{JdbcConnection, JdbcQuery, JdbcQueryParam},
};

use super::{
    OracleJdbcConnectorEntityConfig, OracleJdbcEntitySourceConfig, OracleJdbcTableOptions,
};

/// Query compiler for Oracle JDBC driver
pub struct OracleJdbcQueryCompiler;

impl QueryCompiler for OracleJdbcQueryCompiler {
    type TConnection = JdbcConnection;
    type TQuery = JdbcQuery;
    type TEntitySourceConfig = OracleJdbcEntitySourceConfig;

    fn compile_query(
        _con: &mut JdbcConnection,
        conf: &OracleJdbcConnectorEntityConfig,
        query: sql::Query,
    ) -> Result<JdbcQuery> {
        match &query {
            sql::Query::Select(select) => Self::compile_select_query(conf, &query, select),
            sql::Query::Insert(insert) => Self::compile_insert_query(conf, &query, insert),
            sql::Query::Update(update) => Self::compile_update_query(conf, &query, update),
            sql::Query::Delete(delete) => Self::compile_delete_query(conf, &query, delete),
        }
    }
}

impl OracleJdbcQueryCompiler {
    fn compile_select_query(
        conf: &OracleJdbcConnectorEntityConfig,
        query: &sql::Query,
        select: &sql::Select,
    ) -> Result<JdbcQuery> {
        let mut params = Vec::<JdbcQueryParam>::new();

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

        Ok(JdbcQuery::new(query, params))
    }

    fn compile_insert_query(
        conf: &OracleJdbcConnectorEntityConfig,
        query: &sql::Query,
        insert: &sql::Insert,
    ) -> Result<JdbcQuery> {
        // TODO: custom query support
        let mut params = Vec::<JdbcQueryParam>::new();

        let query = [
            "INSERT INTO".to_string(),
            Self::compile_entity_source(conf, &insert.target, false)?,
            format!(
                "({}) VALUES ({})",
                insert
                    .cols
                    .iter()
                    .map(|(col, _)| Self::compile_attribute_identifier(
                        conf,
                        query,
                        &sql::AttributeIdentifier::new(&insert.target.alias, col),
                        false
                    ))
                    .collect::<Result<Vec<_>>>()?
                    .join(", "),
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

        Ok(JdbcQuery::new(query, params))
    }

    fn compile_update_query(
        conf: &OracleJdbcConnectorEntityConfig,
        query: &sql::Query,
        update: &sql::Update,
    ) -> Result<JdbcQuery> {
        // TODO: custom query support
        let mut params = Vec::<JdbcQueryParam>::new();

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
                            &sql::AttributeIdentifier::new(&update.target.alias, col),
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

        Ok(JdbcQuery::new(query, params))
    }

    fn compile_delete_query(
        conf: &OracleJdbcConnectorEntityConfig,
        query: &sql::Query,
        delete: &sql::Delete,
    ) -> Result<JdbcQuery> {
        // TODO: custom query support
        let mut params = Vec::<JdbcQueryParam>::new();

        let query = [
            "DELETE FROM".to_string(),
            Self::compile_entity_source(conf, &delete.target, false)?,
            Self::compile_where(conf, query, &delete.r#where, &mut params)?,
        ]
        .into_iter()
        .filter(|i| !i.is_empty())
        .collect::<Vec<String>>()
        .join(" ");

        Ok(JdbcQuery::new(query, params))
    }

    fn compile_select_cols(
        conf: &OracleJdbcConnectorEntityConfig,
        query: &sql::Query,
        cols: &Vec<(String, sql::Expr)>,
        params: &mut Vec<JdbcQueryParam>,
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
        conf: &OracleJdbcConnectorEntityConfig,
        query: &sql::Query,
        joins: &Vec<sql::Join>,
        params: &mut Vec<JdbcQueryParam>,
    ) -> Result<String> {
        Ok(joins
            .into_iter()
            .map(|j| Ok(Self::compile_select_join(conf, query, j, params)?))
            .collect::<Result<Vec<String>>>()?
            .join(", "))
    }

    fn compile_select_join(
        conf: &OracleJdbcConnectorEntityConfig,
        query: &sql::Query,
        join: &sql::Join,
        params: &mut Vec<JdbcQueryParam>,
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
            sql::JoinType::Left => todo!(),
            sql::JoinType::Right => todo!(),
            sql::JoinType::Full => todo!(),
        })
    }

    fn compile_where(
        conf: &OracleJdbcConnectorEntityConfig,
        query: &sql::Query,
        r#where: &Vec<sql::Expr>,
        params: &mut Vec<JdbcQueryParam>,
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
        conf: &OracleJdbcConnectorEntityConfig,
        query: &sql::Query,
        group_bys: &Vec<sql::Expr>,
        params: &mut Vec<JdbcQueryParam>,
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
        conf: &OracleJdbcConnectorEntityConfig,
        query: &sql::Query,
        order_bys: &Vec<sql::Ordering>,
        params: &mut Vec<JdbcQueryParam>,
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

        if row_skip > 0 {
            parts.push(format!("OFFSET {} ROWS", row_skip));
        }

        if let Some(lim) = row_limit {
            parts.push(format!("FETCH FIRST {} ROWS ONLY", lim));
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
        conf: &OracleJdbcConnectorEntityConfig,
        query: &sql::Query,
        expr: &sql::Expr,
        params: &mut Vec<JdbcQueryParam>,
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
        // @see https://docs.oracle.com/cd/B19306_01/server.102/b14200/sql_elements008.htm
        if id.contains('"') || id.contains("\0") {
            bail!("Invalid identifier: \"{id}\", cannot contain '\"' or '\\0' chars");
        }

        Ok(format!("\"{}\"", id))
    }

    pub fn compile_entity_source(
        conf: &OracleJdbcConnectorEntityConfig,
        source: &sql::EntitySource,
        include_alias: bool,
    ) -> Result<String> {
        let entity = conf
            .find(&source.entity)
            .with_context(|| format!("Failed to find entity {:?}", source.entity.clone()))?;

        let id = Self::compile_source_identifier(&entity.source_conf)?;

        Ok(if include_alias {
            let alias = Self::compile_identifier(source.alias.clone())?;

            format!("{id} AS {alias}")
        } else {
            id
        })
    }

    pub fn compile_source_identifier(source: &OracleJdbcEntitySourceConfig) -> Result<String> {
        // TODO: custom query
        Ok(match &source {
            OracleJdbcEntitySourceConfig::Table(OracleJdbcTableOptions {
                database_name: Some(db),
                table_name: table,
                ..
            }) => format!(
                "{}.{}",
                Self::compile_identifier(db.clone())?,
                Self::compile_identifier(table.clone())?
            ),
            OracleJdbcEntitySourceConfig::Table(OracleJdbcTableOptions {
                database_name: None,
                table_name: table,
                ..
            }) => Self::compile_identifier(table.clone())?,
            OracleJdbcEntitySourceConfig::CustomQueries(_) => todo!(),
        })
    }

    fn compile_attribute_identifier(
        conf: &OracleJdbcConnectorEntityConfig,
        query: &sql::Query,
        eva: &sql::AttributeIdentifier,
        include_table: bool,
    ) -> Result<String> {
        let source = query.get_entity_source(&eva.entity_alias)?;
        let entity = conf
            .find(&source.entity)
            .with_context(|| format!("Failed to find entity {:?}", source.entity.clone()))?;

        // TODO: custom query
        let table = match &entity.source_conf {
            OracleJdbcEntitySourceConfig::Table(table) => table,
            OracleJdbcEntitySourceConfig::CustomQueries(_) => todo!(),
        };

        let column = table
            .attribute_column_map
            .get(&eva.attribute_id)
            .with_context(|| {
                format!(
                    "Unknown attribute {} on entity {:?}",
                    eva.attribute_id,
                    source.entity.clone()
                )
            })?;

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

    fn compile_constant(c: &sql::Constant, params: &mut Vec<JdbcQueryParam>) -> Result<String> {
        params.push(JdbcQueryParam::Constant(c.value.clone()));
        Ok("?".to_string())
    }

    fn compile_param(p: &sql::Parameter, params: &mut Vec<JdbcQueryParam>) -> Result<String> {
        params.push(JdbcQueryParam::Dynamic(p.id, p.r#type.clone()));
        Ok("?".to_string())
    }

    fn compile_unary_op(
        conf: &OracleJdbcConnectorEntityConfig,
        query: &sql::Query,
        op: &sql::UnaryOp,
        params: &mut Vec<JdbcQueryParam>,
    ) -> Result<String> {
        let inner = Self::compile_expr(conf, query, &*op.expr, params)?;

        Ok(match op.r#type {
            sql::UnaryOpType::LogicalNot => format!("!({})", inner),
            sql::UnaryOpType::Negate => format!("!({})", inner),
            sql::UnaryOpType::BitwiseNot => format!("UTL_RAW.BIT_COMPLEMENT({})", inner),
            sql::UnaryOpType::IsNull => format!("({}) IS NULL", inner),
            sql::UnaryOpType::IsNotNull => format!("({}) IS NOT NULL", inner),
        })
    }

    fn compile_binary_op(
        conf: &OracleJdbcConnectorEntityConfig,
        query: &sql::Query,
        op: &sql::BinaryOp,
        params: &mut Vec<JdbcQueryParam>,
    ) -> Result<String> {
        let l = Self::compile_expr(conf, query, &*op.left, params)?;
        let r = Self::compile_expr(conf, query, &*op.right, params)?;

        // TODO
        Ok(match op.r#type {
            sql::BinaryOpType::Add => format!("({}) + ({})", l, r),
            sql::BinaryOpType::Subtract => format!("({}) - ({})", l, r),
            sql::BinaryOpType::Multiply => format!("({}) * ({})", l, r),
            sql::BinaryOpType::Divide => format!("({}) / ({})", l, r),
            sql::BinaryOpType::LogicalAnd => format!("({}) AND ({})", l, r),
            sql::BinaryOpType::LogicalOr => format!("({}) OR ({})", l, r),
            sql::BinaryOpType::Modulo => todo!(),
            sql::BinaryOpType::Exponent => todo!(),
            sql::BinaryOpType::BitwiseAnd => todo!(),
            sql::BinaryOpType::BitwiseOr => todo!(),
            sql::BinaryOpType::BitwiseXor => todo!(),
            sql::BinaryOpType::BitwiseShiftLeft => todo!(),
            sql::BinaryOpType::BitwiseShiftRight => todo!(),
            sql::BinaryOpType::Concat => todo!(),
            sql::BinaryOpType::Regexp => todo!(),
            sql::BinaryOpType::In => todo!(),
            sql::BinaryOpType::NotIn => todo!(),
            sql::BinaryOpType::Equal => format!("({}) = ({})", l, r),
            sql::BinaryOpType::NullSafeEqual => todo!(),
            sql::BinaryOpType::NotEqual => todo!(),
            sql::BinaryOpType::GreaterThan => todo!(),
            sql::BinaryOpType::GreaterThanOrEqual => todo!(),
            sql::BinaryOpType::LessThan => todo!(),
            sql::BinaryOpType::LessThanOrEqual => todo!(),
        })
    }

    fn compile_cast(
        conf: &OracleJdbcConnectorEntityConfig,
        query: &sql::Query,
        cast: &sql::Cast,
        params: &mut Vec<JdbcQueryParam>,
    ) -> Result<String> {
        todo!()
    }

    fn compile_function_call(
        conf: &OracleJdbcConnectorEntityConfig,
        query: &sql::Query,
        func: &sql::FunctionCall,
        params: &mut Vec<JdbcQueryParam>,
    ) -> Result<String> {
        Ok(match func {
            sql::FunctionCall::Length(arg) => {
                format!(
                    "LENGTH({})",
                    Self::compile_expr(conf, query, &*arg, params)?
                )
            }
            sql::FunctionCall::Abs(_) => todo!(),
            sql::FunctionCall::Uppercase(_) => todo!(),
            sql::FunctionCall::Lowercase(_) => todo!(),
            sql::FunctionCall::Substring(_) => todo!(),
            sql::FunctionCall::Uuid => todo!(),
            sql::FunctionCall::Coalesce(_) => todo!(),
        })
    }

    fn compile_aggregate_call(
        conf: &OracleJdbcConnectorEntityConfig,
        query: &sql::Query,
        agg: &sql::AggregateCall,
        params: &mut Vec<JdbcQueryParam>,
    ) -> Result<String> {
        Ok(match agg {
            sql::AggregateCall::Sum(arg) => {
                format!("SUM({})", Self::compile_expr(conf, query, &*arg, params)?)
            }
            sql::AggregateCall::Count => todo!(),
            sql::AggregateCall::CountDistinct(_) => todo!(),
            sql::AggregateCall::Max(_) => todo!(),
            sql::AggregateCall::Min(_) => todo!(),
            sql::AggregateCall::StringAgg(_) => todo!(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use ansilo_core::{
        config::{EntitySourceConfig, EntityVersionConfig},
        data::{DataType, DataValue},
    };

    use crate::common::entity::EntitySource;

    use super::*;

    fn compile_select(select: sql::Select, conf: OracleJdbcConnectorEntityConfig) -> JdbcQuery {
        let query = sql::Query::Select(select);
        OracleJdbcQueryCompiler::compile_select_query(&conf, &query, query.as_select().unwrap())
            .unwrap()
    }

    fn compile_insert(insert: sql::Insert, conf: OracleJdbcConnectorEntityConfig) -> JdbcQuery {
        let query = sql::Query::Insert(insert);
        OracleJdbcQueryCompiler::compile_insert_query(&conf, &query, query.as_insert().unwrap())
            .unwrap()
    }

    fn compile_update(update: sql::Update, conf: OracleJdbcConnectorEntityConfig) -> JdbcQuery {
        let query = sql::Query::Update(update);
        OracleJdbcQueryCompiler::compile_update_query(&conf, &query, query.as_update().unwrap())
            .unwrap()
    }

    fn compile_delete(delete: sql::Delete, conf: OracleJdbcConnectorEntityConfig) -> JdbcQuery {
        let query = sql::Query::Delete(delete);
        OracleJdbcQueryCompiler::compile_delete_query(&conf, &query, query.as_delete().unwrap())
            .unwrap()
    }

    fn create_entity_config(
        id: &str,
        version: &str,
        source: OracleJdbcEntitySourceConfig,
    ) -> EntitySource<OracleJdbcEntitySourceConfig> {
        EntitySource::minimal(
            id,
            EntityVersionConfig::minimal(
                version.to_string(),
                vec![],
                EntitySourceConfig::minimal(""),
            ),
            source,
        )
    }

    fn mock_entity_table() -> OracleJdbcConnectorEntityConfig {
        let mut conf = OracleJdbcConnectorEntityConfig::new();

        conf.add(create_entity_config(
            "entity",
            "v1",
            OracleJdbcEntitySourceConfig::Table(OracleJdbcTableOptions::new(
                None,
                "table".to_string(),
                HashMap::from([("attr1".to_string(), "col1".to_string())]),
            )),
        ));
        conf.add(create_entity_config(
            "other",
            "v1",
            OracleJdbcEntitySourceConfig::Table(OracleJdbcTableOptions::new(
                None,
                "other".to_string(),
                HashMap::from([("otherattr1".to_string(), "othercol1".to_string())]),
            )),
        ));

        conf
    }

    #[test]
    fn test_oracle_jdbc_compile_select() {
        let mut select = sql::Select::new(sql::source("entity", "v1", "entity"));
        select
            .cols
            .push(("COL".to_string(), sql::Expr::attr("entity", "attr1")));
        let compiled = compile_select(select, mock_entity_table());

        assert_eq!(
            compiled,
            JdbcQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" AS "entity""#,
                vec![]
            )
        );
    }

    #[test]
    fn test_oracle_jdbc_compile_select_where() {
        let mut select = sql::Select::new(sql::source("entity", "v1", "entity"));
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
            JdbcQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" AS "entity" WHERE (("entity"."col1") = (?))"#,
                vec![JdbcQueryParam::Dynamic(1, DataType::Int32)]
            )
        );
    }

    #[test]
    fn test_oracle_jdbc_compile_select_join() {
        let mut select = sql::Select::new(sql::source("entity", "v1", "entity"));
        select
            .cols
            .push(("COL".to_string(), sql::Expr::attr("entity", "attr1")));
        select.joins.push(sql::Join::new(
            sql::JoinType::Inner,
            sql::source("other", "v1", "other"),
            vec![sql::Expr::BinaryOp(sql::BinaryOp::new(
                sql::Expr::attr("entity", "attr1"),
                sql::BinaryOpType::Equal,
                sql::Expr::attr("other", "otherattr1"),
            ))],
        ));
        let compiled = compile_select(select, mock_entity_table());

        assert_eq!(
            compiled,
            JdbcQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" AS "entity" INNER JOIN "other" AS "other" ON (("entity"."col1") = ("other"."othercol1"))"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_oracle_jdbc_compile_select_group_by() {
        let mut select = sql::Select::new(sql::source("entity", "v1", "entity"));
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
            JdbcQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" AS "entity" GROUP BY "entity"."col1", ?"#,
                vec![JdbcQueryParam::Constant(DataValue::Int32(1))]
            )
        );
    }

    #[test]
    fn test_oracle_jdbc_compile_select_order_by() {
        let mut select = sql::Select::new(sql::source("entity", "v1", "entity"));
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
            JdbcQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" AS "entity" ORDER BY "entity"."col1" ASC, ? DESC"#,
                vec![JdbcQueryParam::Constant(DataValue::Int32(1))]
            )
        );
    }

    #[test]
    fn test_oracle_jdbc_compile_select_row_skip_and_limit() {
        let mut select = sql::Select::new(sql::source("entity", "v1", "entity"));
        select
            .cols
            .push(("COL".to_string(), sql::Expr::attr("entity", "attr1")));
        select.row_skip = 10;
        select.row_limit = Some(20);
        let compiled = compile_select(select, mock_entity_table());

        assert_eq!(
            compiled,
            JdbcQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" AS "entity" OFFSET 10 ROWS FETCH FIRST 20 ROWS ONLY"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_oracle_jdbc_compile_select_row_skip() {
        let mut select = sql::Select::new(sql::source("entity", "v1", "entity"));
        select
            .cols
            .push(("COL".to_string(), sql::Expr::attr("entity", "attr1")));
        select.row_skip = 10;
        let compiled = compile_select(select, mock_entity_table());

        assert_eq!(
            compiled,
            JdbcQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" AS "entity" OFFSET 10 ROWS"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_oracle_jdbc_compile_select_row_limit() {
        let mut select = sql::Select::new(sql::source("entity", "v1", "entity"));
        select
            .cols
            .push(("COL".to_string(), sql::Expr::attr("entity", "attr1")));
        select.row_limit = Some(20);
        let compiled = compile_select(select, mock_entity_table());

        assert_eq!(
            compiled,
            JdbcQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" AS "entity" FETCH FIRST 20 ROWS ONLY"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_oracle_jdbc_compile_select_function_call() {
        let mut select = sql::Select::new(sql::source("entity", "v1", "entity"));
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
            JdbcQuery::new(
                r#"SELECT LENGTH("entity"."col1") AS "COL" FROM "table" AS "entity" OFFSET 10 ROWS"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_oracle_jdbc_compile_select_aggregate_call() {
        let mut select = sql::Select::new(sql::source("entity", "v1", "entity"));
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
            JdbcQuery::new(
                r#"SELECT SUM("entity"."col1") AS "COL" FROM "table" AS "entity" OFFSET 10 ROWS"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_oracle_jdbc_compile_select_for_update() {
        let mut select = sql::Select::new(sql::source("entity", "v1", "entity"));
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
            JdbcQuery::new(
                r#"SELECT SUM("entity"."col1") AS "COL" FROM "table" AS "entity" FOR UPDATE"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_oracle_jdbc_compile_insert_query() {
        let mut insert = sql::Insert::new(sql::source("entity", "v1", "entity"));
        insert.cols.push((
            "attr1".to_string(),
            sql::Expr::Parameter(sql::Parameter::new(DataType::Int8, 1)),
        ));

        let compiled = compile_insert(insert, mock_entity_table());

        assert_eq!(
            compiled,
            JdbcQuery::new(
                r#"INSERT INTO "table" ("col1") VALUES (?)"#,
                vec![JdbcQueryParam::Dynamic(1, DataType::Int8)]
            )
        );
    }

    #[test]
    fn test_oracle_jdbc_compile_update_query() {
        let mut update = sql::Update::new(sql::source("entity", "v1", "entity"));
        update
            .cols
            .push(("attr1".to_string(), sql::Expr::constant(DataValue::Int8(1))));

        let compiled = compile_update(update, mock_entity_table());

        assert_eq!(
            compiled,
            JdbcQuery::new(
                r#"UPDATE "table" SET "col1" = ?"#,
                vec![JdbcQueryParam::Constant(DataValue::Int8(1))]
            )
        );
    }

    #[test]
    fn test_oracle_jdbc_compile_update_where_query() {
        let mut update = sql::Update::new(sql::source("entity", "v1", "entity"));
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
            JdbcQuery::new(
                r#"UPDATE "table" SET "col1" = ? WHERE (("table"."col1") = (?))"#,
                vec![
                    JdbcQueryParam::Constant(DataValue::Int8(1)),
                    JdbcQueryParam::Dynamic(1, DataType::Int32)
                ]
            )
        );
    }

    #[test]
    fn test_oracle_jdbc_compile_delete_query() {
        let delete = sql::Delete::new(sql::source("entity", "v1", "entity"));
        let compiled = compile_delete(delete, mock_entity_table());

        assert_eq!(compiled, JdbcQuery::new(r#"DELETE FROM "table""#, vec![]));
    }

    #[test]
    fn test_oracle_jdbc_compile_delete_where_query() {
        let mut delete = sql::Delete::new(sql::source("entity", "v1", "entity"));

        delete.r#where.push(sql::Expr::BinaryOp(sql::BinaryOp::new(
            sql::Expr::attr("entity", "attr1"),
            sql::BinaryOpType::Equal,
            sql::Expr::Parameter(sql::Parameter::new(DataType::Int32, 1)),
        )));

        let compiled = compile_delete(delete, mock_entity_table());

        assert_eq!(
            compiled,
            JdbcQuery::new(
                r#"DELETE FROM "table" WHERE (("table"."col1") = (?))"#,
                vec![JdbcQueryParam::Dynamic(1, DataType::Int32)]
            )
        );
    }
}

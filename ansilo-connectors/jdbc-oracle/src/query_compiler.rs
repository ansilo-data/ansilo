use ansilo_core::{
    data::{DataType, DataValue},
    err::{bail, Context, Result},
    sqlil as sql,
};

use ansilo_connectors_base::interface::QueryCompiler;
use ansilo_connectors_jdbc_base::{JdbcConnection, JdbcQuery, JdbcQueryParam};

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
        _con: &mut Self::TConnection,
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
                        &sql::AttributeId::new(&insert.target.alias, col),
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
            sql::JoinType::Left => format!("LEFT JOIN {} ON {}", target, cond),
            sql::JoinType::Right => format!("RIGHT JOIN {} ON {}", target, cond),
            sql::JoinType::Full => format!("FULL JOIN {} ON {}", target, cond),
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
            .get(&source.entity)
            .with_context(|| format!("Failed to find entity {:?}", source.entity.clone()))?;

        let id = Self::compile_source_identifier(&entity.source)?;

        Ok(if include_alias {
            let alias = Self::compile_identifier(source.alias.clone())?;

            format!("{id} {alias}")
        } else {
            id
        })
    }

    pub fn compile_source_identifier(source: &OracleJdbcEntitySourceConfig) -> Result<String> {
        // TODO: custom query
        Ok(match &source {
            OracleJdbcEntitySourceConfig::Table(OracleJdbcTableOptions {
                owner_name: Some(db),
                table_name: table,
                ..
            }) => format!(
                "{}.{}",
                Self::compile_identifier(db.clone())?,
                Self::compile_identifier(table.clone())?
            ),
            OracleJdbcEntitySourceConfig::Table(OracleJdbcTableOptions {
                owner_name: None,
                table_name: table,
                ..
            }) => Self::compile_identifier(table.clone())?,
            OracleJdbcEntitySourceConfig::CustomQueries(_) => todo!(),
        })
    }

    fn compile_attribute_identifier(
        conf: &OracleJdbcConnectorEntityConfig,
        query: &sql::Query,
        eva: &sql::AttributeId,
        include_table: bool,
    ) -> Result<String> {
        let source = query.get_entity_source(&eva.entity_alias)?;
        let entity = conf
            .get(&source.entity)
            .with_context(|| format!("Failed to find entity {:?}", source.entity.clone()))?;

        // TODO: custom query
        let table = match &entity.source {
            OracleJdbcEntitySourceConfig::Table(table) => table,
            OracleJdbcEntitySourceConfig::CustomQueries(_) => todo!(),
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

        Ok(match op.r#type {
            sql::BinaryOpType::Add => format!("({}) + ({})", l, r),
            sql::BinaryOpType::Subtract => format!("({}) - ({})", l, r),
            sql::BinaryOpType::Multiply => format!("({}) * ({})", l, r),
            sql::BinaryOpType::Divide => format!("({}) / ({})", l, r),
            sql::BinaryOpType::LogicalAnd => format!("({}) AND ({})", l, r),
            sql::BinaryOpType::LogicalOr => format!("({}) OR ({})", l, r),
            sql::BinaryOpType::Modulo => format!("MOD({}, {})", l, r),
            sql::BinaryOpType::Exponent => format!("POWER({}, {})", l, r),
            sql::BinaryOpType::BitwiseAnd => format!("UTL_RAW.BIT_AND({}, {})", l, r),
            sql::BinaryOpType::BitwiseOr => format!("UTL_RAW.BIT_OR({}, {})", l, r),
            sql::BinaryOpType::BitwiseXor => format!("UTL_RAW.BIT_XOR({}, {})", l, r),
            sql::BinaryOpType::BitwiseShiftLeft => unimplemented!(),
            sql::BinaryOpType::BitwiseShiftRight => unimplemented!(),
            sql::BinaryOpType::Concat => format!("({}) || ({})", l, r),
            sql::BinaryOpType::Regexp => format!("REGEXP_LIKE({}, {})", l, r),
            sql::BinaryOpType::Equal => format!("({}) = ({})", l, r),
            sql::BinaryOpType::NullSafeEqual => {
                format!("SYS_OP_MAP_NONNULL({}) = SYS_OP_MAP_NONNULL({})", l, r)
            }
            sql::BinaryOpType::NotEqual => format!("({}) != ({})", l, r),
            sql::BinaryOpType::GreaterThan => format!("({}) > ({})", l, r),
            sql::BinaryOpType::GreaterThanOrEqual => format!("({}) >= ({})", l, r),
            sql::BinaryOpType::LessThan => format!("({}) < ({})", l, r),
            sql::BinaryOpType::LessThanOrEqual => format!("({}) <= ({})", l, r),
        })
    }

    fn compile_cast(
        conf: &OracleJdbcConnectorEntityConfig,
        query: &sql::Query,
        cast: &sql::Cast,
        params: &mut Vec<JdbcQueryParam>,
    ) -> Result<String> {
        let arg = Self::compile_expr(conf, query, &cast.expr, params)?;

        Ok(match &cast.r#type {
            DataType::Utf8String(_) => format!("TO_NCHAR({})", arg),
            DataType::Binary => format!("UTL_RAW.CAST_TO_RAW({})", arg),
            DataType::Boolean => format!("CASE WHEN ({}) THEN TRUE ELSE FALSE END", arg),
            DataType::Int8
            | DataType::UInt8
            | DataType::Int16
            | DataType::UInt16
            | DataType::Int32
            | DataType::UInt32
            | DataType::Int64
            | DataType::UInt64
            | DataType::Decimal(_) => format!("TO_NUMBER({})", arg),
            DataType::Float32 => format!("TO_BINARY_FLOAT({})", arg),
            DataType::Float64 => format!("TO_BINARY_DOUBLE({})", arg),
            DataType::JSON => format!("JSON_SERIALIZE({})", arg),
            DataType::Date => format!("TO_DATE({})", arg),
            DataType::DateTime => format!("TO_TIMESTAMP({})", arg),
            DataType::DateTimeWithTZ => format!("TO_TIMESTAMP_TZ({})", arg),
            DataType::Null => format!("CASE WHEN ({}) THEN NULL ELSE NULL END", arg),
            DataType::Uuid => unimplemented!(),
            DataType::Time => unimplemented!(),
        })
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
            sql::FunctionCall::Abs(arg) => {
                format!("ABS({})", Self::compile_expr(conf, query, &*arg, params)?)
            }
            sql::FunctionCall::Uppercase(arg) => {
                format!("UPPER({})", Self::compile_expr(conf, query, &*arg, params)?)
            }
            sql::FunctionCall::Lowercase(arg) => {
                format!("LOWER({})", Self::compile_expr(conf, query, &*arg, params)?)
            }
            sql::FunctionCall::Substring(call) => format!(
                "SUBSTR({}, {}, {})",
                Self::compile_expr(conf, query, &*call.string, params)?,
                Self::compile_expr(conf, query, &*call.start, params)?,
                Self::compile_expr(conf, query, &*call.len, params)?
            ),
            sql::FunctionCall::Uuid => "SYS_GUID()".into(),
            sql::FunctionCall::Coalesce(args) => format!(
                "COALECSE({})",
                args.iter()
                    .map(|arg| Self::compile_expr(conf, query, &**arg, params))
                    .collect::<Result<Vec<_>>>()?
                    .join(", ")
            ),
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
                params.push(JdbcQueryParam::Constant(DataValue::Utf8String(
                    call.separator.clone(),
                )));
                format!(
                    "LISTAGG({}, ?) WITHIN GROUP (ORDER BY NULL)",
                    Self::compile_expr(conf, query, &call.expr, params)?,
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
        source: OracleJdbcEntitySourceConfig,
    ) -> EntitySource<OracleJdbcEntitySourceConfig> {
        EntitySource::new(
            EntityConfig::minimal(id, vec![], EntitySourceConfig::minimal("")),
            source,
        )
    }

    fn mock_entity_table() -> OracleJdbcConnectorEntityConfig {
        let mut conf = OracleJdbcConnectorEntityConfig::new();

        conf.add(create_entity_config(
            "entity",
            OracleJdbcEntitySourceConfig::Table(OracleJdbcTableOptions::new(
                None,
                "table".to_string(),
                HashMap::from([("attr1".to_string(), "col1".to_string())]),
            )),
        ));
        conf.add(create_entity_config(
            "other",
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
        let mut select = sql::Select::new(sql::source("entity", "entity"));
        select
            .cols
            .push(("COL".to_string(), sql::Expr::attr("entity", "attr1")));
        let compiled = compile_select(select, mock_entity_table());

        assert_eq!(
            compiled,
            JdbcQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" "entity""#,
                vec![]
            )
        );
    }

    #[test]
    fn test_oracle_jdbc_compile_select_where() {
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
            JdbcQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" "entity" WHERE (("entity"."col1") = (?))"#,
                vec![JdbcQueryParam::Dynamic(1, DataType::Int32)]
            )
        );
    }

    #[test]
    fn test_oracle_jdbc_compile_select_inner_join() {
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
            JdbcQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" "entity" INNER JOIN "other" "other" ON (("entity"."col1") = ("other"."othercol1"))"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_oracle_jdbc_compile_select_left_join() {
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
            JdbcQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" "entity" LEFT JOIN "other" "other" ON (("entity"."col1") = ("other"."othercol1"))"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_oracle_jdbc_compile_select_right_join() {
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
            JdbcQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" "entity" RIGHT JOIN "other" "other" ON (("entity"."col1") = ("other"."othercol1"))"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_oracle_jdbc_compile_select_full_join() {
        let mut select = sql::Select::new(sql::source("entity", "entity"));
        select
            .cols
            .push(("COL".to_string(), sql::Expr::attr("entity", "attr1")));
        select.joins.push(sql::Join::new(
            sql::JoinType::Full,
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
            JdbcQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" "entity" FULL JOIN "other" "other" ON (("entity"."col1") = ("other"."othercol1"))"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_oracle_jdbc_compile_select_group_by() {
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
            JdbcQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" "entity" GROUP BY "entity"."col1", ?"#,
                vec![JdbcQueryParam::Constant(DataValue::Int32(1))]
            )
        );
    }

    #[test]
    fn test_oracle_jdbc_compile_select_order_by() {
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
            JdbcQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" "entity" ORDER BY "entity"."col1" ASC, ? DESC"#,
                vec![JdbcQueryParam::Constant(DataValue::Int32(1))]
            )
        );
    }

    #[test]
    fn test_oracle_jdbc_compile_select_row_skip_and_limit() {
        let mut select = sql::Select::new(sql::source("entity", "entity"));
        select
            .cols
            .push(("COL".to_string(), sql::Expr::attr("entity", "attr1")));
        select.row_skip = 10;
        select.row_limit = Some(20);
        let compiled = compile_select(select, mock_entity_table());

        assert_eq!(
            compiled,
            JdbcQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" "entity" OFFSET 10 ROWS FETCH FIRST 20 ROWS ONLY"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_oracle_jdbc_compile_select_row_skip() {
        let mut select = sql::Select::new(sql::source("entity", "entity"));
        select
            .cols
            .push(("COL".to_string(), sql::Expr::attr("entity", "attr1")));
        select.row_skip = 10;
        let compiled = compile_select(select, mock_entity_table());

        assert_eq!(
            compiled,
            JdbcQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" "entity" OFFSET 10 ROWS"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_oracle_jdbc_compile_select_row_limit() {
        let mut select = sql::Select::new(sql::source("entity", "entity"));
        select
            .cols
            .push(("COL".to_string(), sql::Expr::attr("entity", "attr1")));
        select.row_limit = Some(20);
        let compiled = compile_select(select, mock_entity_table());

        assert_eq!(
            compiled,
            JdbcQuery::new(
                r#"SELECT "entity"."col1" AS "COL" FROM "table" "entity" FETCH FIRST 20 ROWS ONLY"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_oracle_jdbc_compile_select_function_call() {
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
            JdbcQuery::new(
                r#"SELECT LENGTH("entity"."col1") AS "COL" FROM "table" "entity" OFFSET 10 ROWS"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_oracle_jdbc_compile_select_aggregate_call() {
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
            JdbcQuery::new(
                r#"SELECT SUM("entity"."col1") AS "COL" FROM "table" "entity" OFFSET 10 ROWS"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_oracle_jdbc_compile_select_for_update() {
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
            JdbcQuery::new(
                r#"SELECT SUM("entity"."col1") AS "COL" FROM "table" "entity" FOR UPDATE"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_oracle_jdbc_compile_insert_query() {
        let mut insert = sql::Insert::new(sql::source("entity", "entity"));
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
        let mut update = sql::Update::new(sql::source("entity", "entity"));
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
        let delete = sql::Delete::new(sql::source("entity", "entity"));
        let compiled = compile_delete(delete, mock_entity_table());

        assert_eq!(compiled, JdbcQuery::new(r#"DELETE FROM "table""#, vec![]));
    }

    #[test]
    fn test_oracle_jdbc_compile_delete_where_query() {
        let mut delete = sql::Delete::new(sql::source("entity", "entity"));

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

use ansilo_core::{
    data::{DataType, DataValue},
    err::{bail, Context, Result},
    sqlil as sql,
};

use ansilo_connectors_base::{common::query::QueryParam, interface::QueryCompiler};
use ansilo_connectors_jdbc_base::{JdbcConnection, JdbcQuery};

use super::{MssqlJdbcConnectorEntityConfig, MssqlJdbcEntitySourceConfig, MssqlJdbcTableOptions};

/// Query compiler for Mssql JDBC driver
pub struct MssqlJdbcQueryCompiler;

impl QueryCompiler for MssqlJdbcQueryCompiler {
    type TConnection = JdbcConnection;
    type TQuery = JdbcQuery;
    type TEntitySourceConfig = MssqlJdbcEntitySourceConfig;

    fn compile_query(
        _con: &mut Self::TConnection,
        conf: &MssqlJdbcConnectorEntityConfig,
        query: sql::Query,
    ) -> Result<JdbcQuery> {
        match &query {
            sql::Query::Select(select) => Self::compile_select_query(conf, &query, select),
            sql::Query::Insert(insert) => Self::compile_insert_query(conf, &query, insert),
            sql::Query::BulkInsert(insert) => Self::compile_bulk_insert_query(conf, &query, insert),
            sql::Query::Update(update) => Self::compile_update_query(conf, &query, update),
            sql::Query::Delete(delete) => Self::compile_delete_query(conf, &query, delete),
        }
    }

    fn query_from_string(
        _connection: &mut Self::TConnection,
        query: String,
        params: Vec<sql::Parameter>,
    ) -> Result<Self::TQuery> {
        Ok(JdbcQuery::new(
            query,
            params.into_iter().map(|p| QueryParam::dynamic(p)).collect(),
        ))
    }
}

impl MssqlJdbcQueryCompiler {
    fn compile_select_query(
        conf: &MssqlJdbcConnectorEntityConfig,
        query: &sql::Query,
        select: &sql::Select,
    ) -> Result<JdbcQuery> {
        let mut params = Vec::<QueryParam>::new();

        let query = [
            "SELECT".to_string(),
            Self::compile_select_cols(conf, query, &select.cols, &mut params)?,
            format!(
                "FROM {}",
                Self::compile_entity_source(conf, &select.from, true)?
            ),
            Self::compile_select_lock_clause(select.row_lock)?,
            Self::compile_select_joins(conf, query, &select.joins, &mut params)?,
            Self::compile_where(conf, query, &select.r#where, &mut params)?,
            Self::compile_select_group_by(conf, query, &select.group_bys, &mut params)?,
            Self::compile_order_by(conf, query, &select.order_bys, &mut params)?,
            Self::compile_offet_limit(&select.order_bys, select.row_skip, select.row_limit)?,
        ]
        .into_iter()
        .filter(|i| !i.is_empty())
        .collect::<Vec<String>>()
        .join(" ");

        Ok(JdbcQuery::new(query, params))
    }

    fn compile_insert_query(
        conf: &MssqlJdbcConnectorEntityConfig,
        query: &sql::Query,
        insert: &sql::Insert,
    ) -> Result<JdbcQuery> {
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

        Ok(JdbcQuery::new(query, params))
    }

    fn compile_bulk_insert_query(
        conf: &MssqlJdbcConnectorEntityConfig,
        query: &sql::Query,
        insert: &sql::BulkInsert,
    ) -> Result<JdbcQuery> {
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

        Ok(JdbcQuery::new(query, params))
    }

    fn compile_update_query(
        conf: &MssqlJdbcConnectorEntityConfig,
        query: &sql::Query,
        update: &sql::Update,
    ) -> Result<JdbcQuery> {
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

        Ok(JdbcQuery::new(query, params))
    }

    fn compile_delete_query(
        conf: &MssqlJdbcConnectorEntityConfig,
        query: &sql::Query,
        delete: &sql::Delete,
    ) -> Result<JdbcQuery> {
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

        Ok(JdbcQuery::new(query, params))
    }

    fn compile_select_cols(
        conf: &MssqlJdbcConnectorEntityConfig,
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
        conf: &MssqlJdbcConnectorEntityConfig,
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
        conf: &MssqlJdbcConnectorEntityConfig,
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
            sql::JoinType::Full => format!("FULL OUTER JOIN {} ON {}", target, cond),
        })
    }

    fn compile_where(
        conf: &MssqlJdbcConnectorEntityConfig,
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
        conf: &MssqlJdbcConnectorEntityConfig,
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
        conf: &MssqlJdbcConnectorEntityConfig,
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

    fn compile_offet_limit(
        order_bys: &Vec<sql::Ordering>,
        row_skip: u64,
        row_limit: Option<u64>,
    ) -> Result<String> {
        let mut parts = vec![];

        if row_skip > 0 || row_limit.is_some() {
            if order_bys.is_empty() {
                parts.push("ORDER BY (SELECT NULL)".into());
            }

            parts.push(format!("OFFSET {} ROWS", row_skip));
        }

        if let Some(lim) = row_limit {
            parts.push(format!("FETCH NEXT {} ROWS ONLY", lim));
        }

        Ok(parts.join(" "))
    }

    fn compile_select_lock_clause(mode: sql::SelectRowLockMode) -> Result<String> {
        Ok(match mode {
            sql::SelectRowLockMode::None => "",
            sql::SelectRowLockMode::ForUpdate => "WITH (UPDLOCK)",
        }
        .into())
    }

    fn compile_expr(
        conf: &MssqlJdbcConnectorEntityConfig,
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
        // @see https://dev.mssql.com/doc/refman/8.0/en/identifiers.html#:~:text=An%20identifier%20may%20be%20quoted,it%20need%20not%20be%20quoted.)
        if id.contains("[") || id.contains("]") {
            bail!("Invalid identifier: \"{id}\", cannot contain '[' or ']' chars");
        }

        Ok(format!("[{}]", id))
    }

    pub fn compile_entity_source(
        conf: &MssqlJdbcConnectorEntityConfig,
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

    pub fn compile_source_identifier(source: &MssqlJdbcEntitySourceConfig) -> Result<String> {
        Ok(match &source {
            MssqlJdbcEntitySourceConfig::Table(MssqlJdbcTableOptions {
                schema_name: db,
                table_name: table,
                ..
            }) => format!(
                "{}.{}",
                Self::compile_identifier(db.clone())?,
                Self::compile_identifier(table.clone())?
            ),
        })
    }

    fn compile_attribute_identifier(
        conf: &MssqlJdbcConnectorEntityConfig,
        query: &sql::Query,
        eva: &sql::AttributeId,
        include_table: bool,
    ) -> Result<String> {
        let source = query.get_entity_source(&eva.entity_alias)?;
        let entity = conf
            .get(&source.entity)
            .with_context(|| format!("Failed to find entity {:?}", source.entity.clone()))?;

        let table = match &entity.source {
            MssqlJdbcEntitySourceConfig::Table(table) => table,
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
        params.push(QueryParam::constant(c.value.clone()));
        Ok("?".to_string())
    }

    fn compile_param(p: &sql::Parameter, params: &mut Vec<QueryParam>) -> Result<String> {
        params.push(QueryParam::dynamic(p.clone()));
        Ok("?".to_string())
    }

    fn compile_unary_op(
        conf: &MssqlJdbcConnectorEntityConfig,
        query: &sql::Query,
        op: &sql::UnaryOp,
        params: &mut Vec<QueryParam>,
    ) -> Result<String> {
        let inner = Self::compile_expr(conf, query, &*op.expr, params)?;

        Ok(match op.r#type {
            sql::UnaryOpType::LogicalNot => format!("NOT ({})", inner),
            sql::UnaryOpType::Negate => format!("-({})", inner),
            sql::UnaryOpType::BitwiseNot => format!("~({})", inner),
            sql::UnaryOpType::IsNull => format!("({}) IS NULL", inner),
            sql::UnaryOpType::IsNotNull => format!("({}) IS NOT NULL", inner),
        })
    }

    fn compile_binary_op(
        conf: &MssqlJdbcConnectorEntityConfig,
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
            sql::BinaryOpType::Exponent => format!("POW({}, {})", l, r),
            sql::BinaryOpType::BitwiseAnd => format!("({}) & ({})", l, r),
            sql::BinaryOpType::BitwiseOr => format!("({}) | ({})", l, r),
            sql::BinaryOpType::BitwiseXor => format!("({}) ^ ({})", l, r),
            sql::BinaryOpType::BitwiseShiftLeft => format!("({}) << ({})", l, r),
            sql::BinaryOpType::BitwiseShiftRight => format!("({}) >> ({})", l, r),
            sql::BinaryOpType::Concat => format!("CONCAT({}, {})", l, r),
            sql::BinaryOpType::Regexp => unimplemented!(),
            sql::BinaryOpType::Equal => format!("({}) = ({})", l, r),
            sql::BinaryOpType::NullSafeEqual => format!("({}) IS DISTINCT FROM ({})", l, r),
            sql::BinaryOpType::NotEqual => format!("({}) != ({})", l, r),
            sql::BinaryOpType::GreaterThan => format!("({}) > ({})", l, r),
            sql::BinaryOpType::GreaterThanOrEqual => format!("({}) >= ({})", l, r),
            sql::BinaryOpType::LessThan => format!("({}) < ({})", l, r),
            sql::BinaryOpType::LessThanOrEqual => format!("({}) <= ({})", l, r),
            sql::BinaryOpType::JsonExtract => {
                format!("JSON_VALUE({}, CONCAT('$.''', ({}), '''')", l, r)
            }
        })
    }

    fn compile_cast(
        conf: &MssqlJdbcConnectorEntityConfig,
        query: &sql::Query,
        cast: &sql::Cast,
        params: &mut Vec<QueryParam>,
    ) -> Result<String> {
        let arg = Self::compile_expr(conf, query, &cast.expr, params)?;

        Ok(match &cast.r#type {
            DataType::Utf8String(_) => format!("CAST({} AS nvarchar)", arg),
            DataType::Binary => format!("CAST({} AS binary)", arg),
            DataType::Boolean => format!("CASE WHEN ({}) THEN TRUE ELSE FALSE END", arg),
            DataType::UInt8 => format!("CAST({} AS tinyint)", arg),
            DataType::Int16 => format!("CAST({} AS smallint)", arg),
            DataType::Int32 => format!("CAST({} AS int)", arg),
            DataType::Int64 => format!("CAST({} AS bigint)", arg),
            DataType::Decimal(opts) => format!(
                "CAST({} AS decimal({}, {}))",
                arg,
                opts.precision.unwrap_or(65),
                opts.scale.unwrap_or(30)
            ),
            DataType::Float32 => format!("CAST({} AS float(24))", arg),
            DataType::Float64 => format!("CAST({} AS float(53))", arg),
            DataType::Date => format!("CAST({} AS date)", arg),
            DataType::DateTime => format!("CAST({} AS datetime2)", arg),
            DataType::DateTimeWithTZ => format!("CAST({} AS datetimeoffset)", arg),
            DataType::Null => format!("CASE WHEN ({}) THEN NULL ELSE NULL END", arg),
            DataType::Time => format!("CAST({} AS time)", arg),
            _ => bail!("Unsupported cast: {:?}", cast),
        })
    }

    fn compile_function_call(
        conf: &MssqlJdbcConnectorEntityConfig,
        query: &sql::Query,
        func: &sql::FunctionCall,
        params: &mut Vec<QueryParam>,
    ) -> Result<String> {
        Ok(match func {
            sql::FunctionCall::Length(arg) => {
                format!("LEN({})", Self::compile_expr(conf, query, &*arg, params)?)
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
                "SUBSTRING({}, {}, {})",
                Self::compile_expr(conf, query, &*call.string, params)?,
                Self::compile_expr(conf, query, &*call.start, params)?,
                Self::compile_expr(conf, query, &*call.len, params)?
            ),
            sql::FunctionCall::Uuid => "UUID()".into(),
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
        conf: &MssqlJdbcConnectorEntityConfig,
        query: &sql::Query,
        agg: &sql::AggregateCall,
        params: &mut Vec<QueryParam>,
    ) -> Result<String> {
        Ok(match agg {
            sql::AggregateCall::Sum(arg) => {
                format!("SUM({})", Self::compile_expr(conf, query, &*arg, params)?)
            }
            sql::AggregateCall::Count => "COUNT_BIG(*)".into(),
            sql::AggregateCall::CountDistinct(arg) => format!(
                "COUNT_BIG(DISTINCT {})",
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
                    "STRING_AGG({}, ?)",
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

    use pretty_assertions::assert_eq;

    fn compile_select(select: sql::Select, conf: MssqlJdbcConnectorEntityConfig) -> JdbcQuery {
        let query = sql::Query::Select(select);
        MssqlJdbcQueryCompiler::compile_select_query(&conf, &query, query.as_select().unwrap())
            .unwrap()
    }

    fn compile_insert(insert: sql::Insert, conf: MssqlJdbcConnectorEntityConfig) -> JdbcQuery {
        let query = sql::Query::Insert(insert);
        MssqlJdbcQueryCompiler::compile_insert_query(&conf, &query, query.as_insert().unwrap())
            .unwrap()
    }

    fn compile_bulk_insert(
        bulk_insert: sql::BulkInsert,
        conf: MssqlJdbcConnectorEntityConfig,
    ) -> JdbcQuery {
        let query = sql::Query::BulkInsert(bulk_insert);
        MssqlJdbcQueryCompiler::compile_bulk_insert_query(
            &conf,
            &query,
            query.as_bulk_insert().unwrap(),
        )
        .unwrap()
    }

    fn compile_update(update: sql::Update, conf: MssqlJdbcConnectorEntityConfig) -> JdbcQuery {
        let query = sql::Query::Update(update);
        MssqlJdbcQueryCompiler::compile_update_query(&conf, &query, query.as_update().unwrap())
            .unwrap()
    }

    fn compile_delete(delete: sql::Delete, conf: MssqlJdbcConnectorEntityConfig) -> JdbcQuery {
        let query = sql::Query::Delete(delete);
        MssqlJdbcQueryCompiler::compile_delete_query(&conf, &query, query.as_delete().unwrap())
            .unwrap()
    }

    fn create_entity_config(
        id: &str,
        source: MssqlJdbcEntitySourceConfig,
    ) -> EntitySource<MssqlJdbcEntitySourceConfig> {
        EntitySource::new(
            EntityConfig::minimal(id, vec![], EntitySourceConfig::minimal("")),
            source,
        )
    }

    fn mock_entity_table() -> MssqlJdbcConnectorEntityConfig {
        let mut conf = MssqlJdbcConnectorEntityConfig::new();

        conf.add(create_entity_config(
            "entity",
            MssqlJdbcEntitySourceConfig::Table(MssqlJdbcTableOptions::new(
                "db".to_string(),
                "table".to_string(),
                HashMap::from([("attr1".to_string(), "col1".to_string())]),
            )),
        ));
        conf.add(create_entity_config(
            "other",
            MssqlJdbcEntitySourceConfig::Table(MssqlJdbcTableOptions::new(
                "db".to_string(),
                "other".to_string(),
                HashMap::from([("otherattr1".to_string(), "othercol1".to_string())]),
            )),
        ));

        conf
    }

    #[test]
    fn test_mssql_jdbc_compile_select() {
        let mut select = sql::Select::new(sql::source("entity", "entity"));
        select
            .cols
            .push(("COL".to_string(), sql::Expr::attr("entity", "attr1")));
        let compiled = compile_select(select, mock_entity_table());

        assert_eq!(
            compiled,
            JdbcQuery::new(
                r#"SELECT [entity].[col1] AS [COL] FROM [db].[table] AS [entity]"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_mssql_jdbc_compile_select_where() {
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
                r#"SELECT [entity].[col1] AS [COL] FROM [db].[table] AS [entity] WHERE (([entity].[col1]) = (?))"#,
                vec![QueryParam::dynamic2(1, DataType::Int32)]
            )
        );
    }

    #[test]
    fn test_mssql_jdbc_compile_select_inner_join() {
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
                r#"SELECT [entity].[col1] AS [COL] FROM [db].[table] AS [entity] INNER JOIN [db].[other] AS [other] ON (([entity].[col1]) = ([other].[othercol1]))"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_mssql_jdbc_compile_select_left_join() {
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
                r#"SELECT [entity].[col1] AS [COL] FROM [db].[table] AS [entity] LEFT JOIN [db].[other] AS [other] ON (([entity].[col1]) = ([other].[othercol1]))"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_mssql_jdbc_compile_select_right_join() {
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
                r#"SELECT [entity].[col1] AS [COL] FROM [db].[table] AS [entity] RIGHT JOIN [db].[other] AS [other] ON (([entity].[col1]) = ([other].[othercol1]))"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_mssql_jdbc_compile_select_full_join() {
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
                r#"SELECT [entity].[col1] AS [COL] FROM [db].[table] AS [entity] FULL OUTER JOIN [db].[other] AS [other] ON (([entity].[col1]) = ([other].[othercol1]))"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_mssql_jdbc_compile_select_group_by() {
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
                r#"SELECT [entity].[col1] AS [COL] FROM [db].[table] AS [entity] GROUP BY [entity].[col1], ?"#,
                vec![QueryParam::Constant(DataValue::Int32(1))]
            )
        );
    }

    #[test]
    fn test_mssql_jdbc_compile_select_order_by() {
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
                r#"SELECT [entity].[col1] AS [COL] FROM [db].[table] AS [entity] ORDER BY [entity].[col1] ASC, ? DESC"#,
                vec![QueryParam::Constant(DataValue::Int32(1))]
            )
        );
    }

    #[test]
    fn test_mssql_jdbc_compile_select_row_skip_and_limit() {
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
                r#"SELECT [entity].[col1] AS [COL] FROM [db].[table] AS [entity] ORDER BY (SELECT NULL) OFFSET 10 ROWS FETCH NEXT 20 ROWS ONLY"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_mssql_jdbc_compile_select_row_skip() {
        let mut select = sql::Select::new(sql::source("entity", "entity"));
        select
            .cols
            .push(("COL".to_string(), sql::Expr::attr("entity", "attr1")));
        select.row_skip = 10;
        let compiled = compile_select(select, mock_entity_table());

        assert_eq!(
            compiled,
            JdbcQuery::new(
                r#"SELECT [entity].[col1] AS [COL] FROM [db].[table] AS [entity] ORDER BY (SELECT NULL) OFFSET 10 ROWS"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_mssql_jdbc_compile_select_row_limit() {
        let mut select = sql::Select::new(sql::source("entity", "entity"));
        select
            .cols
            .push(("COL".to_string(), sql::Expr::attr("entity", "attr1")));
        select.row_limit = Some(20);
        let compiled = compile_select(select, mock_entity_table());

        assert_eq!(
            compiled,
            JdbcQuery::new(
                r#"SELECT [entity].[col1] AS [COL] FROM [db].[table] AS [entity] ORDER BY (SELECT NULL) OFFSET 0 ROWS FETCH NEXT 20 ROWS ONLY"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_mssql_jdbc_compile_select_row_skip_with_order_by() {
        let mut select = sql::Select::new(sql::source("entity", "entity"));
        select
            .cols
            .push(("COL".to_string(), sql::Expr::attr("entity", "attr1")));
        select.order_bys.push(sql::Ordering::new(
            sql::OrderingType::Asc,
            sql::Expr::attr("entity", "attr1"),
        ));
        select.row_skip = 10;
        let compiled = compile_select(select, mock_entity_table());

        assert_eq!(
            compiled,
            JdbcQuery::new(
                r#"SELECT [entity].[col1] AS [COL] FROM [db].[table] AS [entity] ORDER BY [entity].[col1] ASC OFFSET 10 ROWS"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_mssql_jdbc_compile_select_function_call() {
        let mut select = sql::Select::new(sql::source("entity", "entity"));
        select.cols.push((
            "COL".to_string(),
            sql::Expr::FunctionCall(sql::FunctionCall::Length(Box::new(sql::Expr::attr(
                "entity", "attr1",
            )))),
        ));
        let compiled = compile_select(select, mock_entity_table());

        assert_eq!(
            compiled,
            JdbcQuery::new(
                r#"SELECT LEN([entity].[col1]) AS [COL] FROM [db].[table] AS [entity]"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_mssql_jdbc_compile_select_aggregate_call() {
        let mut select = sql::Select::new(sql::source("entity", "entity"));
        select.cols.push((
            "COL".to_string(),
            sql::Expr::AggregateCall(sql::AggregateCall::Sum(Box::new(sql::Expr::attr(
                "entity", "attr1",
            )))),
        ));
        let compiled = compile_select(select, mock_entity_table());

        assert_eq!(
            compiled,
            JdbcQuery::new(
                r#"SELECT SUM([entity].[col1]) AS [COL] FROM [db].[table] AS [entity]"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_mssql_jdbc_compile_select_for_update() {
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
                r#"SELECT SUM([entity].[col1]) AS [COL] FROM [db].[table] AS [entity] WITH (UPDLOCK)"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_mssql_jdbc_compile_select_count() {
        let mut select = sql::Select::new(sql::source("entity", "entity"));
        select.cols.push((
            "COL".to_string(),
            sql::Expr::AggregateCall(sql::AggregateCall::Count),
        ));
        let compiled = compile_select(select, mock_entity_table());

        assert_eq!(
            compiled,
            JdbcQuery::new(
                r#"SELECT COUNT_BIG(*) AS [COL] FROM [db].[table] AS [entity]"#,
                vec![]
            )
        );
    }

    #[test]
    fn test_mssql_jdbc_compile_insert_query() {
        let mut insert = sql::Insert::new(sql::source("entity", "entity"));
        insert.cols.push((
            "attr1".to_string(),
            sql::Expr::Parameter(sql::Parameter::new(DataType::Int8, 1)),
        ));

        let compiled = compile_insert(insert, mock_entity_table());

        assert_eq!(
            compiled,
            JdbcQuery::new(
                r#"INSERT INTO [db].[table] ([col1]) VALUES (?)"#,
                vec![QueryParam::dynamic2(1, DataType::Int8)]
            )
        );
    }

    #[test]
    fn test_mssql_jdbc_compile_bulk_insert_query() {
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
            JdbcQuery::new(
                r#"INSERT INTO [db].[table] ([col1]) VALUES (?), (?), (?)"#,
                vec![
                    QueryParam::dynamic2(1, DataType::Int8),
                    QueryParam::dynamic2(2, DataType::Int8),
                    QueryParam::dynamic2(3, DataType::Int8)
                ]
            )
        );
    }

    #[test]
    fn test_mssql_jdbc_compile_update_query() {
        let mut update = sql::Update::new(sql::source("entity", "entity"));
        update
            .cols
            .push(("attr1".to_string(), sql::Expr::constant(DataValue::Int8(1))));

        let compiled = compile_update(update, mock_entity_table());

        assert_eq!(
            compiled,
            JdbcQuery::new(
                r#"UPDATE [db].[table] SET [col1] = ?"#,
                vec![QueryParam::Constant(DataValue::Int8(1))]
            )
        );
    }

    #[test]
    fn test_mssql_jdbc_compile_update_where_query() {
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
                r#"UPDATE [db].[table] SET [col1] = ? WHERE (([table].[col1]) = (?))"#,
                vec![
                    QueryParam::Constant(DataValue::Int8(1)),
                    QueryParam::dynamic2(1, DataType::Int32)
                ]
            )
        );
    }

    #[test]
    fn test_mssql_jdbc_compile_delete_query() {
        let delete = sql::Delete::new(sql::source("entity", "entity"));
        let compiled = compile_delete(delete, mock_entity_table());

        assert_eq!(
            compiled,
            JdbcQuery::new(r#"DELETE FROM [db].[table]"#, vec![])
        );
    }

    #[test]
    fn test_mssql_jdbc_compile_delete_where_query() {
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
                r#"DELETE FROM [db].[table] WHERE (([table].[col1]) = (?))"#,
                vec![QueryParam::dynamic2(1, DataType::Int32)]
            )
        );
    }
}

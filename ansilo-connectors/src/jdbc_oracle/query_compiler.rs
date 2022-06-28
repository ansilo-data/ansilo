use std::collections::HashMap;

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

impl<'a> QueryCompiler<JdbcConnection<'a>, JdbcQuery, OracleJdbcEntitySourceConfig>
    for OracleJdbcQueryCompiler
{
    fn compile_select(
        &self,
        _con: &JdbcConnection<'a>,
        conf: &OracleJdbcConnectorEntityConfig,
        select: sql::Select,
    ) -> Result<JdbcQuery> {
        self.compile_select_query(conf, select)
    }
}

impl OracleJdbcQueryCompiler {
    fn compile_select_query(
        &self,
        conf: &OracleJdbcConnectorEntityConfig,
        select: sql::Select,
    ) -> Result<JdbcQuery> {
        let mut params = Vec::<JdbcQueryParam>::new();

        let query = [
            "SELECT".to_string(),
            self.compile_select_cols(conf, select.cols, &mut params)?,
            format!(
                "FROM {}",
                self.compile_entity_identifier(conf, select.from)?
            ),
            self.compile_select_joins(conf, select.joins, &mut params)?,
            self.compile_select_where(conf, select.r#where, &mut params)?,
            self.compile_select_group_by(conf, select.group_bys, &mut params)?,
            self.compile_select_order_by(conf, select.order_bys, &mut params)?,
            self.compile_offet_limit(select.row_skip, select.row_limit)?,
        ]
        .into_iter()
        .filter(|i| !i.is_empty())
        .collect::<Vec<String>>()
        .join(" ");

        Ok(JdbcQuery::new(query, params))
    }

    fn compile_select_cols(
        &self,
        conf: &OracleJdbcConnectorEntityConfig,
        cols: HashMap<String, sql::Expr>,
        params: &mut Vec<JdbcQueryParam>,
    ) -> Result<String> {
        Ok(cols
            .into_iter()
            .map(|i| {
                Ok(format!(
                    "{} AS {}",
                    self.compile_expr(conf, i.1, params)?,
                    self.compile_identifier(i.0)?
                ))
            })
            .collect::<Result<Vec<String>>>()?
            .join(", "))
    }

    fn compile_select_joins(
        &self,
        conf: &OracleJdbcConnectorEntityConfig,
        joins: Vec<sql::Join>,
        params: &mut Vec<JdbcQueryParam>,
    ) -> Result<String> {
        Ok(joins
            .into_iter()
            .map(|j| Ok(self.compile_select_join(conf, j, params)?))
            .collect::<Result<Vec<String>>>()?
            .join(", "))
    }

    fn compile_select_join(
        &self,
        conf: &OracleJdbcConnectorEntityConfig,
        join: sql::Join,
        params: &mut Vec<JdbcQueryParam>,
    ) -> Result<String> {
        let target = self.compile_entity_identifier(conf, join.target)?;
        let cond = if join.conds.is_empty() {
            "1=1".to_string()
        } else {
            format!(
                "({})",
                join.conds
                    .into_iter()
                    .map(|e| Ok(self.compile_expr(conf, e, params)?))
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

    fn compile_select_where(
        &self,
        conf: &OracleJdbcConnectorEntityConfig,
        r#where: Vec<sql::Expr>,
        params: &mut Vec<JdbcQueryParam>,
    ) -> Result<String> {
        if r#where.is_empty() {
            return Ok("".to_string());
        }

        let clauses = r#where
            .into_iter()
            .map(|e| Ok(self.compile_expr(conf, e, params)?))
            .collect::<Result<Vec<String>>>()?
            .join(") AND (");

        Ok(format!("WHERE ({})", clauses))
    }

    fn compile_select_group_by(
        &self,
        conf: &OracleJdbcConnectorEntityConfig,
        group_bys: Vec<sql::Expr>,
        params: &mut Vec<JdbcQueryParam>,
    ) -> Result<String> {
        if group_bys.is_empty() {
            return Ok("".to_string());
        }

        let clauses = group_bys
            .into_iter()
            .map(|e| Ok(self.compile_expr(conf, e, params)?))
            .collect::<Result<Vec<String>>>()?
            .join(", ");

        Ok(format!("GROUP BY {}", clauses))
    }

    fn compile_select_order_by(
        &self,
        conf: &OracleJdbcConnectorEntityConfig,
        order_bys: Vec<sql::Ordering>,
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
                    self.compile_expr(conf, i.expr, params)?,
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

    fn compile_offet_limit(&self, row_skip: u64, row_limit: Option<u64>) -> Result<String> {
        let mut parts = vec![];

        if row_skip > 0 {
            parts.push(format!("OFFSET {}", row_skip));
        }

        if let Some(lim) = row_limit {
            parts.push(format!("FETCH FIRST {} ROWS ONLY", lim));
        }

        Ok(parts.join(" "))
    }

    fn compile_expr(
        &self,
        conf: &OracleJdbcConnectorEntityConfig,
        expr: sql::Expr,
        params: &mut Vec<JdbcQueryParam>,
    ) -> Result<String> {
        let sql = match expr {
            sql::Expr::EntityVersion(evi) => self.compile_entity_identifier(conf, evi)?,
            sql::Expr::EntityVersionAttribute(eva) => {
                self.compile_attribute_identifier(conf, eva)?
            }
            sql::Expr::Constant(c) => self.compile_constant(c, params)?,
            sql::Expr::Parameter(p) => self.compile_param(p, params)?,
            sql::Expr::UnaryOp(o) => self.compile_unary_op(conf, o, params)?,
            sql::Expr::BinaryOp(b) => self.compile_binary_op(conf, b, params)?,
            sql::Expr::FunctionCall(f) => self.compile_function_call(conf, f, params)?,
            sql::Expr::AggregateCall(a) => self.compile_aggregate_call(conf, a, params)?,
        };

        Ok(sql)
    }

    pub fn compile_identifier(&self, id: String) -> Result<String> {
        // @see https://docs.oracle.com/cd/B19306_01/server.102/b14200/sql_elements008.htm
        if id.contains('"') || id.contains("\0") {
            bail!("Invalid identifier: \"{id}\", cannot contain '\"' or '\\0' chars");
        }

        Ok(format!("\"{}\"", id))
    }

    pub fn compile_entity_identifier(
        &self,
        conf: &OracleJdbcConnectorEntityConfig,
        evi: sql::EntityVersionIdentifier,
    ) -> Result<String> {
        let entity = conf
            .find(&evi)
            .with_context(|| format!("Failed to find entity {:?}", evi.clone()))?;

        self.compile_source_identifier(&entity.source_conf)
    }

    pub fn compile_source_identifier(
        &self,
        source: &OracleJdbcEntitySourceConfig,
    ) -> Result<String> {
        // TODO: custom query
        Ok(match &source {
            OracleJdbcEntitySourceConfig::Table(OracleJdbcTableOptions {
                database_name: Some(db),
                table_name: table,
                ..
            }) => format!(
                "{}.{}",
                self.compile_identifier(db.clone())?,
                self.compile_identifier(table.clone())?
            ),
            OracleJdbcEntitySourceConfig::Table(OracleJdbcTableOptions {
                database_name: None,
                table_name: table,
                ..
            }) => self.compile_identifier(table.clone())?,
            OracleJdbcEntitySourceConfig::CustomQueries(_) => todo!(),
        })
    }

    fn compile_attribute_identifier(
        &self,
        conf: &OracleJdbcConnectorEntityConfig,
        eva: sql::EntityVersionAttributeIdentifier,
    ) -> Result<String> {
        let entity = conf
            .find(&eva.entity)
            .with_context(|| format!("Failed to find entity {:?}", eva.entity.clone()))?;

        // TODO: custom query
        let attr_col_map = match &entity.source_conf {
            OracleJdbcEntitySourceConfig::Table(table) => &table.attribute_column_name_map,
            OracleJdbcEntitySourceConfig::CustomQueries(_) => todo!(),
        };

        let column = attr_col_map.get(&eva.attribute_id).with_context(|| {
            format!(
                "Unknown attribute {} on entity {:?}",
                eva.attribute_id,
                eva.entity.clone()
            )
        })?;

        Ok(vec![
            self.compile_entity_identifier(conf, eva.entity)?,
            self.compile_identifier(column.clone())?,
        ]
        .join("."))
    }

    fn compile_constant(
        &self,
        c: sql::Constant,
        params: &mut Vec<JdbcQueryParam>,
    ) -> Result<String> {
        params.push(JdbcQueryParam::Constant(c.value));
        Ok("?".to_string())
    }

    fn compile_param(&self, p: sql::Parameter, params: &mut Vec<JdbcQueryParam>) -> Result<String> {
        params.push(JdbcQueryParam::Dynamic(p.r#type));
        Ok("?".to_string())
    }

    fn compile_unary_op(
        &self,
        conf: &OracleJdbcConnectorEntityConfig,

        op: sql::UnaryOp,
        params: &mut Vec<JdbcQueryParam>,
    ) -> Result<String> {
        let inner = self.compile_expr(conf, *op.expr, params)?;

        Ok(match op.r#type {
            sql::UnaryOpType::Not => format!("!({})", inner),
            sql::UnaryOpType::Negate => format!("!({})", inner),
            sql::UnaryOpType::BitwiseNot => format!("UTL_RAW.BIT_COMPLEMENT({})", inner),
            sql::UnaryOpType::IsNull => format!("({}) IS NULL", inner),
            sql::UnaryOpType::IsNotNull => format!("({}) IS NOT NULL", inner),
        })
    }

    fn compile_binary_op(
        &self,
        conf: &OracleJdbcConnectorEntityConfig,
        op: sql::BinaryOp,
        params: &mut Vec<JdbcQueryParam>,
    ) -> Result<String> {
        let l = self.compile_expr(conf, *op.left, params)?;
        let r = self.compile_expr(conf, *op.right, params)?;

        // TODO
        Ok(match op.r#type {
            sql::BinaryOpType::Add => format!("({}) + ({})", l, r),
            sql::BinaryOpType::Subtract => format!("({}) - ({})", l, r),
            sql::BinaryOpType::Multiply => format!("({}) * ({})", l, r),
            sql::BinaryOpType::Divide => format!("({}) / ({})", l, r),
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
            sql::BinaryOpType::NotEqual => todo!(),
            sql::BinaryOpType::GreaterThan => todo!(),
            sql::BinaryOpType::GreaterThanOrEqual => todo!(),
            sql::BinaryOpType::LessThan => todo!(),
            sql::BinaryOpType::LessThanOrEqual => todo!(),
        })
    }

    fn compile_function_call(
        &self,
        conf: &OracleJdbcConnectorEntityConfig,
        func: sql::FunctionCall,
        params: &mut Vec<JdbcQueryParam>,
    ) -> Result<String> {
        Ok(match func {
            sql::FunctionCall::Length(arg) => {
                format!("LENGTH({})", self.compile_expr(conf, *arg, params)?)
            }
            sql::FunctionCall::Abs(_) => todo!(),
            sql::FunctionCall::Uppercase(_) => todo!(),
            sql::FunctionCall::Lowercase(_) => todo!(),
            sql::FunctionCall::Substring(_) => todo!(),
            sql::FunctionCall::Now => todo!(),
            sql::FunctionCall::Uuid => todo!(),
            sql::FunctionCall::Coalesce(_) => todo!(),
        })
    }

    fn compile_aggregate_call(
        &self,
        conf: &OracleJdbcConnectorEntityConfig,
        agg: sql::AggregateCall,
        params: &mut Vec<JdbcQueryParam>,
    ) -> Result<String> {
        Ok(match agg {
            sql::AggregateCall::Sum(arg) => {
                format!("SUM({})", self.compile_expr(conf, *arg, params)?)
            }
            sql::AggregateCall::Count => todo!(),
            sql::AggregateCall::CountDistinct(_) => todo!(),
            sql::AggregateCall::Max(_) => todo!(),
            sql::AggregateCall::Min(_) => todo!(),
            sql::AggregateCall::StringAgg(_, _) => todo!(),
        })
    }
}

#[cfg(test)]
mod tests {
    use ansilo_core::{
        common::data::{DataType},
        config::{EntityAccessiblity, EntityConfig, EntitySourceConfig, EntityVersionConfig},
    };

    use crate::common::entity::EntitySource;

    use super::*;

    fn compile_select(select: sql::Select, conf: OracleJdbcConnectorEntityConfig) -> JdbcQuery {
        let compiler = OracleJdbcQueryCompiler {};
        compiler.compile_select_query(&conf, select).unwrap()
    }

    fn create_entity_config(
        id: &str,
        version: &str,
        source: OracleJdbcEntitySourceConfig,
    ) -> OracleJdbcConnectorEntityConfig {
        let entity_conf = EntitySource::new(
            EntityConfig {
                id: id.to_string(),
                name: "name".to_string(),
                description: "".to_string(),
                tags: vec![],
                versions: vec![EntityVersionConfig {
                    version: version.to_string(),
                    attributes: vec![],
                    constraints: vec![],
                    source: EntitySourceConfig {
                        data_source_id: "".to_string(),
                        options: ansilo_core::config::Value::Null,
                    },
                }],
                accessibility: EntityAccessiblity::Public,
            },
            version.to_string(),
            source,
        )
        .unwrap();

        let mut conf = OracleJdbcConnectorEntityConfig::new();
        conf.add(entity_conf);
        conf
    }

    fn mock_entity_table() -> OracleJdbcConnectorEntityConfig {
        create_entity_config(
            "entity",
            "v1",
            OracleJdbcEntitySourceConfig::Table(OracleJdbcTableOptions::new(
                None,
                "table".to_string(),
                HashMap::from([("attr1".to_string(), "col1".to_string())]),
            )),
        )
    }

    #[test]
    fn test_oracle_jdbc_compile_select() {
        let mut select = sql::Select::new(sql::entity("entity", "v1"));
        select
            .cols
            .insert("COL".to_string(), sql::Expr::attr("entity", "v1", "attr1"));
        let compiled = compile_select(select, mock_entity_table());

        assert_eq!(
            compiled,
            JdbcQuery::new(r#"SELECT "table"."col1" AS "COL" FROM "table""#, vec![])
        );
    }

    #[test]
    fn test_oracle_jdbc_compile_select_where() {
        let mut select = sql::Select::new(sql::entity("entity", "v1"));
        select
            .cols
            .insert("COL".to_string(), sql::Expr::attr("entity", "v1", "attr1"));
        select.r#where.push(sql::Expr::BinaryOp(sql::BinaryOp::new(
            sql::Expr::attr("entity", "v1", "attr1"),
            sql::BinaryOpType::Equal,
            sql::Expr::Parameter(sql::Parameter::new(DataType::Int32, 1)),
        )));
        let compiled = compile_select(select, mock_entity_table());

        assert_eq!(
            compiled,
            JdbcQuery::new(
                r#"SELECT "table"."col1" AS "COL" FROM "table" WHERE (("table"."col1") = (?))"#,
                vec![JdbcQueryParam::Dynamic(DataType::Int32)]
            )
        );
    }
}

use std::collections::HashMap;

use ansilo_core::{
    err::{bail, Result},
    sqlil as sql,
};

use crate::{
    interface::QueryCompiler,
    jdbc::{JdbcConnection, JdbcQuery, JdbcQueryParam},
};

/// Query compiler for Oracle JDBC driver
pub struct OracleJdbcQueryCompiler;

impl<'a> QueryCompiler<JdbcConnection<'a>, JdbcQuery> for OracleJdbcQueryCompiler {
    fn compile_select(&self, _con: &JdbcConnection<'a>, select: sql::Select) -> Result<JdbcQuery> {
        self.compile_select_query(select)
    }
}

impl OracleJdbcQueryCompiler {
    fn compile_select_query(&self, select: sql::Select) -> Result<JdbcQuery> {
        let mut params = Vec::<JdbcQueryParam>::new();

        let query = [
            "SELECT".to_string(),
            self.compile_select_cols(select.cols, &mut params)?,
            format!("FROM {}", self.compile_entity_identifier(select.from)?),
            self.compile_select_joins(select.joins, &mut params)?,
            self.compile_select_where(select.r#where, &mut params)?,
            self.compile_select_group_by(select.group_bys, &mut params)?,
            self.compile_select_order_by(select.order_bys, &mut params)?,
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
        cols: HashMap<String, sql::Expr>,
        params: &mut Vec<JdbcQueryParam>,
    ) -> Result<String> {
        Ok(cols
            .into_iter()
            .map(|i| {
                Ok(format!(
                    "{} AS {}",
                    self.compile_expr(i.1, params)?,
                    self.compile_identifier(i.0)?
                ))
            })
            .collect::<Result<Vec<String>>>()?
            .join(", "))
    }

    fn compile_select_joins(
        &self,
        joins: Vec<sql::Join>,
        params: &mut Vec<JdbcQueryParam>,
    ) -> Result<String> {
        Ok(joins
            .into_iter()
            .map(|j| Ok(self.compile_select_join(j, params)?))
            .collect::<Result<Vec<String>>>()?
            .join(", "))
    }

    fn compile_select_join(
        &self,
        join: sql::Join,
        params: &mut Vec<JdbcQueryParam>,
    ) -> Result<String> {
        let target = self.compile_entity_identifier(join.target)?;
        let cond = if join.conds.is_empty() {
            "1=1".to_string()
        } else {
            format!(
                "({})",
                join.conds
                    .into_iter()
                    .map(|e| Ok(self.compile_expr(e, params)?))
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
        r#where: Vec<sql::Expr>,
        params: &mut Vec<JdbcQueryParam>,
    ) -> Result<String> {
        if r#where.is_empty() {
            return Ok("".to_string());
        }

        let clauses = r#where
            .into_iter()
            .map(|e| Ok(self.compile_expr(e, params)?))
            .collect::<Result<Vec<String>>>()?
            .join(") AND (");

        Ok(format!("WHERE ({})", clauses))
    }

    fn compile_select_group_by(
        &self,
        group_bys: Vec<sql::Expr>,
        params: &mut Vec<JdbcQueryParam>,
    ) -> Result<String> {
        if group_bys.is_empty() {
            return Ok("".to_string());
        }

        let clauses = group_bys
            .into_iter()
            .map(|e| Ok(self.compile_expr(e, params)?))
            .collect::<Result<Vec<String>>>()?
            .join(", ");

        Ok(format!("GROUP BY {}", clauses))
    }

    fn compile_select_order_by(
        &self,
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
                    self.compile_expr(i.expr, params)?,
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

    fn compile_expr(&self, expr: sql::Expr, params: &mut Vec<JdbcQueryParam>) -> Result<String> {
        let sql = match expr {
            sql::Expr::EntityVersion(evi) => self.compile_entity_identifier(evi)?,
            sql::Expr::EntityVersionAttribute(eva) => self.compile_attribute_identifier(eva)?,
            sql::Expr::Constant(c) => self.compile_constant(c, params)?,
            sql::Expr::Parameter(p) => self.compile_param(p, params)?,
            sql::Expr::UnaryOp(o) => self.compile_unary_op(o, params)?,
            sql::Expr::BinaryOp(b) => self.compile_binary_op(b, params)?,
            sql::Expr::FunctionCall(f) => self.compile_function_call(f, params)?,
            sql::Expr::AggregateCall(a) => self.compile_aggregate_call(a, params)?,
        };

        Ok(sql)
    }

    fn compile_identifier(&self, id: String) -> Result<String> {
        // @see https://docs.oracle.com/cd/B19306_01/server.102/b14200/sql_elements008.htm
        if id.contains('"') || id.contains("\0") {
            bail!("Invalid identifier: \"{id}\", cannot contain '\"' or '\\0' chars");
        }

        Ok(format!("\"{}\"", id))
    }

    fn compile_entity_identifier(&self, evi: sql::EntityVersionIdentifier) -> Result<String> {
        // TODO: mapping to underlying identifier
        self.compile_identifier(format!("{}_{}", evi.entity_id, evi.version_id))
    }

    fn compile_attribute_identifier(
        &self,
        eva: sql::EntityVersionAttributeIdentifier,
    ) -> Result<String> {
        // TODO: mapping to underlying identifier
        Ok(vec![
            self.compile_entity_identifier(eva.entity)?,
            self.compile_identifier(eva.attribute_id)?,
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
        op: sql::UnaryOp,
        params: &mut Vec<JdbcQueryParam>,
    ) -> Result<String> {
        let inner = self.compile_expr(*op.expr, params)?;

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
        op: sql::BinaryOp,
        params: &mut Vec<JdbcQueryParam>,
    ) -> Result<String> {
        let l = self.compile_expr(*op.left, params)?;
        let r = self.compile_expr(*op.right, params)?;

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
            sql::BinaryOpType::Equal => todo!(),
            sql::BinaryOpType::NotEqual => todo!(),
            sql::BinaryOpType::GreaterThan => todo!(),
            sql::BinaryOpType::GreaterThanOrEqual => todo!(),
            sql::BinaryOpType::LessThan => todo!(),
            sql::BinaryOpType::LessThanOrEqual => todo!(),
        })
    }

    fn compile_function_call(
        &self,
        func: sql::FunctionCall,
        params: &mut Vec<JdbcQueryParam>,
    ) -> Result<String> {
        Ok(match func {
            sql::FunctionCall::Length(arg) => {
                format!("LENGTH({})", self.compile_expr(*arg, params)?)
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
        agg: sql::AggregateCall,
        params: &mut Vec<JdbcQueryParam>,
    ) -> Result<String> {
        Ok(match agg {
            sql::AggregateCall::Sum(arg) => format!("SUM({})", self.compile_expr(*arg, params)?),
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
    use ansilo_core::sqlil::EntityVersionAttributeIdentifier;

    use super::*;

    fn compile_select(select: sql::Select) -> JdbcQuery {
        let compiler = OracleJdbcQueryCompiler {};
        compiler.compile_select_query(select).unwrap()
    }

    #[test]
    fn test_oracle_jdbc_compile_select() {
        let entity = sql::EntityVersionIdentifier::new("entity", "v1");
        let mut select = sql::Select::new(entity.clone());
        select.cols.insert(
            "COL".to_string(),
            sql::Expr::EntityVersionAttribute(EntityVersionAttributeIdentifier::new(
                entity.clone(), "attr",
            )),
        );
        let compiled = compile_select(select);

        assert_eq!(
            compiled,
            JdbcQuery::new(
                r#"SELECT "entity_v1"."attr" AS "COL" FROM "entity_v1""#,
                vec![]
            )
        );
    }
}

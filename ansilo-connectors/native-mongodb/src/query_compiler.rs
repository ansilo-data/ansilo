use ansilo_core::{
    err::{bail, ensure, Context, Result},
    sqlil as sql,
};

use ansilo_connectors_base::{common::query::QueryParam, interface::QueryCompiler};
use mongodb::bson::{doc, Bson, Document, JavaScriptCodeWithScope};

use crate::{
    val_to_bson, DeleteManyQuery, FindQuery, InsertManyQuery, MongodbConnection, MongodbQuery,
    MongodbQueryType, UpdateManyQuery,
};

use super::{MongodbConnectorEntityConfig, MongodbEntitySourceConfig};

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

    fn query_from_string(
        _connection: &mut Self::TConnection,
        _query: String,
        _params: Vec<sql::Parameter>,
    ) -> Result<Self::TQuery> {
        bail!("Unsupported")
    }
}

impl MongodbQueryCompiler {
    fn compile_select_query(
        conf: &MongodbConnectorEntityConfig,
        query: &sql::Query,
        select: &sql::Select,
    ) -> Result<MongodbQuery> {
        let (db, col) = Self::get_collection(conf, &select.from)?;

        let filter = Self::compile_filter(&select.r#where)?;

        let sort = if select.order_bys.is_empty() {
            None
        } else {
            let mut sorts = Document::new();

            for ordering in select.order_bys.iter().cloned() {
                sorts.insert(
                    Self::compile_field(&ordering.expr)?,
                    if ordering.r#type.is_asc() { 1 } else { -1 },
                );
            }

            Some(sorts)
        };

        Ok(MongodbQuery::new(
            db,
            col,
            MongodbQueryType::Find(FindQuery {
                filter,
                sort,
                skip: if select.row_skip == 0 {
                    None
                } else {
                    Some(select.row_skip)
                },
                limit: select.row_limit,
            }),
            Self::get_params(query)?,
        ))
    }

    fn compile_insert_query(
        conf: &MongodbConnectorEntityConfig,
        query: &sql::Query,
        insert: &sql::Insert,
    ) -> Result<MongodbQuery> {
        let (db, col) = Self::get_collection(conf, &insert.target)?;

        ensure!(
            insert.cols.len() == 1,
            "Only one insert column is supported"
        );

        let doc = Self::compile_expr(&insert.cols[0].1)?;

        Ok(MongodbQuery::new(
            db,
            col,
            MongodbQueryType::InsertMany(InsertManyQuery { docs: vec![doc] }),
            Self::get_params(query)?,
        ))
    }

    fn compile_bulk_insert_query(
        conf: &MongodbConnectorEntityConfig,
        query: &sql::Query,
        insert: &sql::BulkInsert,
    ) -> Result<MongodbQuery> {
        let (db, col) = Self::get_collection(conf, &insert.target)?;

        ensure!(
            insert.cols.len() == 1,
            "Only one insert column is supported"
        );

        let mut docs = vec![];

        for row in insert.values.iter() {
            docs.push(Self::compile_expr(row)?);
        }

        Ok(MongodbQuery::new(
            db,
            col,
            MongodbQueryType::InsertMany(InsertManyQuery { docs }),
            Self::get_params(query)?,
        ))
    }

    fn compile_update_query(
        conf: &MongodbConnectorEntityConfig,
        query: &sql::Query,
        update: &sql::Update,
    ) -> Result<MongodbQuery> {
        let (db, col) = Self::get_collection(conf, &update.target)?;

        ensure!(
            update.cols.len() == 1,
            "Only one update column is supported"
        );

        let new_root = Self::compile_expr(&update.cols[0].1)?;
        let pipeline = vec![doc! { "$replaceRoot": { "newRoot": new_root } }];

        let filter = Self::compile_filter(&update.r#where)?;

        Ok(MongodbQuery::new(
            db,
            col,
            MongodbQueryType::UpdateMany(UpdateManyQuery { pipeline, filter }),
            Self::get_params(query)?,
        ))
    }

    fn compile_delete_query(
        conf: &MongodbConnectorEntityConfig,
        query: &sql::Query,
        delete: &sql::Delete,
    ) -> Result<MongodbQuery> {
        let (db, col) = Self::get_collection(conf, &delete.target)?;

        let filter = Self::compile_filter(&delete.r#where)?;

        Ok(MongodbQuery::new(
            db,
            col,
            MongodbQueryType::DeleteMany(DeleteManyQuery { filter }),
            Self::get_params(query)?,
        ))
    }

    fn compile_filter(r#where: &Vec<sql::Expr>) -> Result<Option<Document>> {
        let filter = if r#where.is_empty() {
            None
        } else {
            Some({
                let mut doc = Document::new();
                doc.insert(
                    "$and",
                    r#where
                        .iter()
                        .map(|e| Self::compile_expr(e))
                        .collect::<Result<Vec<Bson>>>()?,
                );
                doc
            })
        };

        Ok(filter)
    }

    pub fn compile_expr(expr: &sql::Expr) -> Result<Bson> {
        if let Ok(field) = Self::compile_field(expr) {
            return Ok(Bson::String(field));
        }

        let sql = match expr {
            sql::Expr::Constant(c) => Self::compile_constant(c)?,
            sql::Expr::Parameter(p) => Self::compile_param(p)?,
            sql::Expr::UnaryOp(o) => Self::compile_unary_op(o)?,
            sql::Expr::BinaryOp(b) => Self::compile_binary_op(b)?,
            _ => bail!("Unsupported expr: {:?}", expr),
        };

        Ok(sql)
    }

    pub fn compile_field(expr: &sql::Expr) -> Result<String> {
        let mut fields = vec![];

        loop {
            let (expr, field) = match expr {
                sql::Expr::BinaryOp(sql::BinaryOp {
                    left,
                    r#type: sql::BinaryOpType::JsonExtract,
                    right,
                }) if right.as_constant().is_some()
                    && right
                        .as_constant()
                        .unwrap()
                        .value
                        .as_utf8_string()
                        .is_some() =>
                {
                    (
                        left.as_ref(),
                        right
                            .as_constant()
                            .unwrap()
                            .value
                            .as_utf8_string()
                            .unwrap()
                            .clone(),
                    )
                }
                _ => bail!("Expected field expression but found: {:?}", expr),
            };

            fields.push(field);

            if let sql::Expr::Attribute(_) = expr {
                break;
            }
        }

        fields.reverse();
        Ok(fields.join("."))
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

        let col = match &entity.source {
            MongodbEntitySourceConfig::Collection(col) => col,
        };

        Ok((col.database_name.clone(), col.collection_name.clone()))
    }

    fn compile_constant(c: &sql::Constant) -> Result<Bson> {
        val_to_bson(c.value.clone())
    }

    pub(crate) fn compile_param(p: &sql::Parameter) -> Result<Bson> {
        // We abuse the JavaScriptCodeWithScope type to encode query parameters
        Ok(Bson::JavaScriptCodeWithScope(JavaScriptCodeWithScope {
            code: format!("__param::{}", p.id),
            scope: Document::new(),
        }))
    }

    fn compile_unary_op(op: &sql::UnaryOp) -> Result<Bson> {
        let inner = Self::compile_expr(&op.expr)?;

        Ok(match op.r#type {
            sql::UnaryOpType::LogicalNot => Bson::Document(doc! { "$not": inner }),
            _ => bail!("Unsupported expr: {:?}", op),
        })
    }

    fn compile_binary_op(op: &sql::BinaryOp) -> Result<Bson> {
        let l = Self::compile_expr(&op.left)?;
        let r = Self::compile_expr(&op.right)?;

        if let Bson::String(field) = l.clone() {
            match op.r#type {
                sql::BinaryOpType::Equal => {
                    return Ok(Bson::Document(doc! { field: { "$eq": r } }))
                }
                sql::BinaryOpType::NullSafeEqual => {
                    return Ok(Bson::Document(doc! { field: { "$eq": r } }))
                }
                sql::BinaryOpType::NotEqual => {
                    return Ok(Bson::Document(doc! { field: { "$ne": r } }))
                }
                sql::BinaryOpType::GreaterThan => {
                    return Ok(Bson::Document(doc! { field: { "$gt": r } }))
                }
                sql::BinaryOpType::GreaterThanOrEqual => {
                    return Ok(Bson::Document(doc! { field: { "$gte": r } }))
                }
                sql::BinaryOpType::LessThan => {
                    return Ok(Bson::Document(doc! { field: { "$lt": r } }))
                }
                sql::BinaryOpType::LessThanOrEqual => {
                    return Ok(Bson::Document(doc! { field: { "$lte": r } }))
                }
                _ => {}
            };
        }

        Ok(match op.r#type {
            sql::BinaryOpType::LogicalAnd => Bson::Document(doc! { "$and": [l, r] }),
            sql::BinaryOpType::LogicalOr => Bson::Document(doc! { "$or": [l, r] }),
            _ => bail!("Unsupported expr: {:?}", op),
        })
    }

    fn get_params(query: &sql::Query) -> Result<Vec<QueryParam>> {
        let mut params = vec![];

        query.walk_expr(&mut |e| {
            if let Some(p) = e.as_parameter() {
                params.push(QueryParam::Dynamic(p.clone()));
            }
        });

        Ok(params)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ansilo_core::{
        config::{EntityConfig, EntitySourceConfig},
        data::{DataType, DataValue},
    };

    use ansilo_connectors_base::common::entity::EntitySource;
    use mongodb::bson::bson;
    use pretty_assertions::assert_eq;

    use crate::MongodbCollectionOptions;

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

    fn mock_entity_conf() -> MongodbConnectorEntityConfig {
        let mut conf = MongodbConnectorEntityConfig::new();

        conf.add(EntitySource::new(
            EntityConfig::minimal("entity", vec![], EntitySourceConfig::minimal("")),
            MongodbEntitySourceConfig::Collection(MongodbCollectionOptions::new(
                "db".into(),
                "col".into(),
            )),
        ));

        conf
    }

    fn mock_param(id: u32) -> Bson {
        MongodbQueryCompiler::compile_param(&sql::Parameter::new(DataType::Null, id)).unwrap()
    }

    #[test]
    fn test_compile_select() {
        let mut select = sql::Select::new(sql::source("entity", "entity"));
        select.r#where.push(sql::Expr::BinaryOp(sql::BinaryOp::new(
            sql::Expr::BinaryOp(sql::BinaryOp::new(
                sql::Expr::attr("entity", "doc"),
                sql::BinaryOpType::JsonExtract,
                sql::Expr::constant(DataValue::Utf8String("field".into())),
            )),
            sql::BinaryOpType::Equal,
            sql::Expr::Parameter(sql::Parameter::new(DataType::Int32, 1)),
        )));
        select.order_bys.push(sql::Ordering::new(
            sql::OrderingType::Asc,
            sql::Expr::BinaryOp(sql::BinaryOp::new(
                sql::Expr::attr("entity", "doc"),
                sql::BinaryOpType::JsonExtract,
                sql::Expr::constant(DataValue::Utf8String("another_field".into())),
            )),
        ));
        select.row_skip = 5;
        select.row_limit = Some(10);

        let compiled = compile_select(select, mock_entity_conf());

        assert_eq!(
            compiled,
            MongodbQuery::new(
                "db".into(),
                "col".into(),
                MongodbQueryType::Find(FindQuery {
                    filter: Some(doc! { "$and": [{ "field": { "$eq": mock_param(1) } }] }),
                    sort: Some(doc! { "another_field": 1 }),
                    skip: Some(5),
                    limit: Some(10)
                }),
                vec![QueryParam::dynamic2(1, DataType::Int32)]
            )
        );
    }

    #[test]
    fn test_compile_update() {
        let mut update = sql::Update::new(sql::source("entity", "entity"));
        update.cols.push((
            "doc".to_string(),
            sql::Expr::constant(DataValue::JSON(r#"{"new": "doc"}"#.into())),
        ));

        update.r#where.push(sql::Expr::BinaryOp(sql::BinaryOp::new(
            sql::Expr::BinaryOp(sql::BinaryOp::new(
                sql::Expr::attr("entity", "doc"),
                sql::BinaryOpType::JsonExtract,
                sql::Expr::constant(DataValue::Utf8String("field".into())),
            )),
            sql::BinaryOpType::Equal,
            sql::Expr::Parameter(sql::Parameter::new(DataType::Int32, 1)),
        )));

        let compiled = compile_update(update, mock_entity_conf());

        assert_eq!(
            compiled,
            MongodbQuery::new(
                "db".into(),
                "col".into(),
                MongodbQueryType::UpdateMany(UpdateManyQuery {
                    pipeline: vec![doc! { "$replaceRoot": { "newRoot": {"new": "doc"} } }],
                    filter: Some(doc! { "$and": [{ "field": { "$eq": mock_param(1) } }] }),
                }),
                vec![QueryParam::dynamic2(1, DataType::Int32)]
            )
        );
    }

    #[test]
    fn test_compile_insert() {
        let mut insert = sql::Insert::new(sql::source("entity", "entity"));
        insert.cols.push((
            "doc".to_string(),
            sql::Expr::constant(DataValue::JSON(r#"{"new": "doc"}"#.into())),
        ));

        let compiled = compile_insert(insert, mock_entity_conf());

        assert_eq!(
            compiled,
            MongodbQuery::new(
                "db".into(),
                "col".into(),
                MongodbQueryType::InsertMany(InsertManyQuery {
                    docs: vec![bson!({"new": "doc"})]
                }),
                vec![]
            )
        );
    }

    #[test]
    fn test_compile_bulk_insert() {
        let mut insert = sql::BulkInsert::new(sql::source("entity", "entity"));
        insert.cols.push("doc".into());
        insert.values.push(sql::Expr::constant(DataValue::JSON(
            r#"{"new": "doc"}"#.into(),
        )));
        insert.values.push(sql::Expr::constant(DataValue::JSON(
            r#"{"second": "docu"}"#.into(),
        )));

        let compiled = compile_bulk_insert(insert, mock_entity_conf());

        assert_eq!(
            compiled,
            MongodbQuery::new(
                "db".into(),
                "col".into(),
                MongodbQueryType::InsertMany(InsertManyQuery {
                    docs: vec![bson!({"new": "doc"}), bson!({"second": "docu"})]
                }),
                vec![]
            )
        );
    }

    #[test]
    fn test_compile_delete() {
        let mut delete = sql::Delete::new(sql::source("entity", "entity"));

        delete.r#where.push(sql::Expr::BinaryOp(sql::BinaryOp::new(
            sql::Expr::BinaryOp(sql::BinaryOp::new(
                sql::Expr::attr("entity", "doc"),
                sql::BinaryOpType::JsonExtract,
                sql::Expr::constant(DataValue::Utf8String("field".into())),
            )),
            sql::BinaryOpType::Equal,
            sql::Expr::Parameter(sql::Parameter::new(DataType::Int32, 1)),
        )));

        let compiled = compile_delete(delete, mock_entity_conf());

        assert_eq!(
            compiled,
            MongodbQuery::new(
                "db".into(),
                "col".into(),
                MongodbQueryType::DeleteMany(DeleteManyQuery {
                    filter: Some(doc! { "$and": [{ "field": { "$eq": mock_param(1) } }] }),
                }),
                vec![QueryParam::dynamic2(1, DataType::Int32)]
            )
        );
    }
}

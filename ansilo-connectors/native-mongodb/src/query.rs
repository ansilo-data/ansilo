use std::{
    collections::HashMap,
    io::Write,
    sync::{Arc, Mutex},
};

use ansilo_connectors_base::{
    common::{data::QueryParamSink, query::QueryParam},
    interface::{LoggedQuery, QueryHandle, QueryInputStructure},
};
use ansilo_core::{
    data::DataValue,
    err::{bail, Context, Error, Result},
};
use itertools::Itertools;
use mongodb::{
    bson::{Bson, Document},
    options::FindOptions,
    results::{DeleteResult, InsertManyResult, UpdateResult},
    sync::{ClientSession, SessionCursor},
};
use serde::Serialize;

use crate::{result_set::MongodbResultSet, val_to_bson};

/// Mongodb query
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct MongodbQuery {
    database: String,
    collection: String,
    q: MongodbQueryType,
    params: Vec<QueryParam>,
}

impl MongodbQuery {
    pub fn new(
        database: String,
        collection: String,
        q: MongodbQueryType,
        params: Vec<QueryParam>,
    ) -> Self {
        Self {
            database,
            collection,
            q,
            params,
        }
    }

    fn replace_query_params(&mut self, params: HashMap<u32, DataValue>) -> Result<()> {
        self.q.walk_bson_mut(|bson| {
            // We abuse the JavaScriptCodeWithScope type to encode query parameters
            if let Bson::JavaScriptCodeWithScope(code) = bson {
                if code.code.starts_with("__param::") {
                    let param_id = code.code["__param::".len()..].parse::<u32>()?;
                    let param = params
                        .get(&param_id)
                        .context("Failed to get param")?
                        .clone();

                    *bson = val_to_bson(param)?;
                }
            }

            Ok(())
        })
    }
}

/// Mongodb query type
#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum MongodbQueryType {
    Find(FindQuery),
    InsertMany(InsertManyQuery),
    UpdateMany(UpdateManyQuery),
    DeleteMany(DeleteManyQuery),
}

impl MongodbQueryType {
    fn walk_bson_mut(&mut self, cb: impl FnMut(&mut Bson) -> Result<()>) -> Result<()> {
        match self {
            MongodbQueryType::Find(q) => q.walk_bson_mut(cb),
            MongodbQueryType::InsertMany(q) => q.walk_bson_mut(cb),
            MongodbQueryType::UpdateMany(q) => q.walk_bson_mut(cb),
            MongodbQueryType::DeleteMany(q) => q.walk_bson_mut(cb),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FindQuery {
    pub filter: Option<Document>,
    pub sort: Option<Document>,
    pub skip: Option<u64>,
    pub limit: Option<u64>,
}

impl FindQuery {
    fn walk_bson_mut(&mut self, mut cb: impl FnMut(&mut Bson) -> Result<()>) -> Result<()> {
        if let Some(filter) = self.filter.as_mut() {
            walk_doc(filter, &mut cb)?;
        }

        if let Some(sort) = self.sort.as_mut() {
            walk_doc(sort, &mut cb)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct InsertManyQuery {
    pub docs: Vec<Bson>,
}

impl InsertManyQuery {
    fn walk_bson_mut(&mut self, mut cb: impl FnMut(&mut Bson) -> Result<()>) -> Result<()> {
        for doc in self.docs.iter_mut() {
            walk_bson(doc, &mut cb)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct UpdateManyQuery {
    pub pipeline: Vec<Document>,
    pub filter: Option<Document>,
}

impl UpdateManyQuery {
    fn walk_bson_mut(&mut self, mut cb: impl FnMut(&mut Bson) -> Result<()>) -> Result<()> {
        for doc in self.pipeline.iter_mut() {
            walk_doc(doc, &mut cb)?;
        }

        if let Some(filter) = self.filter.as_mut() {
            walk_doc(filter, &mut cb)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DeleteManyQuery {
    pub filter: Option<Document>,
}

impl DeleteManyQuery {
    fn walk_bson_mut(&mut self, mut cb: impl FnMut(&mut Bson) -> Result<()>) -> Result<()> {
        if let Some(filter) = self.filter.as_mut() {
            walk_doc(filter, &mut cb)?;
        }

        Ok(())
    }
}

fn walk_doc(doc: &mut Document, cb: &mut impl FnMut(&mut Bson) -> Result<()>) -> Result<()> {
    let keys = doc.keys().cloned().collect_vec();

    for key in keys {
        walk_bson(doc.get_mut(key).unwrap(), cb)?;
    }

    Ok(())
}

fn walk_bson(bson: &mut Bson, cb: &mut impl FnMut(&mut Bson) -> Result<()>) -> Result<()> {
    cb(bson)?;

    match bson {
        Bson::Array(v) => {
            for i in v.iter_mut() {
                walk_bson(i, cb)?;
            }
        }
        Bson::Document(d) => walk_doc(d, cb)?,
        _ => {}
    }

    Ok(())
}

/// Mongodb query result
#[derive(Debug)]
pub enum MongodbQueryResult {
    Find(SessionCursor<Document>),
    InsertMany(InsertManyResult),
    UpdateMany(UpdateResult),
    DeleteMany(DeleteResult),
}

/// Mongodb prepared query
pub struct MongodbPreparedQuery {
    /// The mongo client
    client: mongodb::sync::Client,
    /// The mongo session
    sess: Arc<Mutex<ClientSession>>,
    /// The query details
    inner: MongodbQuery,
    /// Buffer for storing query params
    sink: QueryParamSink,
}

impl MongodbPreparedQuery {
    pub(crate) fn new(
        client: mongodb::sync::Client,
        sess: Arc<Mutex<ClientSession>>,
        inner: MongodbQuery,
    ) -> Result<Self> {
        let sink = QueryParamSink::new(inner.params.clone());

        Ok(Self {
            client,
            sess,
            inner,
            sink,
        })
    }

    /// Gets the mongodb query with the placeholders used for query parameters
    /// replaced with the actual values
    fn with_query_params(&self) -> Result<MongodbQuery> {
        let params = self.sink.get_dyn()?;

        let mut query = self.inner.clone();
        query.replace_query_params(params.clone())?;

        Ok(query)
    }

    fn execute(&mut self) -> Result<MongodbQueryResult> {
        let query = self.with_query_params()?;

        let col = self
            .client
            .database(&query.database)
            .collection::<Document>(&query.collection);

        let mut sess = self
            .sess
            .lock()
            .map_err(|_| Error::msg("Failed to lock sess"))?;

        let res = match query.q {
            MongodbQueryType::Find(q) => MongodbQueryResult::Find(
                col.find_with_session(
                    q.filter,
                    Some(
                        FindOptions::builder()
                            .sort(q.sort)
                            .skip(q.skip)
                            .limit(q.limit.map(|i| i as i64))
                            .build(),
                    ),
                    &mut sess,
                )?,
            ),
            MongodbQueryType::InsertMany(q) => {
                let docs = q
                    .docs
                    .into_iter()
                    .map(|d| match d {
                        Bson::Document(d) => Ok(d),
                        _ => bail!(
                            "Failed to insert, expecting BSON Document but found: {:?}",
                            d
                        ),
                    })
                    .collect::<Result<Vec<_>>>()?;

                MongodbQueryResult::InsertMany(col.insert_many_with_session(docs, None, &mut sess)?)
            }
            MongodbQueryType::UpdateMany(q) => {
                MongodbQueryResult::UpdateMany(col.update_many_with_session(
                    q.filter.unwrap_or(Document::new()),
                    q.pipeline,
                    None,
                    &mut sess,
                )?)
            }
            MongodbQueryType::DeleteMany(q) => MongodbQueryResult::DeleteMany(
                col.delete_many_with_session(q.filter.unwrap_or(Document::new()), None, &mut sess)?,
            ),
        };

        Ok(res)
    }
}

impl QueryHandle for MongodbPreparedQuery {
    type TResultSet = MongodbResultSet;

    fn get_structure(&self) -> Result<QueryInputStructure> {
        Ok(self.sink.get_input_structure().clone())
    }

    fn write(&mut self, buff: &[u8]) -> Result<usize> {
        Ok(self.sink.write(buff)?)
    }

    fn restart(&mut self) -> Result<()> {
        self.sink.clear();
        Ok(())
    }

    fn execute_query(&mut self) -> Result<Self::TResultSet> {
        let res = self.execute()?;

        let cursor = match res {
            MongodbQueryResult::Find(c) => Some(c),
            _ => None,
        };

        Ok(MongodbResultSet::new(cursor, Arc::clone(&self.sess)))
    }

    fn execute_modify(&mut self) -> Result<Option<u64>> {
        let res = self.execute()?;

        let affected = match res {
            MongodbQueryResult::Find(_) => None,
            MongodbQueryResult::InsertMany(r) => Some(r.inserted_ids.len() as u64),
            MongodbQueryResult::UpdateMany(r) => Some(r.modified_count),
            MongodbQueryResult::DeleteMany(r) => Some(r.deleted_count),
        };

        Ok(affected)
    }

    fn logged(&self) -> Result<LoggedQuery> {
        Ok(LoggedQuery::new(
            &serde_json::to_string_pretty(&self.with_query_params()?)?,
            vec![],
            None,
        ))
    }
}

#[cfg(test)]
mod tests {
    use ansilo_core::{data::DataType, sqlil};
    use mongodb::bson::doc;

    use pretty_assertions::assert_eq;

    use crate::MongodbQueryCompiler;

    use super::*;

    fn mock_param(id: u32) -> Bson {
        MongodbQueryCompiler::compile_param(&sqlil::Parameter::new(DataType::Null, id)).unwrap()
    }

    #[test]
    fn test_replace_query_param_select() {
        let mut query = MongodbQuery::new(
            "db".into(),
            "col".into(),
            MongodbQueryType::Find(FindQuery {
                filter: Some(
                    doc! { "field": {"$eq": mock_param(1) }, "another": {"$ne": mock_param(2)} },
                ),
                sort: Some(doc! {"foo": mock_param(3)}),
                skip: None,
                limit: None,
            }),
            vec![
                QueryParam::dynamic2(1, DataType::Int32),
                QueryParam::dynamic2(2, DataType::rust_string()),
                QueryParam::dynamic2(3, DataType::Int32),
            ],
        );

        query
            .replace_query_params(
                [
                    (1, DataValue::Int32(123)),
                    (2, DataValue::Utf8String("hello".into())),
                    (3, DataValue::Int32(-1)),
                ]
                .into_iter()
                .collect(),
            )
            .unwrap();

        assert_eq!(
            query,
            MongodbQuery::new(
                "db".into(),
                "col".into(),
                MongodbQueryType::Find(FindQuery {
                    filter: Some(doc! { "field": {"$eq": 123 }, "another": {"$ne": "hello"} },),
                    sort: Some(doc! {"foo": -1}),
                    skip: None,
                    limit: None,
                }),
                vec![
                    QueryParam::dynamic2(1, DataType::Int32),
                    QueryParam::dynamic2(2, DataType::rust_string()),
                    QueryParam::dynamic2(3, DataType::Int32),
                ],
            )
        );
    }
}

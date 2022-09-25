use std::{
    collections::HashMap,
    io::Write,
    mem,
    ops::{Deref, DerefMut},
    pin::Pin,
    sync::{Arc, Mutex},
};

use ansilo_connectors_base::{
    common::{data::QueryParamSink, query::QueryParam},
    interface::{LoggedQuery, QueryHandle, QueryInputStructure},
};
use ansilo_core::{
    data::DataValue,
    err::{Context, Result},
};
use itertools::Itertools;
use mongodb::{
    bson::{Bson, DbPointer, Document},
    options::FindOptions,
    results::{DeleteResult, InsertManyResult, UpdateResult},
    sync::{ClientSession, Cursor, SessionCursor},
};
use rumongodb::{ParamsFromIter, ToSql};
use serde::Serialize;

use crate::{result_set::MongodbResultSet, to_mongodb, val_to_bson, OwnedMongodbRows};

/// Mongodb query
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct MongodbQuery {
    database: String,
    collection: String,
    q: MongodbQueryType,
}

impl MongodbQuery {
    pub fn new(database: String, collection: String, q: MongodbQueryType) -> Self {
        Self {
            database,
            collection,
            q,
        }
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
    filter: Option<Document>,
    sort: Option<Document>,
    skip: Option<u64>,
    limit: Option<u64>,
}

impl FindQuery {
    fn walk_bson_mut(&mut self, cb: impl FnMut(&mut Bson) -> Result<()>) -> Result<()> {
        if let Some(filter) = self.filter.as_mut() {
            walk_doc(filter, cb)?;
        }

        if let Some(sort) = self.sort.as_mut() {
            walk_doc(sort, cb)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct InsertManyQuery {
    docs: Vec<Document>,
}

impl InsertManyQuery {
    fn walk_bson_mut(&mut self, cb: impl FnMut(&mut Bson) -> Result<()>) -> Result<()> {
        for doc in self.docs.iter_mut() {
            walk_doc(doc, cb)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct UpdateManyQuery {
    set: Document,
    filter: Option<Document>,
}

impl UpdateManyQuery {
    fn walk_bson_mut(&mut self, cb: impl FnMut(&mut Bson) -> Result<()>) -> Result<()> {
        walk_doc(&mut self.set, cb)?;

        if let Some(filter) = self.filter.as_mut() {
            walk_doc(filter, cb)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DeleteManyQuery {
    filter: Option<Document>,
}

impl DeleteManyQuery {
    fn walk_bson_mut(&mut self, cb: impl FnMut(&mut Bson) -> Result<()>) -> Result<()> {
        if let Some(filter) = self.filter.as_mut() {
            walk_doc(filter, cb)?;
        }

        Ok(())
    }
}

fn walk_doc(doc: &mut Document, cb: impl FnMut(&mut Bson) -> Result<()>) -> Result<()> {
    let keys = doc.keys().cloned().collect_vec();

    for key in keys {
        walk_bson(doc.get_mut(key).unwrap(), cb)?;
    }

    Ok(())
}

fn walk_bson(bson: &mut Bson, cb: impl FnMut(&mut Bson) -> Result<()>) -> Result<()> {
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
#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum MongodbQueryResult {
    Find(SessionCursor<Bson>),
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
    /// Logged params
    logged_params: Vec<DataValue>,
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
            logged_params: vec![],
        })
    }

    /// Gets the mongodb query with the placeholders used for query parameters
    /// replaced with the actual values
    fn with_query_params(&self) -> Result<MongodbQuery> {
        let params = self.sink.get_dyn()?;

        let mut query = self.inner.clone();

        query.q.walk_bson_mut(|bson| {
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

    fn execute(&mut self) -> Result<MongodbQueryResult> {
        let query = self.with_query_params()?;
        let col = self
            .client
            .database(&query.database)
            .collection::<Document>(&query.collection);
        let mut sess = self.sess.lock().context("Failed to lock sess")?;

        let res = match query.q {
            MongodbQueryType::Find(q) => MongodbQueryResult::Find(
                col.find_with_session(
                    q.filter,
                    Some(
                        FindOptions::builder()
                            .sort(q.sort)
                            .skip(q.skip)
                            .limit(q.limit)
                            .build(),
                    ),
                    &mut sess,
                )?,
            ),
            MongodbQueryType::InsertMany(q) => MongodbQueryResult::InsertMany(
                col.insert_many_with_session(q.docs, None, &mut sess)?,
            ),
            MongodbQueryType::UpdateMany(q) => MongodbQueryResult::UpdateMany(
                col.update_many_with_session(q.filter, q.set, None, &mut sess)?,
            ),
            MongodbQueryType::DeleteMany(q) => MongodbQueryResult::DeleteMany(
                col.delete_many_with_session(q.filter, None, &mut sess)?,
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
        self.logged_params.clear();
        Ok(())
    }

    fn execute_query(&mut self) -> Result<Self::TResultSet> {
        let res = self.execute()?;

        let cursor = match res {
            MongodbQueryResult::Find(c) => Some(c),
            _ => None,
        };

        Ok(MongodbResultSet::new(cursor, Arc::clone(&self.sess))?)
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
            &self.inner.sql,
            self.logged_params
                .iter()
                .map(|val| format!("value={:?}", val))
                .collect(),
            None,
        ))
    }
}

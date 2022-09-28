use std::{
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
    ops::{Deref, DerefMut},
    pin::Pin,
};

use ansilo_core::{
    config::EntityConfig,
    err::{bail, ensure, Result},
    sqlil,
};
use apache_avro::Schema;
use serde::Serialize;

use ansilo_connectors_base::{
    common::{data::QueryParamSink, query::QueryParam},
    interface::{LoggedQuery, QueryHandle, QueryInputStructure},
};

use crate::{data::into_arvo_value, schema::into_arvo_schema};

use super::{ArvoFile, ArvoResultSet};

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ArvoQuery {
    pub(crate) entity: EntityConfig,
    pub(crate) file: ArvoFile,
    pub(crate) q: ArvoQueryType,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum ArvoQueryType {
    /// Vec<(result alias, field_name)>
    ReadAll(Vec<(String, String)>),
    /// Vec<(field_name, field_name)>
    InsertBatch(Vec<(String, sqlil::Parameter)>),
}

impl ArvoQuery {
    pub fn new(entity: EntityConfig, file: ArvoFile, q: ArvoQueryType) -> Self {
        Self { entity, file, q }
    }
}

pub struct ArvoQueryHandle {
    query: ArvoQuery,
    params: QueryParamSink,
}

enum ExecuteResult {
    RowCount(u64),
    Reader(OwnedReader<BufReader<File>>),
}

impl ArvoQueryHandle {
    pub fn new(query: ArvoQuery) -> Self {
        let params = match &query.q {
            ArvoQueryType::ReadAll(_) => vec![],
            ArvoQueryType::InsertBatch(params) => params
                .iter()
                .map(|(_, p)| QueryParam::Dynamic(p.clone()))
                .collect(),
        };

        Self {
            query,
            params: QueryParamSink::new(params),
        }
    }

    fn execute(&mut self) -> Result<ExecuteResult> {
        let file = OpenOptions::new()
            .read(true)
            .create(true)
            .open(self.query.file.path())?;

        let schema = if let Some(schema) = self.query.file.schema().cloned() {
            schema
        } else {
            into_arvo_schema(&self.query.entity)?
        };

        let res = match self.query.q {
            ArvoQueryType::ReadAll(_) => {
                let reader = OwnedReader::new(schema, BufReader::new(file))?;

                ExecuteResult::Reader(reader)
            }
            ArvoQueryType::InsertBatch(_) => {
                let params = self.params.get_all()?;
                let mut writer = apache_avro::Writer::new(&schema, BufWriter::new(file));
                let mut written = 0;

                let fields = match schema.clone() {
                    Schema::Record { fields, .. } => fields,
                    _ => bail!("Unexpected code path"),
                };

                ensure!(
                    params.len() % fields.len() == 0,
                    "Unexpected number of query params"
                );

                for row in params.chunks(fields.len()) {
                    let row = row
                        .iter()
                        .enumerate()
                        .map(|(idx, d)| (fields[idx].name.clone(), into_arvo_value(d.clone())))
                        .collect::<Vec<_>>();

                    let row = apache_avro::types::Value::Record(row);

                    writer.append(row)?;
                    written += 1;
                }

                writer.flush()?;
                ExecuteResult::RowCount(written)
            }
        };

        Ok(res)
    }
}

impl QueryHandle for ArvoQueryHandle {
    type TResultSet = ArvoResultSet;

    fn get_structure(&self) -> Result<QueryInputStructure> {
        Ok(self.params.get_input_structure().clone())
    }

    fn write(&mut self, buff: &[u8]) -> Result<usize> {
        let len = self.params.write(buff)?;
        Ok(len)
    }

    fn restart(&mut self) -> Result<()> {
        self.params.clear();
        Ok(())
    }

    fn execute_query(&mut self) -> Result<ArvoResultSet> {
        Ok(match self.execute()? {
            ExecuteResult::RowCount(_) => ArvoResultSet::empty(),
            ExecuteResult::Reader(r) => ArvoResultSet::new(r, self.query.clone())?,
        })
    }

    fn execute_modify(&mut self) -> Result<Option<u64>> {
        Ok(match self.execute()? {
            ExecuteResult::RowCount(c) => Some(c),
            ExecuteResult::Reader(_) => None,
        })
    }

    fn logged(&self) -> Result<LoggedQuery> {
        Ok(LoggedQuery::new(
            format!("{:?}", self.query),
            self.params
                .get_all()?
                .into_iter()
                .map(|p| format!("value={:?}", p))
                .collect(),
            None,
        ))
    }
}

/// Workaround of lifetime restriction for apache_arvo::Reader
pub struct OwnedReader<T: Read> {
    _schema: Pin<Box<Schema>>,
    inner: apache_avro::Reader<'static, T>,
}

impl<T: Read> OwnedReader<T> {
    fn new(schema: Schema, inner: T) -> Result<Self> {
        // SAFETY: We transmute this reference into a 'static
        // which should be ok as we maintain the validity of this reference
        // for as long as the inner Reader is alive by owning the box in this struct

        let schema = Box::pin(schema);
        let inner = apache_avro::Reader::with_schema(
            unsafe { std::mem::transmute::<&Schema, &'static Schema>(&schema) },
            inner,
        )?;

        Ok(Self { _schema: schema, inner })
    }
}

impl<T: Read> Deref for OwnedReader<T> {
    type Target = apache_avro::Reader<'static, T>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T: Read> DerefMut for OwnedReader<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        todo!()
    }
}

use std::{
    cmp, mem,
    ops::{Deref, DerefMut},
    pin::Pin,
    sync::{Arc, Mutex},
};

use ansilo_connectors_base::{
    common::data::DataWriter,
    interface::{ResultSet, RowStructure},
};
use ansilo_core::{
    data::{DataType, DataValue},
    err::{Context, Result},
};
use mongodb::{
    bson::{Bson, Document},
    sync::{ClientSession, SessionCursor},
};
use rumongodb::types::Value;

use crate::{doc_to_json, from_mongodb, from_mongodb_type, OwnedMongodbStatment};

/// Mongodb result set
pub struct MongodbResultSet {
    /// The cursor for the results
    cursor: SessionCursor<Document>,
    /// The session
    sess: Arc<Mutex<ClientSession>>,
    /// Output buffer
    buf: Vec<u8>,
    /// Finished reading rows
    done: bool,
}

impl MongodbResultSet {
    pub fn new(cursor: SessionCursor<Document>, sess: Arc<Mutex<ClientSession>>) -> Self {
        Self {
            cursor,
            sess,
            buf: vec![],
            done: false,
        }
    }
}

impl ResultSet for MongodbResultSet {
    fn get_structure(&self) -> Result<RowStructure> {
        Ok(RowStructure::new(vec![("doc".into(), DataType::JSON)]))
    }

    fn read(&mut self, buff: &mut [u8]) -> Result<usize> {
        if self.done {
            return Ok(0);
        }

        let mut read = 0;

        loop {
            if !self.buf.is_empty() {
                let new = cmp::min(buff.len() - read, self.buf.len());

                buff[read..(read + new)].copy_from_slice(&self.buf[..new]);
                self.buf.drain(..new);
                read += new;
            }

            if buff.len() == read {
                return Ok(read);
            }

            let mut sess = self.sess.lock().context("Failed to lock sess")?;

            if let Some(doc) = self
                .cursor
                .advance(&mut sess)
                .context("Failed to advance cursor")?
            {
                let doc = self
                    .cursor
                    .deserialize_current()
                    .context("Failed to deserialize document")?;

                let val = DataValue::JSON(serde_json::to_string(&doc_to_json(doc)?)?);

                self.buf
                    .extend_from_slice(DataWriter::to_vec(vals)?.as_slice());
            } else {
                self.done = true;
                return Ok(read);
            }
        }
    }
}

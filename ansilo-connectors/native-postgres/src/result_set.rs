use std::{cmp, ops::DerefMut, pin::Pin, sync::Arc};

use ansilo_connectors_base::{
    common::data::DataWriter,
    interface::{ResultSet, RowStructure},
};
use ansilo_core::{
    data::DataType,
    err::{Context, Result},
};
use ansilo_logging::debug;
use futures_util::TryStreamExt;
use tokio::runtime::Runtime;
use tokio_postgres::{Client, Portal, Row, RowStream};

use crate::{data::from_pg, runtime::runtime, OwnedTransaction};

pub(crate) const BATCH_SIZE: usize = 1000;

/// Postgres result set
pub struct PostgresResultSet<T> {
    /// The postgres tranaction
    transaction: Arc<OwnedTransaction<T>>,
    /// The portal which streams the results
    portal: Portal,
    /// The current row stream
    stream: Option<Pin<Box<RowStream>>>,
    /// The resultant column types
    cols: Vec<(String, DataType)>,
    /// Output buffer
    buf: Vec<u8>,
    /// Finished reading rows
    done: bool,
}

impl<T: DerefMut<Target = Client>> PostgresResultSet<T> {
    pub fn new(
        transaction: Arc<OwnedTransaction<T>>,
        portal: Portal,
        stream: RowStream,
        cols: Vec<(String, DataType)>,
    ) -> Self {
        Self {
            transaction,
            portal,
            stream: Some(Box::pin(stream)),
            cols,
            buf: vec![],
            done: false,
        }
    }
}

impl<T: DerefMut<Target = Client>> ResultSet for PostgresResultSet<T> {
    fn get_structure(&self) -> Result<RowStructure> {
        Ok(RowStructure::new(self.cols.clone()))
    }

    fn read(&mut self, buff: &mut [u8]) -> Result<usize> {
        if self.done {
            return Ok(0);
        }

        let mut read = 0;
        let rt = runtime();

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

            if let Some(row) = self.get_next_row(&rt)? {
                let vals = row
                    .columns()
                    .iter()
                    .enumerate()
                    .map(|(idx, c)| from_pg(&row, idx, c.type_()))
                    .collect::<Result<Vec<_>>>()?;

                self.buf
                    .extend_from_slice(DataWriter::to_vec(vals)?.as_slice());
            } else {
                self.done = true;
                return Ok(read);
            }
        }
    }
}

impl<T: DerefMut<Target = Client>> PostgresResultSet<T> {
    fn get_next_row(&mut self, rt: &Runtime) -> Result<Option<Row>> {
        loop {
            let (new, stream) = if let Some(stream) = self.stream.as_mut() {
                (false, stream)
            } else {
                (true, self.get_next_batch(rt)?)
            };

            if let Some(row) = rt.block_on(stream.try_next())? {
                return Ok(Some(row));
            }

            // Check if empty batch (eof)
            if new {
                return Ok(None);
            } else {
                // Else this batch is empty, get a new one
                self.stream = None;
            }
        }
    }

    pub(crate) fn get_next_batch(&mut self, rt: &Runtime) -> Result<&mut Pin<Box<RowStream>>> {
        if !self.stream.is_some() {
            debug!("Retrieving {BATCH_SIZE} rows");
            self.stream = Some(Box::pin(
                rt.block_on(
                    self.transaction
                        .inner()
                        .as_ref()
                        .context("Transaction closed")?
                        .query_portal_raw(&self.portal, BATCH_SIZE as _),
                )?,
            ));
        }

        Ok(self.stream.as_mut().unwrap())
    }
}

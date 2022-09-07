use std::{cmp, pin::Pin};

use ansilo_connectors_base::{
    common::data::DataWriter,
    interface::{ResultSet, RowStructure},
};
use ansilo_core::{data::DataType, err::Result};
use futures_util::TryStreamExt;
use tokio_postgres::RowStream;

use crate::{data::from_pg, runtime::runtime};

/// Postgres result set
pub struct PostgresResultSet {
    /// The stream of table rows
    stream: Pin<Box<RowStream>>,
    /// The resultant column types
    cols: Vec<(String, DataType)>,
    /// Output buffer
    buf: Vec<u8>,
}

impl PostgresResultSet {
    pub fn new(stream: RowStream, cols: Vec<(String, DataType)>) -> Self {
        Self {
            stream: Box::pin(stream),
            cols,
            buf: vec![],
        }
    }
}

impl ResultSet for PostgresResultSet {
    fn get_structure(&self) -> Result<RowStructure> {
        Ok(RowStructure::new(self.cols.clone()))
    }

    fn read(&mut self, buff: &mut [u8]) -> Result<usize> {
        let mut read = 0;

        loop {
            if !self.buf.is_empty() {
                read = cmp::min(buff.len(), self.buf.len());

                buff[..read].copy_from_slice(&self.buf[..read]);
                self.buf.drain(..read);
            }

            if buff.len() == read {
                return Ok(read);
            }

            let rt = runtime();

            if let Some(row) = rt.block_on(self.stream.try_next())? {
                let vals = row
                    .columns()
                    .iter()
                    .enumerate()
                    .map(|(idx, c)| from_pg(&row, idx, c.type_()))
                    .collect::<Result<Vec<_>>>()?;

                self.buf
                    .extend_from_slice(DataWriter::to_vec(vals)?.as_slice());
            } else {
                return Ok(read);
            }
        }
    }
}

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
    /// Finished reading rows
    done: bool,
}

impl PostgresResultSet {
    pub fn new(stream: RowStream, cols: Vec<(String, DataType)>) -> Self {
        Self {
            stream: Box::pin(stream),
            cols,
            buf: vec![],
            done: false,
        }
    }
}

impl ResultSet for PostgresResultSet {
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
                self.done = true;
                return Ok(read);
            }
        }
    }
}

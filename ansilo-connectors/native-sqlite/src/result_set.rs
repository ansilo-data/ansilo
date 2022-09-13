use std::{
    cmp, mem,
    ops::{Deref, DerefMut},
    pin::Pin,
};

use ansilo_connectors_base::{
    common::data::DataWriter,
    interface::{ResultSet, RowStructure},
};
use ansilo_core::{
    data::DataType,
    err::{Context, Result},
};
use rusqlite::types::Value;

use crate::{from_sqlite, from_sqlite_type, OwnedSqliteStatment};

/// Sqlite result set
pub struct SqliteResultSet {
    /// The stream of table rows
    rows: OwnedSqliteRows,
    /// Column types
    cols: Vec<(String, DataType)>,
    /// Output buffer
    buf: Vec<u8>,
    /// Finished reading rows
    done: bool,
}

impl SqliteResultSet {
    pub(crate) fn new(rows: OwnedSqliteRows) -> Result<Self> {
        let cols = rows
            .stmt
            .columns()
            .into_iter()
            .map(|c| {
                Ok((
                    c.name().to_string(),
                    from_sqlite_type(c.decl_type().unwrap_or("BLOB"))?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            rows,
            cols,
            buf: vec![],
            done: false,
        })
    }
}

impl ResultSet for SqliteResultSet {
    fn get_structure(&self) -> Result<RowStructure> {
        Ok(RowStructure::new(self.cols.clone()))
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

            if let Some(row) = self.rows.rows.next().context("Failed to read row")? {
                let vals = self
                    .cols
                    .iter()
                    .enumerate()
                    .map(|(idx, (_, typ))| {
                        row.get::<_, Value>(idx)
                            .context("Failed to get row value")
                            .and_then(|v| from_sqlite(v, typ))
                    })
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

/// To get around restrictions in the rusqlite api design
/// we have a touch of unsafety here, similar to owning_ref
/// but for our usecase
pub(crate) struct OwnedSqliteRows {
    /// The owned reference to the statement which this Rows reads
    /// from
    stmt: Pin<Box<OwnedSqliteStatment>>,
    /// The rows
    rows: rusqlite::Rows<'static>,
}

impl OwnedSqliteRows {
    pub fn query(stmt: OwnedSqliteStatment, params: impl rusqlite::Params) -> Result<Self> {
        // Box the statement so it has a stable address
        let mut stmt = Box::pin(stmt);
        let rows = stmt.query(params).context("Failed to execute query")?;

        // SAFETY: We maintain a stable reference to the statement
        // through pinning it in this struct
        let rows = unsafe { mem::transmute::<_, rusqlite::Rows<'static>>(rows) };

        Ok(Self { stmt, rows })
    }
}

impl Deref for OwnedSqliteRows {
    type Target = rusqlite::Rows<'static>;

    fn deref(&self) -> &Self::Target {
        &self.rows
    }
}

impl DerefMut for OwnedSqliteRows {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.rows
    }
}

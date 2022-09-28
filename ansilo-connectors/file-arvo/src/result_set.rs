use std::{cmp, collections::HashMap, fs::File, io::BufReader};

use ansilo_connectors_base::{
    common::data::DataWriter,
    interface::{ResultSet, RowStructure},
};
use ansilo_core::err::{bail, Context, Result};
use apache_avro::{types::Value as ArvoValue, Schema};

use crate::{
    data::{from_arvo_type, from_arvo_value},
    ArvoQuery, ArvoQueryType, OwnedReader,
};

/// Arvo result set
pub enum ArvoResultSet {
    Reader {
        /// The arvo reader
        reader: OwnedReader<BufReader<File>>,
        /// Thie inner query
        query: ArvoQuery,
        /// Output buffer
        buf: Vec<u8>,
        /// Finished reading rows
        done: bool,
    },
    Empty,
}

impl ArvoResultSet {
    pub(crate) fn new(reader: OwnedReader<BufReader<File>>, query: ArvoQuery) -> Result<Self> {
        Ok(Self::Reader {
            reader,
            query,
            buf: vec![],
            done: false,
        })
    }

    pub(crate) fn empty() -> Self {
        Self::Empty
    }
}

impl ResultSet for ArvoResultSet {
    fn get_structure(&self) -> Result<RowStructure> {
        match self {
            ArvoResultSet::Reader { reader, query, .. } => {
                let col_field_map = match &query.q {
                    ArvoQueryType::ReadAll(cols) => cols,
                    _ => bail!("Unexpected code path"),
                };

                let fields = match reader.reader_schema() {
                    Some(Schema::Record { fields, .. }) => fields,
                    _ => bail!("Unexpected code path"),
                };

                let mut col_types = vec![];

                for (alias, field) in col_field_map.iter() {
                    let field = fields
                        .iter()
                        .find(|f| &f.name == field)
                        .context("Failed to find field")?;
                    let (r#type, _) = from_arvo_type(&field.schema)?;

                    col_types.push((alias.clone(), r#type));
                }

                Ok(RowStructure::new(col_types))
            }
            ArvoResultSet::Empty => Ok(RowStructure::new(vec![])),
        }
    }

    fn read(&mut self, buff: &mut [u8]) -> Result<usize> {
        match self {
            ArvoResultSet::Reader {
                reader,
                query,
                buf,
                done,
            } => {
                if *done {
                    return Ok(0);
                }

                let mut read = 0;

                loop {
                    if !buf.is_empty() {
                        let new = cmp::min(buff.len() - read, buf.len());

                        buff[read..(read + new)].copy_from_slice(&buf[..new]);
                        buf.drain(..new);
                        read += new;
                    }

                    if buff.len() == read {
                        return Ok(read);
                    }

                    if let Some(row) = reader.next() {
                        let row = match row? {
                            ArvoValue::Record(fields) => {
                                fields.into_iter().collect::<HashMap<_, _>>()
                            }
                            row => bail!("Unexpected arvo value: {:?}", row),
                        };
                        let mut vals = vec![];

                        let col_field_map = match &query.q {
                            ArvoQueryType::ReadAll(cols) => cols,
                            _ => bail!("Unexpected code path"),
                        };

                        for (_, field) in col_field_map.iter() {
                            vals.push(from_arvo_value(
                                row.get(field).cloned().unwrap_or(ArvoValue::Null),
                            )?);
                        }

                        buf.extend_from_slice(DataWriter::to_vec(vals)?.as_slice());
                    } else {
                        *done = true;
                        return Ok(read);
                    }
                }
            }
            ArvoResultSet::Empty => return Ok(0),
        }
    }
}

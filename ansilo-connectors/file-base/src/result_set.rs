use std::cmp;

use ansilo_connectors_base::{
    common::data::DataWriter,
    interface::{ResultSet, RowStructure},
};
use ansilo_core::err::{Context, Result};

use crate::{FileReader, FileStructure, ReadColumnsQuery};

/// File result set
pub enum FileResultSet<R: FileReader> {
    Reader {
        /// The file structure
        structure: FileStructure,
        /// The file reader
        reader: R,
        /// Thie inner query
        query: ReadColumnsQuery,
        /// Mapping between columns in the file and the columns in the resulting row
        /// Vec<(file_col_idx, res_col_idx)>
        col_map: Vec<(usize, usize)>,
        /// Output buffer
        buf: Vec<u8>,
        /// Finished reading rows
        done: bool,
    },
    Empty,
}

impl<R: FileReader> FileResultSet<R> {
    pub(crate) fn new(
        structure: FileStructure,
        reader: R,
        query: ReadColumnsQuery,
    ) -> Result<Self> {
        let mut col_map = vec![];

        for (output_idx, (_, column)) in query.cols.iter().enumerate() {
            let file_col_idx = structure
                .cols
                .iter()
                .position(|f| &f.name == column)
                .with_context(|| format!("Failed to find column '{}' in file", column))?;

            col_map.push((output_idx, file_col_idx));
        }

        Ok(Self::Reader {
            structure,
            reader,
            col_map,
            query,
            buf: vec![],
            done: false,
        })
    }

    pub(crate) fn empty() -> Self {
        Self::Empty
    }
}

impl<R: FileReader> ResultSet for FileResultSet<R> {
    fn get_structure(&self) -> Result<RowStructure> {
        match self {
            FileResultSet::Reader {
                structure,
                query,
                col_map,
                ..
            } => {
                let mut col_types = vec![];

                for (output_idx, file_idx) in col_map.iter().cloned() {
                    let (alias, _) = &query.cols[output_idx];
                    let r#type = &structure.cols[file_idx].r#type;

                    col_types.push((alias.clone(), r#type.clone()));
                }

                Ok(RowStructure::new(col_types))
            }
            FileResultSet::Empty => Ok(RowStructure::new(vec![])),
        }
    }

    fn read(&mut self, buff: &mut [u8]) -> Result<usize> {
        match self {
            FileResultSet::Reader {
                reader,
                col_map,
                buf,
                done,
                ..
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

                    if let Some(row) = reader.read_row()? {
                        let mut output = vec![];

                        for (_, file_idx) in col_map.iter().cloned() {
                            output.push(row[file_idx].clone());
                        }

                        buf.extend_from_slice(DataWriter::to_vec(output)?.as_slice());
                    } else {
                        *done = true;
                        return Ok(read);
                    }
                }
            }
            FileResultSet::Empty => return Ok(0),
        }
    }
}

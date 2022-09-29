use std::{collections::HashMap, io::Write, marker::PhantomData, path::PathBuf, sync::Arc};

use ansilo_core::{
    config::EntityConfig,
    data::DataValue,
    err::{bail, ensure, Context, Result},
    sqlil,
};
use serde::Serialize;

use ansilo_connectors_base::{
    common::{data::QueryParamSink, query::QueryParam},
    interface::{LoggedQuery, QueryHandle, QueryInputStructure},
};

use crate::{FileIO, FileResultSet, FileStructure, FileWriter};

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FileQuery {
    /// The entity config
    pub entity: EntityConfig,
    /// The path to the file
    pub file: PathBuf,
    /// The type of query
    pub q: FileQueryType,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum FileQueryType {
    ReadColumns(ReadColumnsQuery),
    InsertRows(InsertRowsQuery),
    Truncate,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ReadColumnsQuery {
    /// Vec<(result_alias, field_name)>
    pub cols: Vec<(String, String)>,
}

impl ReadColumnsQuery {
    pub fn new(cols: Vec<(String, String)>) -> Self {
        Self { cols }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct InsertRowsQuery {
    /// Vec<col_name>
    pub cols: Vec<String>,
    /// Vec<query_param> (can be longer that cols if bulk insert)
    pub params: Vec<sqlil::Parameter>,
}

impl InsertRowsQuery {
    pub fn new(cols: Vec<String>, params: Vec<sqlil::Parameter>) -> Self {
        Self { cols, params }
    }
}

impl FileQuery {
    pub fn new(entity: EntityConfig, file: PathBuf, q: FileQueryType) -> Self {
        Self { entity, file, q }
    }
}

pub struct FileQueryHandle<F: FileIO> {
    conf: Arc<F::Conf>,
    query: FileQuery,
    structure: FileStructure,
    params: QueryParamSink,
    _io: PhantomData<F>,
}

enum ExecuteResult<F: FileIO> {
    Ok,
    RowCount(u64),
    ResultSet(FileResultSet<F::Reader>),
}

impl<F: FileIO> FileQueryHandle<F> {
    pub fn new(conf: Arc<F::Conf>, structure: FileStructure, query: FileQuery) -> Result<Self> {
        let params = match &query.q {
            FileQueryType::ReadColumns(_) | FileQueryType::Truncate => vec![],
            FileQueryType::InsertRows(insert) => insert
                .params
                .iter()
                .map(|p| QueryParam::Dynamic(p.clone()))
                .collect::<Vec<_>>(),
        };

        Ok(Self {
            conf,
            query,
            structure,
            params: QueryParamSink::new(params),
            _io: PhantomData,
        })
    }

    fn execute(&mut self) -> Result<ExecuteResult<F>> {
        let path = &self.query.file.as_path();

        let res = match &self.query.q {
            FileQueryType::ReadColumns(q) => {
                let reader = F::reader(&self.conf, &self.structure, path)
                    .context("Failed to create reader")?;
                let result_set = FileResultSet::new(self.structure.clone(), reader, q.clone())?;

                ExecuteResult::ResultSet(result_set)
            }
            FileQueryType::InsertRows(insert) => {
                let params = self.params.get_all()?;
                let mut writer = F::writer(&self.conf, &self.structure, path)
                    .context("Failed to create writer")?;
                let mut rows_written = 0;

                ensure!(
                    params.len() % insert.cols.len() == 0,
                    "Unexpected number of query params"
                );

                for row in params.chunks(insert.cols.len()) {
                    let row = row
                        .iter()
                        .enumerate()
                        .map(|(idx, d)| (insert.cols[idx].as_str(), d))
                        .collect::<HashMap<_, _>>();
                    let mut row_vec = vec![];

                    for col in self.structure.cols.iter() {
                        if let Some(val) = row.get(&col.name.as_str()) {
                            row_vec.push((*val).clone());
                        }
                        // If not all columns supplied, fill remaining with nulls
                        else if col.nullable {
                            row_vec.push(DataValue::Null);
                        } else {
                            bail!(
                                "Insert column '{}' must be supplied for non-null column",
                                col.name
                            )
                        }
                    }

                    writer.write_row(row_vec)?;
                    rows_written += 1;
                }

                writer.flush()?;
                ExecuteResult::RowCount(rows_written)
            }
            FileQueryType::Truncate => {
                F::truncate(&self.conf, &self.structure, self.query.file.as_path())
                    .context("Failed to truncate file")?;
                ExecuteResult::Ok
            }
        };

        Ok(res)
    }
}

impl<F: FileIO> QueryHandle for FileQueryHandle<F> {
    type TResultSet = FileResultSet<F::Reader>;

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

    fn execute_query(&mut self) -> Result<FileResultSet<F::Reader>> {
        Ok(match self.execute()? {
            ExecuteResult::ResultSet(r) => r,
            ExecuteResult::RowCount(_) => FileResultSet::empty(),
            ExecuteResult::Ok => FileResultSet::empty(),
        })
    }

    fn execute_modify(&mut self) -> Result<Option<u64>> {
        Ok(match self.execute()? {
            ExecuteResult::RowCount(c) => Some(c),
            ExecuteResult::ResultSet(_) => None,
            ExecuteResult::Ok => None,
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

#[cfg(test)]
mod tests {
    use crate::{
        test::{MockConfig, MockIO, MockReader, MockWriter},
        FileColumn,
    };

    use super::*;

    use ansilo_connectors_base::interface::{ResultSet, RowStructure};
    use ansilo_core::{config::EntitySourceConfig, data::DataType};
    use pretty_assertions::assert_eq;

    fn mock_conf(reader: Option<MockReader>, writer: Option<MockWriter>) -> Arc<MockConfig> {
        Arc::new(MockConfig {
            path: "/unused".into(),
            extension: None,
            mock_structure: Default::default(),
            reader,
            writer,
        })
    }

    #[test]
    fn test_read_empty_rows() {
        let conf = mock_conf(Some(MockReader::new(vec![])), None);
        let mut query = FileQueryHandle::<MockIO>::new(
            conf,
            FileStructure::new(
                vec![FileColumn::new("col".into(), DataType::Int32, true, None)],
                None,
            ),
            FileQuery::new(
                EntityConfig::minimal("unused", vec![], EntitySourceConfig::minimal("")),
                "/unused".into(),
                FileQueryType::ReadColumns(ReadColumnsQuery::new(vec![(
                    "alias".into(),
                    "col".into(),
                )])),
            ),
        )
        .unwrap();

        assert_eq!(
            query.get_structure().unwrap(),
            QueryInputStructure::new(vec![])
        );

        let mut result_set = query.execute_query().unwrap().reader().unwrap();

        assert_eq!(
            result_set.get_structure(),
            &RowStructure::new(vec![("alias".into(), DataType::Int32)]),
        );

        assert_eq!(result_set.read_row_vec().unwrap(), None);
    }

    #[test]
    fn test_read_rows() {
        let conf = mock_conf(
            Some(MockReader::new(vec![
                vec![DataValue::Int32(1)],
                vec![DataValue::Int32(2)],
            ])),
            None,
        );
        let mut query = FileQueryHandle::<MockIO>::new(
            conf,
            FileStructure::new(
                vec![FileColumn::new("col".into(), DataType::Int32, true, None)],
                None,
            ),
            FileQuery::new(
                EntityConfig::minimal("unused", vec![], EntitySourceConfig::minimal("")),
                "/unused".into(),
                FileQueryType::ReadColumns(ReadColumnsQuery::new(vec![(
                    "alias".into(),
                    "col".into(),
                )])),
            ),
        )
        .unwrap();

        let mut result_set = query.execute_query().unwrap().reader().unwrap();

        assert_eq!(
            result_set.read_row_vec().unwrap(),
            Some(vec![DataValue::Int32(1)])
        );
        assert_eq!(
            result_set.read_row_vec().unwrap(),
            Some(vec![DataValue::Int32(2)])
        );
        assert_eq!(result_set.read_row_vec().unwrap(), None);
    }

    #[test]
    fn test_read_row_filter_columns() {
        let conf = mock_conf(
            Some(MockReader::new(vec![
                vec![
                    DataValue::Int32(11),
                    DataValue::Int32(12),
                    DataValue::Int32(13),
                ],
                vec![
                    DataValue::Int32(21),
                    DataValue::Int32(22),
                    DataValue::Int32(23),
                ],
            ])),
            None,
        );
        let mut query = FileQueryHandle::<MockIO>::new(
            conf,
            FileStructure::new(
                vec![
                    FileColumn::new("col1".into(), DataType::Int32, true, None),
                    FileColumn::new("col2".into(), DataType::Int32, true, None),
                    FileColumn::new("col3".into(), DataType::Int32, true, None),
                ],
                None,
            ),
            FileQuery::new(
                EntityConfig::minimal("unused", vec![], EntitySourceConfig::minimal("")),
                "/unused".into(),
                FileQueryType::ReadColumns(ReadColumnsQuery::new(vec![
                    ("alias1a".into(), "col1".into()),
                    ("alias1b".into(), "col1".into()),
                    ("alias3a".into(), "col3".into()),
                ])),
            ),
        )
        .unwrap();

        let mut result_set = query.execute_query().unwrap().reader().unwrap();

        assert_eq!(
            result_set.read_row_vec().unwrap(),
            Some(vec![
                DataValue::Int32(11),
                DataValue::Int32(11),
                DataValue::Int32(13)
            ])
        );
        assert_eq!(
            result_set.read_row_vec().unwrap(),
            Some(vec![
                DataValue::Int32(21),
                DataValue::Int32(21),
                DataValue::Int32(23)
            ])
        );
        assert_eq!(result_set.read_row_vec().unwrap(), None);
    }

    #[test]
    fn test_write_row() {
        let mock = MockWriter::new();
        let conf = mock_conf(None, Some(mock.clone()));
        let query = FileQueryHandle::<MockIO>::new(
            conf,
            FileStructure::new(
                vec![
                    FileColumn::new("col1".into(), DataType::Int32, true, None),
                    FileColumn::new("col2".into(), DataType::Int32, true, None),
                ],
                None,
            ),
            FileQuery::new(
                EntityConfig::minimal("unused", vec![], EntitySourceConfig::minimal("")),
                "/unused".into(),
                FileQueryType::InsertRows(InsertRowsQuery::new(
                    vec!["col1".into(), "col2".into()],
                    vec![
                        sqlil::Parameter::new(DataType::Int32, 1),
                        sqlil::Parameter::new(DataType::Int32, 2),
                    ],
                )),
            ),
        )
        .unwrap();

        assert_eq!(
            query.get_structure().unwrap(),
            QueryInputStructure::new(vec![(1, DataType::Int32), (2, DataType::Int32),])
        );

        let mut writer = query.writer().unwrap();
        writer
            .write_all([DataValue::Int32(1), DataValue::Int32(2)].into_iter())
            .unwrap();
        let rows = writer.inner().unwrap().execute_modify().unwrap();

        assert_eq!(rows, Some(1));
        assert_eq!(
            mock.rows(),
            vec![vec![DataValue::Int32(1), DataValue::Int32(2)]]
        );
    }

    #[test]
    fn test_write_multiple_rows() {
        let mock = MockWriter::new();
        let conf = mock_conf(None, Some(mock.clone()));
        let query = FileQueryHandle::<MockIO>::new(
            conf,
            FileStructure::new(
                vec![
                    FileColumn::new("col1".into(), DataType::Int32, true, None),
                    FileColumn::new("col2".into(), DataType::Int32, true, None),
                ],
                None,
            ),
            FileQuery::new(
                EntityConfig::minimal("unused", vec![], EntitySourceConfig::minimal("")),
                "/unused".into(),
                FileQueryType::InsertRows(InsertRowsQuery::new(
                    vec!["col1".into(), "col2".into()],
                    vec![
                        sqlil::Parameter::new(DataType::Int32, 1),
                        sqlil::Parameter::new(DataType::Int32, 2),
                        sqlil::Parameter::new(DataType::Int32, 3),
                        sqlil::Parameter::new(DataType::Int32, 4),
                    ],
                )),
            ),
        )
        .unwrap();

        assert_eq!(
            query.get_structure().unwrap(),
            QueryInputStructure::new(vec![
                (1, DataType::Int32),
                (2, DataType::Int32),
                (3, DataType::Int32),
                (4, DataType::Int32),
            ])
        );

        let mut writer = query.writer().unwrap();
        writer
            .write_all(
                [
                    DataValue::Int32(1),
                    DataValue::Int32(2),
                    DataValue::Int32(3),
                    DataValue::Int32(4),
                ]
                .into_iter(),
            )
            .unwrap();
        let rows = writer.inner().unwrap().execute_modify().unwrap();

        assert_eq!(rows, Some(2));
        assert_eq!(
            mock.rows(),
            vec![
                vec![DataValue::Int32(1), DataValue::Int32(2)],
                vec![DataValue::Int32(3), DataValue::Int32(4)]
            ]
        );
    }

    #[test]
    fn test_write_default_nullable_column() {
        let mock = MockWriter::new();
        let conf = mock_conf(None, Some(mock.clone()));
        let query = FileQueryHandle::<MockIO>::new(
            conf,
            FileStructure::new(
                vec![
                    FileColumn::new("col1".into(), DataType::Int32, true, None),
                    FileColumn::new("col2".into(), DataType::Int32, true, None),
                ],
                None,
            ),
            FileQuery::new(
                EntityConfig::minimal("unused", vec![], EntitySourceConfig::minimal("")),
                "/unused".into(),
                FileQueryType::InsertRows(InsertRowsQuery::new(
                    vec!["col1".into()],
                    vec![sqlil::Parameter::new(DataType::Int32, 1)],
                )),
            ),
        )
        .unwrap();

        assert_eq!(
            query.get_structure().unwrap(),
            QueryInputStructure::new(vec![(1, DataType::Int32),])
        );

        let mut writer = query.writer().unwrap();
        writer.write_all([DataValue::Int32(1)].into_iter()).unwrap();
        let rows = writer.inner().unwrap().execute_modify().unwrap();

        assert_eq!(rows, Some(1));
        assert_eq!(
            mock.rows(),
            vec![vec![DataValue::Int32(1), DataValue::Null],]
        );
    }

    #[test]
    fn test_write_default_throws_when_not_null_column_is_not_supplied() {
        let mock = MockWriter::new();
        let conf = mock_conf(None, Some(mock.clone()));

        let query = FileQueryHandle::<MockIO>::new(
            conf,
            FileStructure::new(
                vec![
                    FileColumn::new("col1".into(), DataType::Int32, true, None),
                    FileColumn::new("col2".into(), DataType::Int32, false, None),
                ],
                None,
            ),
            FileQuery::new(
                EntityConfig::minimal("unused", vec![], EntitySourceConfig::minimal("")),
                "/unused".into(),
                FileQueryType::InsertRows(InsertRowsQuery::new(
                    vec!["col1".into()],
                    vec![sqlil::Parameter::new(DataType::Int32, 1)],
                )),
            ),
        )
        .unwrap();

        let mut writer = query.writer().unwrap();
        writer.write_all([DataValue::Int32(1)].into_iter()).unwrap();
        writer.inner().unwrap().execute_modify().unwrap_err();
    }

    #[test]
    fn test_truncate() {
        let mock = MockWriter::new();
        let conf = mock_conf(None, Some(mock.clone()));

        let mut query = FileQueryHandle::<MockIO>::new(
            conf,
            FileStructure::new(vec![], None),
            FileQuery::new(
                EntityConfig::minimal("unused", vec![], EntitySourceConfig::minimal("")),
                "/unused".into(),
                FileQueryType::Truncate,
            ),
        )
        .unwrap();

        assert_eq!(query.execute_modify().unwrap(), None);
    }
}

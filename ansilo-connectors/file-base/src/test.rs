use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use ansilo_core::{
    data::DataValue,
    err::{Context, Result},
};

use crate::{FileConfig, FileConnection, FileIO, FileReader, FileStructure, FileWriter};

#[derive(Clone)]
pub struct MockIO;

#[derive(Clone)]
pub struct MockConfig {
    pub path: PathBuf,
    pub extension: Option<&'static str>,
    pub mock_structure: HashMap<PathBuf, FileStructure>,
    pub reader: Option<MockReader>,
    pub writer: Option<MockWriter>,
}

#[derive(Clone)]
pub struct MockReader {
    pub rows: Arc<Mutex<(usize, Vec<Vec<DataValue>>)>>,
}

#[derive(Clone)]
pub struct MockWriter {
    pub rows: Arc<Mutex<Vec<Vec<DataValue>>>>,
}

#[allow(unused)]
impl FileIO for MockIO {
    type Conf = MockConfig;
    type Reader = MockReader;
    type Writer = MockWriter;

    fn get_structure(conf: &Self::Conf, path: &Path) -> Result<FileStructure> {
        conf.mock_structure
            .get(&path.to_path_buf())
            .context("Unknown path")
            .cloned()
    }

    fn estimate_row_count(conf: &Self::Conf, path: &Path) -> Result<Option<u64>> {
        unimplemented!()
    }

    fn get_extension(conf: &Self::Conf) -> Option<&'static str> {
        conf.extension.clone()
    }

    fn reader(conf: &Self::Conf, structure: &FileStructure, path: &Path) -> Result<Self::Reader> {
        conf.reader.clone().context("reader")
    }

    fn writer(conf: &Self::Conf, structure: &FileStructure, path: &Path) -> Result<Self::Writer> {
        conf.writer.clone().context("writer")
    }

    fn truncate(conf: &Self::Conf, structure: &FileStructure, path: &Path) -> Result<()> {
        Ok(())
    }
}

impl FileConfig for MockConfig {
    fn get_path(&self) -> &Path {
        self.path.as_path()
    }
}

impl MockConfig {
    pub fn con(&self) -> FileConnection<MockIO> {
        FileConnection::new(Arc::new(self.clone()))
    }
}

impl MockReader {
    pub fn new(rows: Vec<Vec<DataValue>>) -> Self {
        Self {
            rows: Arc::new(Mutex::new((0, rows))),
        }
    }
}

impl FileReader for MockReader {
    fn read_row(&mut self) -> Result<Option<Vec<DataValue>>> {
        let mut state = self.rows.lock().unwrap();
        let (idx, rows) = &mut *state;

        let row = rows.get(*idx);
        *idx += 1;

        Ok(row.cloned())
    }
}

impl MockWriter {
    pub fn new() -> Self {
        Self {
            rows: Arc::new(Mutex::new(vec![])),
        }
    }

    pub fn rows(&self) -> Vec<Vec<DataValue>> {
        let rows = self.rows.lock().unwrap();

        rows.clone()
    }
}

impl FileWriter for MockWriter {
    fn write_row(&mut self, row: Vec<DataValue>) -> Result<()> {
        let mut rows = self.rows.lock().unwrap();

        rows.push(row);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

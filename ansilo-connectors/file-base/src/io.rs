use std::path::Path;

use ansilo_connectors_base::interface::RowStructure;
use ansilo_core::{
    config::EntityConfig,
    data::{DataType, DataValue},
    err::Result,
};

use crate::FileConfig;

/// Trait for reading and writing to a file
pub trait FileIO: Sized + Send + Sync + Clone + 'static {
    type Conf: FileConfig;
    type Reader: FileReader;
    type Writer: FileWriter;

    /// Gets the structure of the file
    fn get_structure(conf: &Self::Conf, path: &Path) -> Result<FileStructure>;

    /// Estimates the number of rows in the file
    fn estimate_row_count(conf: &Self::Conf, path: &Path) -> Result<Option<u64>>;

    /// Gets the extension of the file
    fn get_extension(conf: &Self::Conf) -> Option<&'static str>;

    /// Whether the connector supports reading
    #[allow(unused)]
    fn supports_reading(conf: &Self::Conf, path: &Path) -> Result<bool> {
        Ok(true)
    }

    /// Gets a file reader
    fn reader(conf: &Self::Conf, structure: &FileStructure, path: &Path) -> Result<Self::Reader>;

    /// Whether the connector supports writing
    #[allow(unused)]
    fn supports_writing(conf: &Self::Conf, path: &Path) -> Result<bool> {
        Ok(true)
    }

    /// Gets a file writer, the structure is supplied
    /// so the file can be created with the supplied structure if it does not exist
    fn writer(conf: &Self::Conf, structure: &FileStructure, path: &Path) -> Result<Self::Writer>;

    /// Whether the connector supports truncating the file
    #[allow(unused)]
    fn supports_truncating(conf: &Self::Conf, path: &Path) -> Result<bool> {
        Ok(true)
    }

    /// Truncates the file at the supplied path
    fn truncate(conf: &Self::Conf, structure: &FileStructure, path: &Path) -> Result<()>;
}

/// Trait for reading records from a file
pub trait FileReader {
    /// Reads a record from the underlying file
    /// Returns None if there are no more records
    fn read_row(&mut self) -> Result<Option<Vec<DataValue>>>;
}

/// Trait for writing records to a file
pub trait FileWriter {
    /// Writes a record to the underlying file
    fn write_row(&mut self, row: Vec<DataValue>) -> Result<()>;
    /// Flushes any buffers to the file
    fn flush(&mut self) -> Result<()>;
}

/// The structure of a file
#[derive(Debug, Clone, PartialEq)]
pub struct FileStructure {
    /// The list of named columns in the file with their corrosponding data types
    pub cols: Vec<FileColumn>,
    /// The description of the file, if any
    pub desc: Option<String>,
}

impl From<&EntityConfig> for FileStructure {
    fn from(e: &EntityConfig) -> Self {
        Self::new(
            e.attributes
                .iter()
                .map(|a| {
                    FileColumn::new(
                        a.id.clone(),
                        a.r#type.clone(),
                        a.nullable,
                        a.description.clone(),
                    )
                })
                .collect(),
            e.description.clone(),
        )
    }
}

impl FileStructure {
    pub fn new(cols: Vec<FileColumn>, desc: Option<String>) -> Self {
        Self { cols, desc }
    }
}

impl Into<RowStructure> for FileStructure {
    fn into(self) -> RowStructure {
        RowStructure::new(self.cols.into_iter().map(|c| (c.name, c.r#type)).collect())
    }
}

/// Structure of a file column
#[derive(Debug, Clone, PartialEq)]
pub struct FileColumn {
    /// The name of the column
    pub name: String,
    /// The type of the column
    pub r#type: DataType,
    /// Whether the type is nullable
    pub nullable: bool,
    /// The description of the column, if any
    pub desc: Option<String>,
}

impl FileColumn {
    pub fn new(name: String, r#type: DataType, nullable: bool, desc: Option<String>) -> Self {
        Self {
            name,
            r#type,
            nullable,
            desc,
        }
    }
}

/// Struct for impls which dont support reading
pub struct NullReader;

#[allow(unused)]
impl FileReader for NullReader {
    fn read_row(&mut self) -> Result<Option<Vec<DataValue>>> {
        unimplemented!()
    }
}

/// Struct for impls which dont support writing
pub struct NullWriter;

#[allow(unused)]
impl FileWriter for NullWriter {
    fn write_row(&mut self, row: Vec<DataValue>) -> Result<()> {
        unimplemented!()
    }

    fn flush(&mut self) -> Result<()> {
        unimplemented!()
    }
}

use ansilo_core::{
    data::DataType,
    err::Result,
};
use bincode::{Decode, Encode};


/// A result set from an executed query
pub trait ResultSet {
    /// Gets the row structure of the result set
    fn get_structure(&self) -> Result<RowStructure>;

    /// Reads row data from the result set into the supplied slice
    /// Returns the number of bytes read of 0 if no bytes are left to read
    fn read(&mut self, buff: &mut [u8]) -> Result<usize>;
}

/// The structure of a row
#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct RowStructure {
    /// The list of named columns in the row with their corrosponding data types
    pub cols: Vec<(String, DataType)>,
}

impl RowStructure {
    pub fn new(cols: Vec<(String, DataType)>) -> Self {
        Self { cols }
    }

    pub fn types(&self) -> Vec<DataType> {
        self.cols.iter().map(|i| i.1.clone()).collect()
    }
}

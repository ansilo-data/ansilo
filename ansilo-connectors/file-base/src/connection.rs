use std::{marker::PhantomData, sync::Arc};

use ansilo_core::{auth::AuthContext, err::Result};

use ansilo_connectors_base::interface::{Connection, ConnectionPool, QueryHandle};

use crate::{FileIO, FileQuery, FileQueryHandle, FileResultSet, FileStructure};

#[derive(Clone)]
pub struct FileConnectionUnpool<F: FileIO> {
    conf: Arc<F::Conf>,
}

impl<F: FileIO> FileConnectionUnpool<F> {
    pub fn new(conf: F::Conf) -> Self {
        Self {
            conf: Arc::new(conf),
        }
    }
}

impl<F: FileIO> ConnectionPool for FileConnectionUnpool<F> {
    type TConnection = FileConnection<F>;

    fn acquire(&mut self, _auth: Option<&AuthContext>) -> Result<Self::TConnection> {
        Ok(FileConnection::new(Arc::clone(&self.conf)))
    }
}

#[derive(Clone)]
pub struct FileConnection<F: FileIO> {
    conf: Arc<F::Conf>,
    _io: PhantomData<F>,
}

impl<F: FileIO> FileConnection<F> {
    pub fn new(conf: Arc<F::Conf>) -> Self {
        Self {
            conf,
            _io: PhantomData,
        }
    }

    pub fn conf(&self) -> &F::Conf {
        self.conf.as_ref()
    }

    pub fn execute_query(&mut self, query: FileQuery) -> Result<FileResultSet<F::Reader>> {
        self.prepare(query)?.execute_query()
    }

    pub fn execute_modify(&mut self, query: FileQuery) -> Result<Option<u64>> {
        self.prepare(query)?.execute_modify()
    }
}

impl<F: FileIO> Connection for FileConnection<F> {
    type TQuery = FileQuery;
    type TQueryHandle = FileQueryHandle<F>;
    type TTransactionManager = ();

    fn prepare(&mut self, query: Self::TQuery) -> Result<Self::TQueryHandle> {
        let structure = if query.file.try_exists()? && query.file.metadata()?.len() > 0 {
            F::get_structure(&self.conf, query.file.as_path())?
        } else {
            FileStructure::from(&query.entity)
        };

        FileQueryHandle::<F>::new(Arc::clone(&self.conf), structure, query)
    }

    fn transaction_manager(&mut self) -> Option<&mut Self::TTransactionManager> {
        None
    }
}

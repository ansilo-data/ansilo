use std::{marker::PhantomData, sync::Arc};

use ansilo_core::{auth::AuthContext, err::Result};

use ansilo_connectors_base::interface::{Connection, ConnectionPool};

use crate::{FileIO, FileQuery, FileQueryHandle};

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
}

impl<F: FileIO> Connection for FileConnection<F> {
    type TQuery = FileQuery;
    type TQueryHandle = FileQueryHandle<F>;
    type TTransactionManager = ();

    fn prepare(&mut self, query: Self::TQuery) -> Result<Self::TQueryHandle> {
        let structure = F::get_structure(&self.conf, query.file.as_path())?;

        FileQueryHandle::<F>::new(Arc::clone(&self.conf), structure, query)
    }

    fn transaction_manager(&mut self) -> Option<&mut Self::TTransactionManager> {
        None
    }
}

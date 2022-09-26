use std::sync::{Arc, Mutex, MutexGuard};

use ansilo_connectors_base::interface::{Connection, TransactionManager};
use ansilo_core::err::{Context, Error, Result};
use mongodb::sync::ClientSession;

use crate::{MongodbPreparedQuery, MongodbQuery};

/// Connection to a mongodb database
pub struct MongodbConnection {
    /// The inner connection
    client: mongodb::sync::Client,
    /// The client session
    sess: Arc<Mutex<ClientSession>>,
    /// Whether we are in a transaction
    trans: bool,
}

impl MongodbConnection {
    pub fn new(client: mongodb::sync::Client, sess: ClientSession) -> Self {
        Self {
            client,
            sess: Arc::new(Mutex::new(sess)),
            trans: false,
        }
    }

    pub fn client<'a>(&'a self) -> &'a mongodb::sync::Client {
        &self.client
    }
}

impl Connection for MongodbConnection {
    type TQuery = MongodbQuery;
    type TQueryHandle = MongodbPreparedQuery;
    type TTransactionManager = Self;

    fn prepare(&mut self, query: Self::TQuery) -> Result<Self::TQueryHandle> {
        Ok(MongodbPreparedQuery::new(
            self.client.clone(),
            Arc::clone(&self.sess),
            query,
        )?)
    }

    fn transaction_manager(&mut self) -> Option<&mut Self::TTransactionManager> {
        Some(self)
    }
}

impl TransactionManager for MongodbConnection {
    fn is_in_transaction(&mut self) -> Result<bool> {
        Ok(self.trans)
    }

    fn begin_transaction(&mut self) -> Result<()> {
        self.lock_sess()?
            .start_transaction(None)
            .context("Failed to begin transaction")?;
        self.trans = true;
        Ok(())
    }

    fn rollback_transaction(&mut self) -> Result<()> {
        self.lock_sess()?
            .abort_transaction()
            .context("Failed to abort transaction")?;
        self.trans = false;
        Ok(())
    }

    fn commit_transaction(&mut self) -> Result<()> {
        self.lock_sess()?
            .commit_transaction()
            .context("Failed to commit transaction")?;
        self.trans = false;
        Ok(())
    }
}

impl MongodbConnection {
    fn lock_sess<'a>(&'a self) -> Result<MutexGuard<'a, ClientSession>> {
        let sess = self
            .sess
            .lock()
            .map_err(|_| Error::msg("Failed to lock sess"))?;

        Ok(sess)
    }
}

use ansilo_connectors_base::interface::ConnectionPool;
use ansilo_core::{
    auth::AuthContext,
    err::{Context, Result},
};
use mongodb::options::ClientOptions;

use crate::{conf::MongodbConnectionConfig, MongodbConnection};

/// We do not require currently pool connections for mongodb
/// It may be worthwhile at some point but not now.
#[derive(Clone)]
pub struct MongodbConnectionUnpool {
    pub(crate) conf: MongodbConnectionConfig,
}

impl MongodbConnectionUnpool {
    pub fn new(conf: MongodbConnectionConfig) -> Self {
        Self { conf }
    }
}

impl ConnectionPool for MongodbConnectionUnpool {
    type TConnection = MongodbConnection;

    fn acquire(&mut self, _auth: Option<&AuthContext>) -> Result<Self::TConnection> {
        let opts =
            ClientOptions::parse(&self.conf.url).context("Failed to parse connection string")?;
        let con =
            mongodb::sync::Client::with_options(opts).context("Failed to connect to mongodb")?;

        let sess = con.start_session(None).context("Failed to start sess")?;

        Ok(MongodbConnection::new(self.conf.clone(), con, sess))
    }
}

use ansilo_connectors_base::interface::ConnectionPool;
use ansilo_core::{
    auth::AuthContext,
    err::{Context, Result},
};
use mongodb::options::ClientOptions;
use rumongodb::OpenFlags;

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
        let opts = ClientOptions::parse_connection_string_sync(&self.conf.url)
            .context("Failed to parse connection string")?;
        let con =
            mongodb::sync::Client::with_options(options).context("Failed to connect to mongodb")?;

        let sess = con.start_session(None).context("Failed to start sess")?;

        Ok(MongodbConnection::new(con, sess))
    }
}

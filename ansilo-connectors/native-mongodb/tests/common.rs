use ansilo_connectors_native_mongodb::{MongodbConnection, MongodbConnectionConfig, MongodbConnector};

pub fn connect_to_mongodb() -> MongodbConnection {
    MongodbConnector::connect(MongodbConnectionConfig {
        path: ":memory:".into(),
        extensions: vec![],
    })
    .unwrap()
}

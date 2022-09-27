use std::time::Duration;

use ansilo_connectors_base::test::ecs::{start_containers, wait_for_log, ContainerInstances};
use ansilo_connectors_native_mongodb::{
    MongodbConnection, MongodbConnectionConfig, MongodbConnector,
};

#[macro_export]
macro_rules! current_dir {
    () => {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join(file!())
            .parent()
            .unwrap()
            .to_owned()
    };
}

/// Starts an mongo DB instance and waits for it to become ready to accept connections
pub fn start_mongo() -> ContainerInstances {
    let infra_path = current_dir!().to_path_buf();
    let services = start_containers("mongo", infra_path.clone(), false, Duration::from_secs(120));

    wait_for_log(
        infra_path.clone(),
        services.get("mongo").unwrap(),
        "Mongo startup successful!",
        Duration::from_secs(60),
    );

    services
}

pub fn connect_to_mongo(containers: &ContainerInstances) -> MongodbConnection {
    let config = MongodbConnectionConfig {
        url: format!(
            "mongodb://ansilo_admin:ansilo_testing@{}:27018/db",
            containers.get("mongo").unwrap().ip
        ),
        disable_transactions: false,
    };

    MongodbConnector::connect(config).unwrap()
}

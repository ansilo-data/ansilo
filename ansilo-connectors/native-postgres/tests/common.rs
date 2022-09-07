use std::{env, time::Duration};

use ansilo_connectors_base::test::ecs::{start_containers, wait_for_log, ContainerInstances};
use ansilo_connectors_native_postgres::{
    PooledClient, PostgresConnection, PostgresConnectionConfig, PostgresConnector,
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

/// Starts an postgres DB instance and waits for it to become ready to accept connections
/// NOTE: The instance takes a long time to boot up due to the image size
/// so it is not terminated at the end of each test, rather it has a
/// script which will exit automatically after idleing for 30 min
pub fn start_postgres() -> ContainerInstances {
    let infra_path = current_dir!().to_path_buf();
    let services = start_containers(
        "postgres",
        infra_path.clone(),
        false,
        Duration::from_secs(120),
    );

    wait_for_log(
        infra_path.clone(),
        services.get("postgres").unwrap(),
        "database system is ready to accept connections",
        Duration::from_secs(60),
    );

    services
}

pub fn connect_to_postgres(containers: &ContainerInstances) -> PostgresConnection<PooledClient> {
    let mut config = PostgresConnectionConfig::default();
    config.opts = Some(format!(
        "host={} port=5433 user=ansilo_admin password=ansilo_testing dbname=postgres",
        containers.get("postgres").unwrap().ip
    ));

    PostgresConnector::connect(config).unwrap()
}

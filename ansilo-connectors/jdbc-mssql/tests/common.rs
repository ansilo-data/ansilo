use std::{collections::HashMap, env, time::Duration};

use ansilo_connectors_base::test::ecs::{
    get_current_target_dir, start_containers, wait_for_log, ContainerInstances,
};
use ansilo_connectors_jdbc_base::JdbcConnection;
use ansilo_connectors_jdbc_mssql::{MssqlJdbcConnectionConfig, MssqlJdbcConnector};

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

/// Starts an Mssql DB instance and waits for it to become ready to accept connections
pub fn start_mssql() -> ContainerInstances {
    let infra_path = current_dir!().to_path_buf();
    let services = start_containers("mssql", infra_path.clone(), false, Duration::from_secs(180));

    wait_for_log(
        infra_path.clone(),
        services.get("mssql").unwrap(),
        "MSSQL startup successful!",
        Duration::from_secs(180),
    );

    services
}

pub fn connect_to_mssql(containers: &ContainerInstances) -> JdbcConnection {
    env::set_var(
        "ANSILO_CLASSPATH",
        get_current_target_dir().to_str().unwrap(),
    );

    let config = MssqlJdbcConnectionConfig::new(
        format!(
            "jdbc:sqlserver://{}:1435;database=testdb;user=ansilo_admin;password=Ansilo_testing!;loginTimeout=60;encrypt=false",
            containers.get("mssql").unwrap().ip
        ),
        HashMap::new(),
        None,
    );

    MssqlJdbcConnector::connect(config).unwrap()
}

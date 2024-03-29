use std::{collections::HashMap, env, time::Duration};

use ansilo_connectors_base::test::ecs::{
    get_current_target_dir, start_containers, wait_for_log, ContainerInstances,
};
use ansilo_connectors_jdbc_base::JdbcConnection;
use ansilo_connectors_jdbc_mysql::{MysqlJdbcConnectionConfig, MysqlJdbcConnector};

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

/// Starts an Mysql DB instance and waits for it to become ready to accept connections
pub fn start_mysql() -> ContainerInstances {
    let infra_path = current_dir!().to_path_buf();
    let services = start_containers("mysql", infra_path.clone(), false, Duration::from_secs(120));

    wait_for_log(
        infra_path.clone(),
        services.get("mysql").unwrap(),
        "ready for connections",
        Duration::from_secs(60),
    );

    services
}

pub fn connect_to_mysql(containers: &ContainerInstances) -> JdbcConnection {
    env::set_var(
        "ANSILO_CLASSPATH",
        get_current_target_dir().to_str().unwrap(),
    );

    let config = MysqlJdbcConnectionConfig::new(
        format!(
            "jdbc:mysql://{}:3307/db",
            containers.get("mysql").unwrap().ip
        ),
        {
            let mut props = HashMap::<String, String>::new();
            props.insert("user".into(), "ansilo_admin".to_string());
            props.insert("password".into(), "ansilo_testing".into());
            props
        },
        None,
    );

    MysqlJdbcConnector::connect(config).unwrap()
}

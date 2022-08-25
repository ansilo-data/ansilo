use std::{collections::HashMap, env, time::Duration};

use ansilo_connectors_base::test::ecs::{
    get_current_target_dir, start_containers, wait_for_log, ContainerInstances,
};
use ansilo_connectors_jdbc_base::JdbcConnection;
use ansilo_connectors_jdbc_oracle::{OracleJdbcConnectionConfig, OracleJdbcConnector};

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

/// Starts an Oracle DB instance and waits for it to become ready to accept connections
/// NOTE: The instance takes a long time to boot up due to the image size
/// so it is not terminated at the end of each test, rather it has a
/// script which will exit automatically after idleing for 30 min
pub fn start_oracle() -> ContainerInstances {
    let infra_path = current_dir!().to_path_buf();
    let services = start_containers(
        "oracle",
        infra_path.clone(),
        false,
        Duration::from_secs(600),
    );

    wait_for_log(
        infra_path,
        services.get("oracle").unwrap(),
        "alter pluggable database all open",
        Duration::from_secs(180),
    );

    services
}

pub fn connect_to_oracle(containers: &ContainerInstances) -> JdbcConnection {
    env::set_var(
        "ANSILO_CLASSPATH",
        get_current_target_dir().to_str().unwrap(),
    );

    let config = OracleJdbcConnectionConfig::new(
        format!(
            "jdbc:oracle:thin:@{}:1522/db",
            containers.get("oracle").unwrap().ip
        ),
        {
            let mut props = HashMap::<String, String>::new();
            props.insert("oracle.jdbc.user".into(), "ansilo_admin".to_string());
            props.insert("oracle.jdbc.password".into(), "ansilo_testing".into());
            props
        },
        None,
    );

    OracleJdbcConnector::connect(config).unwrap()
}

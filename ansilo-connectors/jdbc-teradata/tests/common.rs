use std::{collections::HashMap, env, process};

use ansilo_connectors_base::test::ecs::get_current_target_dir;
use ansilo_connectors_jdbc_base::JdbcConnection;
use ansilo_connectors_jdbc_teradata::{TeradataJdbcConnectionConfig, TeradataJdbcConnector};

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

pub fn start_teradata() {
    let res = process::Command::new("bash")
        .arg(current_dir!().join("infra/start-teradata-vm.sh"))
        .spawn()
        .unwrap()
        .wait()
        .unwrap();

    assert!(res.success());
}

pub fn connect_to_teradata() -> JdbcConnection {
    env::set_var(
        "ANSILO_CLASSPATH",
        get_current_target_dir().to_str().unwrap(),
    );

    let config = TeradataJdbcConnectionConfig::new(
        "jdbc:teradata://ansilo-teradata-test.japaneast.cloudapp.azure.com/DBS_PORT=1026,USER=ansilo_admin,PASSWORD=ansilo_testing".into(),
        HashMap::new(),
        None,
    );

    TeradataJdbcConnector::connect(config).unwrap()
}

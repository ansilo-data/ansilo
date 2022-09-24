use std::{
    collections::HashMap,
    env, process,
    sync::atomic::{AtomicBool, Ordering},
};

use ansilo_connectors_base::test::ecs::get_current_target_dir;
use ansilo_connectors_jdbc_base::JdbcConnection;
use ansilo_connectors_jdbc_teradata::{TeradataJdbcConnectionConfig, TeradataJdbcConnector};

static HAS_INIT: AtomicBool = AtomicBool::new(false);

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
    if HAS_INIT.load(Ordering::SeqCst) {
        return;
    }

    let res = process::Command::new("bash")
        .arg(current_dir!().join("infra/start-teradata-vm.sh"))
        .spawn()
        .unwrap()
        .wait()
        .unwrap();

    assert!(res.success());

    HAS_INIT.store(true, Ordering::SeqCst);
}

pub fn connect_to_teradata() -> JdbcConnection {
    env::set_var(
        "ANSILO_CLASSPATH",
        get_current_target_dir().to_str().unwrap(),
    );

    let config = TeradataJdbcConnectionConfig::new(
        "jdbc:teradata://ansilo-teradata-test.japaneast.cloudapp.azure.com/DBS_PORT=1026,USER=ansilo_admin,PASSWORD=ansilo_testing,CHARSET=UTF8".into(),
        HashMap::new(),
        vec!["SET SESSION CHARACTER SET UNICODE PASS THROUGH ON;".into()],
        None,
    );

    TeradataJdbcConnector::connect(config).unwrap()
}

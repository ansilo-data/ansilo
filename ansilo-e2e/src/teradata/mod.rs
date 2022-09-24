use std::{collections::HashMap, env, fs, path::PathBuf, process::Command, time::Duration};

use ansilo_connectors_base::{
    interface::{Connection, QueryHandle},
    test::ecs::get_current_target_dir,
};
use ansilo_connectors_jdbc_base::{JdbcConnection, JdbcQuery};
use ansilo_connectors_jdbc_teradata::{TeradataJdbcConnectionConfig, TeradataJdbcConnector};
use ansilo_logging::info;
use glob::glob;

use crate::util::{dir::workspace_dir, locking::FunctionCache};

pub fn start_teradata() {
    let mut cache = FunctionCache::<()>::new("teradata", Duration::from_secs(600));

    if cache.valid().is_some() {
        return;
    }

    let res = Command::new("bash")
        .arg(
            workspace_dir()
                .join("ansilo-connectors/jdbc-teradata/tests/infra/start-teradata-vm.sh"),
        )
        .spawn()
        .unwrap()
        .wait()
        .unwrap();

    assert!(res.success());

    cache.save(&());
}

pub fn connect_to_teradata() -> JdbcConnection {
    env::set_var(
        "ANSILO_CLASSPATH",
        get_current_target_dir().to_str().unwrap(),
    );

    let config = TeradataJdbcConnectionConfig::new(
        "jdbc:teradata://ansilo-teradata-test.japaneast.cloudapp.azure.com/DBS_PORT=1026,USER=ansilo_admin,PASSWORD=ansilo_testing,CHARSET=UTF16".into(),
        HashMap::new(),
        vec!["SET SESSION CHARACTER SET UNICODE PASS THROUGH ON;".into()],
        None,
    );

    TeradataJdbcConnector::connect(config).unwrap()
}

pub fn init_teradata_sql(path: PathBuf) -> JdbcConnection {
    let mut connection = connect_to_teradata();

    for path in glob(path.to_str().unwrap()).unwrap().map(|i| i.unwrap()) {
        info!("Running teradata init script: {}", path.display());
        let sql = fs::read_to_string(path).unwrap();
        let statements = sql.split("$$").filter(|s| s.trim().len() > 0);

        for stmt in statements {
            connection
                .prepare(JdbcQuery::new(stmt, vec![]))
                .unwrap()
                .execute_modify()
                .unwrap();
        }
    }

    connection
}

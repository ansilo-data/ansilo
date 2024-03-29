use std::{collections::HashMap, env, fs, path::PathBuf, sync::Mutex, time::Duration};

use ansilo_connectors_base::{
    interface::{Connection, QueryHandle},
    test::ecs::{ start_containers, wait_for_log, ContainerInstances},
};
use ansilo_connectors_jdbc_base::{JdbcConnection, JdbcQuery};
use ansilo_connectors_jdbc_oracle::{OracleJdbcConnectionConfig, OracleJdbcConnector};
use ansilo_logging::info;
use glob::glob;

use crate::util::{dir::workspace_dir, locking::FunctionCache};

static ORACLE_MUTEX: Mutex<()> = Mutex::new(());

/// Starts an Oracle DB instance and waits for it to become ready to accept connections
/// NOTE: The instance takes a long time to boot up due to the image size
/// so it is not terminated at the end of each test, rather it has a
/// script which will exit automatically after idleing for 30 min
pub fn start_oracle() -> ContainerInstances {
    let _lock = ORACLE_MUTEX.lock().unwrap();

    let mut cache = FunctionCache::<ContainerInstances>::new("oracle", Duration::from_secs(600));

    if let Some(services) = cache.valid() {
        cache.extend();
        env::set_var("ORACLE_IP", services.get("oracle").unwrap().ip.to_string());
        return services;
    }

    let infra_path = workspace_dir().join("ansilo-connectors/jdbc-oracle/tests");
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

    cache.save(&services);

    // Env var is referenced by our config.yml files to connect to the oracle instance
    env::set_var("ORACLE_IP", services.get("oracle").unwrap().ip.to_string());

    services
}

pub fn init_oracle_sql(containers: &ContainerInstances, path: PathBuf) -> JdbcConnection {
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

    let mut connection = OracleJdbcConnector::connect(config).unwrap();

    for path in glob(path.to_str().unwrap()).unwrap().map(|i| i.unwrap()) {
        info!("Running oracle init script: {}", path.display());
        let sql = fs::read_to_string(path).unwrap();
        let statements = sql.split("$$").filter(|s| s.trim().len() > 0);

        for stmt in statements {
            connection
                .prepare(JdbcQuery::new(stmt, vec![]))
                .unwrap()
                .execute_query()
                .unwrap();
        }
    }

    connection
}

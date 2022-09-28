use std::{collections::HashMap, env, fs, path::PathBuf, sync::Mutex, thread, time::Duration};

use ansilo_connectors_base::test::ecs::{
    get_current_target_dir, start_containers, wait_for_log, ContainerInstances,
};
use ansilo_connectors_jdbc_base::JdbcConnection;
use ansilo_connectors_jdbc_mssql::{MssqlJdbcConnectionConfig, MssqlJdbcConnector};
use ansilo_logging::info;
use glob::glob;

use crate::util::{dir::workspace_dir, locking::FunctionCache};

static MSSQL_MUTEX: Mutex<()> = Mutex::new(());

/// Starts an mssql DB instance and waits for it to become ready to accept connections
pub fn start_mssql() -> ContainerInstances {
    let _lock = MSSQL_MUTEX.lock().unwrap();

    env::set_var(
        "ANSILO_CLASSPATH",
        get_current_target_dir().to_str().unwrap(),
    );

    let mut cache = FunctionCache::<ContainerInstances>::new("mssql", Duration::from_secs(600));

    if let Some(services) = cache.valid() {
        cache.extend();
        env::set_var("MSSQL_IP", services.get("mssql").unwrap().ip.to_string());
        return services;
    }

    let infra_path = workspace_dir().join("ansilo-connectors/jdbc-mssql/tests");
    let services = start_containers("mssql", infra_path.clone(), false, Duration::from_secs(180));

    wait_for_log(
        infra_path.clone(),
        services.get("mssql").unwrap(),
        "MSSQL startup successful!",
        Duration::from_secs(180),
    );

    thread::sleep(Duration::from_secs(5));

    cache.save(&services);

    // Env var is referenced by our config.yml files to connect to the mssql instance
    env::set_var("MSSQL_IP", services.get("mssql").unwrap().ip.to_string());

    services
}

pub fn init_mssql_sql(containers: &ContainerInstances, path: PathBuf) -> JdbcConnection {
    let config = MssqlJdbcConnectionConfig::new(
        format!(
            "jdbc:sqlserver://{}:1435;database=testdb;user=ansilo_admin;password=Ansilo_testing!;loginTimeout=60;encrypt=false",
            containers.get("mssql").unwrap().ip
        ),
        HashMap::new(),
        None,
    );

    let mut connection = MssqlJdbcConnector::connect(config).unwrap();

    for path in glob(path.to_str().unwrap()).unwrap().map(|i| i.unwrap()) {
        info!("Running mssql init script: {}", path.display());
        let sql = fs::read_to_string(path).unwrap();
        let statements = sql.split("$$").filter(|s| s.trim().len() > 0);

        for stmt in statements {
            connection.execute(stmt, vec![]).unwrap();
        }
    }

    connection
}

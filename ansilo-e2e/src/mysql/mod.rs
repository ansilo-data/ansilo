use std::{collections::HashMap, env, fs, path::PathBuf, sync::Mutex, thread, time::Duration};

use ansilo_connectors_base::{
    interface::{Connection, QueryHandle},
    test::ecs::{start_containers, wait_for_log, ContainerInstances},
};
use ansilo_connectors_jdbc_base::{JdbcConnection, JdbcQuery};
use ansilo_connectors_jdbc_mysql::{MysqlJdbcConnectionConfig, MysqlJdbcConnector};
use ansilo_logging::info;
use glob::glob;

use crate::util::{dir::workspace_dir, locking::FunctionCache};

static MYSQL_MUTEX: Mutex<()> = Mutex::new(());

/// Starts an mysql DB instance and waits for it to become ready to accept connections
pub fn start_mysql() -> ContainerInstances {
    let _lock = MYSQL_MUTEX.lock().unwrap();

    let mut cache = FunctionCache::<ContainerInstances>::new("mysql", Duration::from_secs(600));

    if let Some(services) = cache.valid() {
        cache.extend();
        env::set_var("MYSQL_IP", services.get("mysql").unwrap().ip.to_string());
        return services;
    }

    let infra_path = workspace_dir().join("ansilo-connectors/jdbc-mysql/tests");
    let services = start_containers("mysql", infra_path.clone(), false, Duration::from_secs(120));

    wait_for_log(
        infra_path,
        services.get("mysql").unwrap(),
        "ready for connections",
        Duration::from_secs(60),
    );

    thread::sleep(Duration::from_secs(15));

    cache.save(&services);

    // Env var is referenced by our config.yml files to connect to the mysql instance
    env::set_var("MYSQL_IP", services.get("mysql").unwrap().ip.to_string());

    services
}

pub fn init_mysql_sql(containers: &ContainerInstances, path: PathBuf) -> JdbcConnection {
    let config = MysqlJdbcConnectionConfig::new(
        format!(
            "jdbc:mysql://{}:3307/db",
            containers.get("mysql").unwrap().ip
        ),
        {
            let mut props = HashMap::<String, String>::new();
            props.insert("user".into(), "ansilo_admin".to_string());
            props.insert("password".into(), "ansilo_testing".into());
            props.insert("characterEncoding".into(), "utf8".into());
            props
        },
        None,
    );

    let mut connection = MysqlJdbcConnector::connect(config).unwrap();

    for path in glob(path.to_str().unwrap()).unwrap().map(|i| i.unwrap()) {
        info!("Running mysql init script: {}", path.display());
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

use std::{env, fs, path::PathBuf, sync::Mutex, thread, time::Duration};

use ansilo_connectors_base::test::ecs::{start_containers, wait_for_log, ContainerInstances};
use ansilo_connectors_native_postgres::{
    PooledClient, PostgresConnection, PostgresConnectionConfig, PostgresConnector,
};
use ansilo_logging::info;
use glob::glob;

use crate::util::{dir::workspace_dir, locking::FunctionCache};

static POSTGRES_MUTEX: Mutex<()> = Mutex::new(());

/// Starts an postgres DB instance and waits for it to become ready to accept connections
pub fn start_postgres() -> ContainerInstances {
    let _lock = POSTGRES_MUTEX.lock().unwrap();

    let mut cache = FunctionCache::<ContainerInstances>::new("postgres", Duration::from_secs(600));

    if let Some(services) = cache.valid() {
        cache.extend();
        env::set_var(
            "POSTGRES_IP",
            services.get("postgres").unwrap().ip.to_string(),
        );
        return services;
    }

    let infra_path = workspace_dir().join("ansilo-connectors/native-postgres/tests");
    let services = start_containers(
        "postgres",
        infra_path.clone(),
        false,
        Duration::from_secs(120),
    );

    wait_for_log(
        infra_path,
        services.get("postgres").unwrap(),
        "database system is ready to accept connections",
        Duration::from_secs(60),
    );

    thread::sleep(Duration::from_secs(5));

    cache.save(&services);

    // Env var is referenced by our config.yml files to connect to the postgres instance
    env::set_var(
        "POSTGRES_IP",
        services.get("postgres").unwrap().ip.to_string(),
    );

    services
}

pub fn init_postgres_sql(
    containers: &ContainerInstances,
    path: PathBuf,
) -> PostgresConnection<PooledClient> {
    let mut config = PostgresConnectionConfig::default();
    config.url = Some(format!(
        "host={} port=5433 user=ansilo_admin password=ansilo_testing dbname=postgres",
        containers.get("postgres").unwrap().ip
    ));

    let mut connection = PostgresConnector::connect(config).unwrap();

    for path in glob(path.to_str().unwrap()).unwrap().map(|i| i.unwrap()) {
        info!("Running postgres init script: {}", path.display());
        let sql = fs::read_to_string(path).unwrap();
        let statements = sql.split("$$").filter(|s| s.trim().len() > 0);

        for stmt in statements {
            connection.execute_modify(stmt, vec![]).unwrap();
        }
    }

    connection
}

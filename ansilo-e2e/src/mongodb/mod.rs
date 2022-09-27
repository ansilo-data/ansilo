use std::{env, fs, path::PathBuf, sync::Mutex, time::Duration};

use ansilo_connectors_base::test::ecs::{start_containers, wait_for_log, ContainerInstances};
use ansilo_connectors_native_mongodb::{
    bson::Bson, MongodbConnection, MongodbConnectionConfig, MongodbConnector,
};
use glob::glob;

use ansilo_logging::info;

use crate::util::{dir::workspace_dir, locking::FunctionCache};

static MONGO_MUTEX: Mutex<()> = Mutex::new(());

/// Starts an mongo DB instance and waits for it to become ready to accept connections
pub fn start_mongodb() -> ContainerInstances {
    let _lock = MONGO_MUTEX.lock().unwrap();

    let mut cache = FunctionCache::<ContainerInstances>::new("mongo", Duration::from_secs(600));

    if let Some(services) = cache.valid() {
        cache.extend();
        env::set_var("MONGO_IP", services.get("mongo").unwrap().ip.to_string());
        return services;
    }

    let infra_path = workspace_dir().join("ansilo-connectors/native-mongodb/tests");
    let services = start_containers("mongo", infra_path.clone(), false, Duration::from_secs(120));

    wait_for_log(
        infra_path.clone(),
        services.get("mongo").unwrap(),
        "Mongo startup successful!",
        Duration::from_secs(120),
    );

    env::set_var("MONGO_IP", services.get("mongo").unwrap().ip.to_string());
    cache.save(&services);

    services
}

pub fn init_mongodb(containers: &ContainerInstances, path: PathBuf) -> MongodbConnection {
    let config = MongodbConnectionConfig {
        url: format!(
            "mongodb://ansilo_admin:ansilo_testing@{}:27018/db",
            containers.get("mongo").unwrap().ip
        ),
        disable_transactions: false,
    };

    let connection = MongodbConnector::connect(config).unwrap();

    for path in glob(path.to_str().unwrap()).unwrap().map(|i| i.unwrap()) {
        info!("Running mongo commands: {}", path.display());
        let cmd = fs::read_to_string(path).unwrap();
        let json: serde_json::Value = serde_json::from_str(&cmd).unwrap();
        let cmds = json.as_array().unwrap();

        for cmd in cmds {
            let cmd = Bson::try_from(cmd.clone()).unwrap();
            let mut cmd = cmd.as_document().unwrap().clone();
            let ignore_error = cmd.remove("ignore_error");

            let res = connection
                .client()
                .default_database()
                .unwrap()
                .run_command(cmd.clone(), None);

            if let Some(Bson::Boolean(true)) = ignore_error {
                if let Err(err) = res {
                    info!("Mongo db returned error for command: {:?}", err);
                }
            } else {
                res.unwrap();
            }
        }
    }

    connection
}

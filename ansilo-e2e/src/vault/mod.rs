use std::{env, sync::Mutex, time::Duration};

use ansilo_connectors_base::test::ecs::{start_containers, wait_for_log, ContainerInstances};

use crate::{current_dir, util::locking::FunctionCache};

static VAULT_MUTEX: Mutex<()> = Mutex::new(());

/// Starts an vault instance and waits for it to become ready to accept connections
pub fn start_vault() -> ContainerInstances {
    let _lock = VAULT_MUTEX.lock().unwrap();

    let mut cache = FunctionCache::<ContainerInstances>::new("vault", Duration::from_secs(600));

    if let Some(services) = cache.valid() {
        cache.extend();
        env::set_var("VAULT_IP", services.get("vault").unwrap().ip.to_string());
        return services;
    }

    let infra_path = current_dir!().join("infra");
    let services = start_containers("vault", infra_path.clone(), false, Duration::from_secs(180));

    wait_for_log(
        infra_path.clone(),
        services.get("vault").unwrap(),
        "Vault started successfully!",
        Duration::from_secs(120),
    );

    cache.save(&services);

    // Env var is referenced by our config.yml files to connect to the vault instance
    env::set_var("VAULT_IP", services.get("vault").unwrap().ip.to_string());

    services
}

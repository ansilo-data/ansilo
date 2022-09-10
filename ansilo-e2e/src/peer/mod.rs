use std::{convert::TryInto, env, path::PathBuf, sync::Mutex};

use ansilo_core::data::uuid::Uuid;
use ansilo_main::Ansilo;
use postgres::Client;

use crate::util::main::{connect, run_instance_without_connect};

static MUTEX: Mutex<()> = Mutex::new(());

pub fn run_instances<const N: usize>(
    configs: [(&'static str, PathBuf); N],
) -> [(Ansilo, Client); N] {
    let _lock = MUTEX.lock().unwrap();
    let mut instances = vec![];

    for (name, config_path) in configs.iter() {
        println!(" === Starting instance {name} ===");
        env::set_var("INSTANCE_NAME", name);
        env::set_var("TEMP_DIR", format!("/tmp/ansilo-peer/{}", Uuid::new_v4()));
        let (instance, port) = run_instance_without_connect(config_path.clone());
        let client = connect(port);

        env::set_var(format!("URL_{name}"), format!("http://localhost:{port}"));

        instances.push((instance, client))
    }

    for (name, _) in configs.iter() {
        env::remove_var(format!("URL_{name}"));
    }

    env::remove_var("TEMP_DIR");
    env::remove_var("INSTANCE_NAME");

    if let Ok(i) = instances.try_into() {
        i
    } else {
        unreachable!()
    }
}

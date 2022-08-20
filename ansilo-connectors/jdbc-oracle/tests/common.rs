use std::{env, time::Duration};

use ansilo_connectors_base::test::ecs::{start_containers, wait_for_log, ContainerInstances};

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

/// Starts an Oracle DB instance and waits for it to become ready to accept connections
/// NOTE: The instance takes a long time to boot up due to the image size
/// so it is not terminated at the end of each test, rather it has a
/// script which will exit automatically after idleing for 30 min
pub fn start_oracle() -> ContainerInstances {
    let infra_path = current_dir!().to_path_buf();
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

    services
}

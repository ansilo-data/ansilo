use std::{time::Duration, env};

use ansilo_connectors_base::test::ecs::{
    get_current_target_dir, start_containers, wait_for_log, ContainerInstances,
};

/// Starts an Oracle DB instance and waits for it to become ready to accept connections
/// NOTE: The instance takes a long time to boot up due to the image size
/// so it is not terminated at the end of each test, rather it has a
/// script which will exit automatically after idleing for 30 min
pub fn start_oracle() -> ContainerInstances {
    env::set_var(
        "ANSILO_CLASSPATH",
        get_current_target_dir().to_str().unwrap(),
    );

    let infra_path = crate::util::workspace_dir().join("ansilo-connectors/jdbc-oracle/tests");
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

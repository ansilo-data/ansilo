use std::time::Duration;

use crate::{
    common::{start_containers, wait_for_log, ContainerInstances},
    current_dir,
};

mod connect;

/// Starts an Oracle DB instance and waits for it to become ready to accept connections
/// NOTE: The instance takes a long time to boot up due to the image size
/// so it is not terminated at the end of each test, rather it has a
/// script which will exit automatically after idleing for 30 min
fn start_oracle() -> ContainerInstances {
    let services = start_containers(current_dir!(), false, Duration::from_secs(600));

    wait_for_log(
        current_dir!(),
        services.get("oracle").unwrap(),
        "alter pluggable database all open",
        Duration::from_secs(180),
    );

    services
}

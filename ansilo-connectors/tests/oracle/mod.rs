use std::time::Duration;

use crate::{
    common::{start_containers, wait_for_log, ContainerInstances},
    current_dir,
};

mod connect;

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

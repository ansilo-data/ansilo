use std::path::Path;

use crate::common::{start_containers, ContainerInstances};

mod connect;

fn start_oracle() -> ContainerInstances {
    start_containers(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join(file!())
            .parent()
            .unwrap()
            .to_owned(),
    )
}

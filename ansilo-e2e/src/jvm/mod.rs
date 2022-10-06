use std::env;

use ansilo_connectors_base::test::ecs::get_current_target_dir;
use ansilo_logging::debug;
use ansilo_util_pgx_install::ctor;

/// Ensure that our jars are loaded by our jvm
/// for all jdbc-based connectors
#[ctor]
fn init_jvm_classpath() {
    let class_path = get_current_target_dir();
    debug!("Setting jvm class path to {}", class_path.display());
    env::set_var("ANSILO_CLASSPATH", class_path.to_str().unwrap());
}

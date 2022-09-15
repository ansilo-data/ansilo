use std::env;

use ansilo_connectors_base::test::ecs::get_current_target_dir;
use ansilo_e2e::{current_dir, web::url};
use serial_test::serial;

#[test]
#[serial]
fn test() {
    ansilo_logging::init_for_tests();

    // Tests run under target/{profile}/deps so it
    // the default frontend path does not resolve correctly,
    // we correct it here
    env::set_var(
        "ANSILO_FRONTEND_PATH",
        get_current_target_dir()
            .join("frontend")
            .to_string_lossy()
            .to_string(),
    );

    let (instance, _port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    let res = reqwest::blocking::get(url(&instance, "/index.html"))
        .unwrap()
        .error_for_status()
        .unwrap()
        .text()
        .unwrap();

    assert!(res.contains("<!DOCTYPE html>"));
}

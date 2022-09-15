use ansilo_core::build::ansilo_version;
use ansilo_e2e::{current_dir, web::url};
use ansilo_web::VersionInfo;
use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
fn test() {
    ansilo_logging::init_for_tests();
    let (instance, _port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    let res = reqwest::blocking::get(url(&instance, "/api/version"))
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<VersionInfo>()
        .unwrap();

    assert_eq!(res.version, ansilo_version());
}

use ansilo_core::{build::ansilo_version, web::node::NodeInfo};
use ansilo_e2e::{current_dir, web::url};
use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
fn test() {
    ansilo_logging::init_for_tests();
    let (instance, _port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    let res = reqwest::blocking::get(url(&instance, "/api/v1/node"))
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<NodeInfo>()
        .unwrap();

    assert_eq!(
        res,
        NodeInfo {
            name: "Web Test".into(),
            version: ansilo_version()
        }
    );
}

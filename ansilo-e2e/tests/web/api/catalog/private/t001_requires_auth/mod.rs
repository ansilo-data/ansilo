use ansilo_e2e::{current_dir, web::url};
use pretty_assertions::assert_eq;
use reqwest::StatusCode;
use serial_test::serial;

#[test]
#[serial]
fn test() {
    ansilo_logging::init_for_tests();
    let (instance, _port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    let client = reqwest::blocking::Client::new();
    let res = client
        .get(url(&instance, "/api/v1/catalog/private"))
        .send()
        .unwrap();

    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

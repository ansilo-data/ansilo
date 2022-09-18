use std::fs;

use ansilo_e2e::{current_dir, web::url_https};
use serial_test::serial;

#[test]
#[serial]
fn test() {
    ansilo_logging::init_for_tests();
    let (instance, _port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    let client = reqwest::blocking::Client::builder().add_root_certificate(
        reqwest::Certificate::from_pem(
            fs::read(current_dir!().join("keys/rootCA.crt"))
                .unwrap()
                .as_slice(),
        )
        .unwrap(),
    );

    let _ = client
        .build()
        .unwrap()
        .get(url_https(&instance, "/"))
        .send()
        .unwrap();
}

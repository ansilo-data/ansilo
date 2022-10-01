use ansilo_e2e::{current_dir, web::url};
use ansilo_web::api::healthcheck::HealthCheck;
use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
fn test() {
    ansilo_logging::init_for_tests();
    let (instance, _port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    let res: HealthCheck = reqwest::blocking::get(url(&instance, "/api/health"))
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .unwrap();

    dbg!(res.clone());
    assert_eq!(res.subsystems.get("HTTP").unwrap().healthy, true);
    assert_eq!(res.subsystems.get("Postgres").unwrap().healthy, true);
    assert_eq!(res.subsystems.get("Proxy").unwrap().healthy, true);
    assert_eq!(res.subsystems.get("FDW").unwrap().healthy, true);
    assert_eq!(res.subsystems.get("Authenticator").unwrap().healthy, true);
    assert_eq!(res.subsystems.get("Scheduler").unwrap().healthy, true);
}

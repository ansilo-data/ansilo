use ansilo_e2e::current_dir;
use serial_test::serial;

#[test]
#[serial]
fn test() {
    ansilo_logging::init_for_tests();
    ansilo_e2e::vault::start_vault();
    let err = std::panic::catch_unwind(|| {
        let (_instance, _port) =
            ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));
    })
    .unwrap_err();

    let err: Box<String> = err.downcast().unwrap();
    dbg!(err.to_string());
    assert!(err.contains("Failed to authenticate with Vault"))
}

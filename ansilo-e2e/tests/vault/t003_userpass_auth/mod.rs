use ansilo_e2e::current_dir;
use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
fn test() {
    ansilo_logging::init_for_tests();
    ansilo_e2e::vault::start_vault();
    let (instance, _port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    // Assert secret is correct
    let authenticator = instance.subsystems().unwrap().authenticator();
    let user = authenticator.get_user("test_user1").unwrap();
    assert_eq!(user.r#type.as_password().unwrap().password, "mysecret");
    let user = authenticator.get_user("test_user2").unwrap();
    assert_eq!(user.r#type.as_password().unwrap().password, "anothersecret");
}

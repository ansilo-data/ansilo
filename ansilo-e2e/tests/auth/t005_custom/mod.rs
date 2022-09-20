use std::env;

use ansilo_core::auth::{AuthContext, CustomAuthContext, ProviderAuthContext};
use ansilo_e2e::current_dir;
use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
fn test_invalid_user() {
    ansilo_logging::init_for_tests();
    let (_instance, port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    assert_eq!(
        ansilo_e2e::util::main::connect_opts("invalid", "pass", port, |_| ())
            .err()
            .unwrap()
            .to_string(),
        "db error: ERROR: User 'invalid' does not exist"
    );
}

#[test]
#[serial]
fn test_invalid_password() {
    ansilo_logging::init_for_tests();
    let (_instance, port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    assert_eq!(
        ansilo_e2e::util::main::connect_opts("test_user", "wrong_pass", port, |_| ())
            .err()
            .unwrap()
            .to_string(),
        "db error: ERROR: incorrect password"
    );
}

#[test]
#[serial]
fn test_valid_login() {
    ansilo_logging::init_for_tests();
    let (_instance, port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    ansilo_e2e::util::main::connect_opts("test_user", "password1", port, |_| ()).unwrap();
}

#[test]
#[serial]
fn test_grants_to_view() {
    ansilo_logging::init_for_tests();
    let (_instance, port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    let mut client =
        ansilo_e2e::util::main::connect_opts("test_user", "password1", port, |_| ()).unwrap();

    let rows = client.query("SELECT * FROM dummy", &[]).unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "data".to_string());
}

#[test]
#[serial]
fn test_auth_context() {
    ansilo_logging::init_for_tests();
    let (_instance, port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    let mut client =
        ansilo_e2e::util::main::connect_opts("test_user", "password1", port, |_| ()).unwrap();

    let row = client.query_one("SELECT auth_context()", &[]).unwrap();
    let ctx = row.get::<_, serde_json::Value>(0);
    let ctx: AuthContext = serde_json::from_value(ctx).unwrap();

    assert_eq!(ctx.username, "test_user");
    assert_eq!(ctx.provider, "custom");
    assert_eq!(
        ctx.more,
        ProviderAuthContext::Custom(CustomAuthContext {
            data: serde_json::json!({})
        })
    );
}

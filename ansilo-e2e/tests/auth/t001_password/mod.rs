use std::{env, time::Duration};

use ansilo_core::auth::{AuthContext, PasswordAuthContext, ProviderAuthContext};
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
        "db error: ERROR: Incorrect password"
    );
}

#[test]
#[serial]
fn test_valid_login() {
    ansilo_logging::init_for_tests();
    let (_instance, port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    ansilo_e2e::util::main::connect_opts("test_user", "password123", port, |_| ()).unwrap();
}

#[test]
#[serial]
fn test_grants_to_view() {
    ansilo_logging::init_for_tests();
    let (_instance, port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    let mut client =
        ansilo_e2e::util::main::connect_opts("test_user", "password123", port, |_| ()).unwrap();

    let rows = client.query("SELECT * FROM dummy", &[]).unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "data".to_string());
}

#[test]
#[serial]
fn test_denied_ungranted_view() {
    ansilo_logging::init_for_tests();
    let (_instance, port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    let mut client =
        ansilo_e2e::util::main::connect_opts("test_user", "password123", port, |_| ()).unwrap();

    assert_eq!(
        client
            .query("SELECT * FROM private", &[])
            .err()
            .unwrap()
            .to_string(),
        "db error: ERROR: permission denied for view private"
    );
}

#[test]
#[serial]
fn test_auth_context() {
    ansilo_logging::init_for_tests();
    let (_instance, port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    let mut client =
        ansilo_e2e::util::main::connect_opts("test_user", "password123", port, |_| ()).unwrap();

    let row = client.query_one("SELECT auth_context()", &[]).unwrap();
    let ctx = row.get::<_, serde_json::Value>(0);
    let ctx: AuthContext = serde_json::from_value(ctx).unwrap();

    assert_eq!(ctx.username, "test_user");
    assert_eq!(ctx.provider, "password");
    assert_eq!(
        ctx.more,
        ProviderAuthContext::Password(PasswordAuthContext::default())
    );
}

#[test]
#[serial]
fn test_startup_parameters() {
    ansilo_logging::init_for_tests();
    let (_instance, port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    let mut client =
        ansilo_e2e::util::main::connect_opts("test_user", "password123", port, |conf| {
            conf.application_name("test_app_name");
        })
        .unwrap();

    let row = client.query_one("SHOW application_name", &[]).unwrap();
    assert_eq!(row.get::<_, String>(0), "test_app_name");
}

#[test]
#[serial]
fn test_override_auth_context_fails() {
    ansilo_logging::init_for_tests();
    let (_instance, port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    let mut client =
        ansilo_e2e::util::main::connect_opts("test_user", "password123", port, |_| ()).unwrap();

    let res = client
        .query_one(
            r#"
            SELECT __ansilo_auth.ansilo_set_auth_context(
                '{"username": "admin", "provider": "password", "type": "password", "authenticated_at": 1234}',
                '12345678901234567890'
            )
        "#,
            &[],
        )
        .err()
        .unwrap();

    assert_eq!(res.to_string(), "db error: ERROR: Already in auth context");
}

#[test]
#[serial]
fn test_reset_auth_context_with_incorrect_token_fails() {
    ansilo_logging::init_for_tests();
    let (_instance, port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    let mut client =
        ansilo_e2e::util::main::connect_opts("test_user", "password123", port, |_| ()).unwrap();

    let res = client
        .query_one(
            r#"SELECT __ansilo_auth.ansilo_reset_auth_context('12345678901234567890')"#,
            &[],
        )
        .err()
        .unwrap();

    assert_eq!(res.to_string(), "db error: FATAL: Invalid reset nonce when attempting to reset auth context, aborting process to prevent tampering");

    // Assert connection closed
    client.is_valid(Duration::from_secs(5)).err().unwrap();
}

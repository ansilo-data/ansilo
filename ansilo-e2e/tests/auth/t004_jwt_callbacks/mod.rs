use std::env;

use ansilo_e2e::{
    auth::jwt::{make_rsa_token, valid_exp},
    current_dir,
};
use pretty_assertions::assert_eq;
use serde_json::json;
use serial_test::serial;

#[test]
#[serial]
fn test_missing_scope_does_not_allow_data_access() {
    ansilo_logging::init_for_tests();
    let (_instance, port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    let token = make_rsa_token(
        json!({"scope": "invalid", "exp": valid_exp()}),
        include_bytes!("keys/private.key"),
    );
    let mut client = ansilo_e2e::util::main::connect_opts("token", &token, port, |_| ()).unwrap();

    let res = client.query("SELECT * FROM people", &[]);
    assert_eq!(
        res.unwrap_err().to_string(),
        "db error: ERROR: Strict check failed: read scope is required"
    );

    let res = client.query("INSERT INTO people VALUES (1, 'new')", &[]);
    assert_eq!(
        res.unwrap_err().to_string(),
        "db error: ERROR: Strict check failed: maintain scope is required"
    );

    let res = client.execute("UPDATE people SET name = 'changed'", &[]);
    assert_eq!(
        res.unwrap_err().to_string(),
        "db error: ERROR: Strict check failed: read scope is required"
    );

    let res = client.execute("DELETE FROM people", &[]);
    assert_eq!(
        res.unwrap_err().to_string(),
        "db error: ERROR: Strict check failed: read scope is required"
    );
}

#[test]
#[serial]
fn test_read_scope_only_grants_select() {
    ansilo_logging::init_for_tests();
    let (_instance, port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    let token = make_rsa_token(
        json!({"scope": "read", "exp": valid_exp()}),
        include_bytes!("keys/private.key"),
    );
    let mut client = ansilo_e2e::util::main::connect_opts("token", &token, port, |_| ()).unwrap();

    let rows = client.query("SELECT * FROM people", &[]).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>("name"), "John".to_string());

    let res = client.query("INSERT INTO people VALUES (2, 'new')", &[]);
    assert_eq!(
        res.unwrap_err().to_string(),
        "db error: ERROR: Strict check failed: maintain scope is required"
    );

    let res = client.execute("UPDATE people SET name = 'changed'", &[]);
    assert_eq!(
        res.unwrap_err().to_string(),
        "db error: ERROR: Strict check failed: maintain scope is required"
    );

    let res = client.execute("DELETE FROM people", &[]);
    assert_eq!(
        res.unwrap_err().to_string(),
        "db error: ERROR: Strict check failed: maintain scope is required"
    );
}

#[test]
#[serial]
fn test_maintain_scope_grants_crud_access() {
    ansilo_logging::init_for_tests();
    let (_instance, port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    let token = make_rsa_token(
        json!({"scope": "maintain", "exp": valid_exp()}),
        include_bytes!("keys/private.key"),
    );
    let mut client = ansilo_e2e::util::main::connect_opts("token", &token, port, |_| ()).unwrap();

    let rows = client.query("SELECT * FROM people", &[]).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>("name"), "John".to_string());

    let inserted = client
        .execute("INSERT INTO people VALUES (1, 'new')", &[])
        .unwrap();
    assert_eq!(inserted, 1);

    let updated = client
        .execute("UPDATE people SET name = 'changed'", &[])
        .unwrap();
    assert_eq!(updated, 2);

    let deleted = client.execute("DELETE FROM people", &[]).unwrap();
    assert_eq!(deleted, 2);
}

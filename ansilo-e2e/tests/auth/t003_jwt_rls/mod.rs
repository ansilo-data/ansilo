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

    let rows = client.query("SELECT * FROM storage", &[]).unwrap();
    assert!(rows.is_empty());

    let res = client
        .query("INSERT INTO storage VALUES ('new')", &[])
        .err();
    assert_eq!(
        res.unwrap().to_string(),
        "db error: ERROR: new row violates row-level security policy for table \"storage\""
    );

    let updated = client
        .execute("UPDATE storage SET data = 'changed'", &[])
        .unwrap();
    assert_eq!(updated, 0);

    let deleted = client.execute("DELETE FROM storage", &[]).unwrap();
    assert_eq!(deleted, 0);
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

    let rows = client.query("SELECT * FROM storage", &[]).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "secret".to_string());

    let res = client
        .query("INSERT INTO storage VALUES ('new')", &[])
        .err();
    assert_eq!(
        res.unwrap().to_string(),
        "db error: ERROR: new row violates row-level security policy for table \"storage\""
    );

    let updated = client
        .execute("UPDATE storage SET data = 'changed'", &[])
        .unwrap();
    assert_eq!(updated, 0);

    let deleted = client.execute("DELETE FROM storage", &[]).unwrap();
    assert_eq!(deleted, 0);
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

    let rows = client.query("SELECT * FROM storage", &[]).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "secret".to_string());

    let inserted = client
        .execute("INSERT INTO storage VALUES ('new')", &[])
        .unwrap();
    assert_eq!(inserted, 1);

    let updated = client
        .execute("UPDATE storage SET data = 'changed'", &[])
        .unwrap();
    assert_eq!(updated, 2);

    let deleted = client.execute("DELETE FROM storage", &[]).unwrap();
    assert_eq!(deleted, 2);
}

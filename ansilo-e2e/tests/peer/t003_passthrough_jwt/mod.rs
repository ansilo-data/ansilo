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
fn test_succeeds_with_valid_token_with_required_scope() {
    ansilo_logging::init_for_tests();
    #[allow(unused)]
    let [(peer_instance, _), (main_instance, _)] = ansilo_e2e::peer::run_instances([
        ("PEER", current_dir!().join("peer-config.yml")),
        ("MAIN", current_dir!().join("main-config.yml")),
    ]);

    let token = make_rsa_token(
        json!({"scope": "read_people", "exp": valid_exp()}),
        include_bytes!("keys/private.key"),
    );
    let mut main_client =
        ansilo_e2e::util::main::connect_to_as_user(&main_instance, "token", &token);

    let rows = main_client
        .query(
            r#"
            SELECT * FROM people;
            "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>("name"), "Elizabeth");
    assert_eq!(rows[0].get::<_, i32>("age"), 20);
}

#[test]
#[serial]
fn test_fails_with_valid_token_without_required_scope() {
    ansilo_logging::init_for_tests();
    #[allow(unused)]
    let [(peer_instance, _), (main_instance, _)] = ansilo_e2e::peer::run_instances([
        ("PEER", current_dir!().join("peer-config.yml")),
        ("MAIN", current_dir!().join("main-config.yml")),
    ]);

    let token = make_rsa_token(
        json!({"scope": "wrong_scope", "exp": valid_exp()}),
        include_bytes!("keys/private.key"),
    );
    let mut main_client =
        ansilo_e2e::util::main::connect_to_as_user(&main_instance, "token", &token);

    let res = main_client
        .query(
            r#"
            SELECT * FROM people;
            "#,
            &[],
        )
        .unwrap_err();

    dbg!(res.to_string());
    assert!(res
        .to_string()
        .contains("scope \"read_people\" is required"));
}

#[test]
#[serial]
fn test_fails_with_invalid_token() {
    ansilo_logging::init_for_tests();
    #[allow(unused)]
    let [(peer_instance, _), (main_instance, _)] = ansilo_e2e::peer::run_instances([
        ("PEER", current_dir!().join("peer-config.yml")),
        ("MAIN", current_dir!().join("main-config.yml")),
    ]);

    let token = make_rsa_token(
        json!({"scope": "read_people", "exp": valid_exp()}),
        include_bytes!("keys/foreign-private.key"),
    );
    let mut main_client =
        ansilo_e2e::util::main::connect_to_as_user(&main_instance, "token2", &token);

    let res = main_client
        .query(
            r#"
            SELECT * FROM people;
            "#,
            &[],
        )
        .unwrap_err();

    dbg!(res.to_string());
    assert!(res.to_string().contains("Failed to authenticate JWT"));
}

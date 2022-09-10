use std::env;

use ansilo_e2e::current_dir;
use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
fn test_succeeds_with_valid_user() {
    ansilo_logging::init_for_tests();
    #[allow(unused)]
    let [(peer_instance, mut peer_client), (main_instance, mut main_client)] =
        ansilo_e2e::peer::run_instances([
            ("PEER", current_dir!().join("peer-config.yml")),
            ("MAIN", current_dir!().join("main-config.yml")),
        ]);

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
fn test_fails_with_invalid_user() {
    ansilo_logging::init_for_tests();
    #[allow(unused)]
    let [(peer_instance, mut peer_client), (main_instance, mut main_client)] =
        ansilo_e2e::peer::run_instances([
            ("PEER", current_dir!().join("peer-config.yml")),
            ("MAIN", current_dir!().join("main-config.yml")),
        ]);

    let mut main_client =
        ansilo_e2e::util::main::connect_to_as_user(&main_instance, "user", "pass1");

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
        .contains("db error: ERROR: User 'user' does not exist"));
}

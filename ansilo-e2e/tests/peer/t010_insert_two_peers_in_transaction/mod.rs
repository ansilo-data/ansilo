use std::env;

use ansilo_e2e::current_dir;
use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
fn test_commit() {
    ansilo_logging::init_for_tests();
    #[allow(unused)]
    let [(peer1_instance, mut peer1_client), (peer2_instance, mut peer2_client), (main_instance, mut main_client)] =
        ansilo_e2e::peer::run_instances([
            ("PEER1", current_dir!().join("peer-1-config.yml")),
            ("PEER2", current_dir!().join("peer-2-config.yml")),
            ("MAIN", current_dir!().join("main-config.yml")),
        ]);

    main_client.execute("BEGIN", &[]).unwrap();

    let rows = main_client
        .execute(r#"INSERT INTO people (id, name) VALUES (1, 'Jared');"#, &[])
        .unwrap();
    assert_eq!(rows, 1);

    let rows = main_client
        .execute(
            r#"INSERT INTO pets (id, name, owner_id) VALUES (5, 'Luna', 1);"#,
            &[],
        )
        .unwrap();
    assert_eq!(rows, 1);

    // Check data is not visible to client connections yet
    let rows = peer1_client.query(r#"SELECT * FROM people"#, &[]).unwrap();
    assert_eq!(rows.len(), 0);

    let rows = peer2_client.query(r#"SELECT * FROM pets"#, &[]).unwrap();
    assert_eq!(rows.len(), 0);

    main_client.execute("COMMIT", &[]).unwrap();

    // Check data is visible to client after commit
    let rows = peer1_client.query(r#"SELECT * FROM people"#, &[]).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        (
            rows[0].get::<_, i64>("id"),
            rows[0].get::<_, String>("name")
        ),
        (1, "Jared".to_string())
    );

    let rows = peer2_client.query(r#"SELECT * FROM pets"#, &[]).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        (
            rows[0].get::<_, i64>("id"),
            rows[0].get::<_, String>("name"),
            rows[0].get::<_, i64>("owner_id")
        ),
        (5, "Luna".to_string(), 1)
    );
}

#[test]
#[serial]
fn test_rollback() {
    ansilo_logging::init_for_tests();
    #[allow(unused)]
    let [(peer1_instance, mut peer1_client), (peer2_instance, mut peer2_client), (main_instance, mut main_client)] =
        ansilo_e2e::peer::run_instances([
            ("PEER1", current_dir!().join("peer-1-config.yml")),
            ("PEER2", current_dir!().join("peer-2-config.yml")),
            ("MAIN", current_dir!().join("main-config.yml")),
        ]);

    main_client.execute("BEGIN", &[]).unwrap();

    let rows = main_client
        .execute(r#"INSERT INTO people (id, name) VALUES (1, 'Jared');"#, &[])
        .unwrap();
    assert_eq!(rows, 1);

    let rows = main_client
        .execute(
            r#"INSERT INTO pets (id, name, owner_id) VALUES (5, 'Luna', 1);"#,
            &[],
        )
        .unwrap();
    assert_eq!(rows, 1);

    // Check data is not visible to client connections yet
    let rows = peer1_client.query(r#"SELECT * FROM people"#, &[]).unwrap();
    assert_eq!(rows.len(), 0);

    let rows = peer2_client.query(r#"SELECT * FROM pets"#, &[]).unwrap();
    assert_eq!(rows.len(), 0);

    main_client.execute("ROLLBACK", &[]).unwrap();

    // Check data is still not visible after rollback
    let rows = peer1_client.query(r#"SELECT * FROM people"#, &[]).unwrap();
    assert_eq!(rows.len(), 0);

    let rows = peer2_client.query(r#"SELECT * FROM pets"#, &[]).unwrap();
    assert_eq!(rows.len(), 0);
}

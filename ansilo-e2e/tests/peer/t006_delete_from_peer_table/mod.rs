use std::env;

use ansilo_connectors_base::interface::LoggedQuery;
use ansilo_e2e::current_dir;
use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
fn test() {
    ansilo_logging::init_for_tests();
    #[allow(unused)]
    let [(peer_instance, mut peer_client), (main_instance, mut main_client)] =
        ansilo_e2e::peer::run_instances([
            ("PEER", current_dir!().join("peer-config.yml")),
            ("MAIN", current_dir!().join("main-config.yml")),
        ]);

    let rows = main_client
        .execute(
            r#"
            DELETE FROM people
            WHERE id = 1;
            "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 1);

    let rows = peer_client
        .query(
            r#"
            SELECT * FROM people;
            "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("id"), 2);
    assert_eq!(rows[0].get::<_, String>("name"), "Mary");

    assert_eq!(
        main_instance.log().get_from_memory().unwrap(),
        vec![(
            "peer".to_string(),
            LoggedQuery::new(
                [
                    r#"DELETE FROM "public"."people" "#,
                    r#"WHERE (("people"."id") = ($1))"#,
                ]
                .join(""),
                vec![
                    "value=Int32(1) type=int4".into(),
                ],
                Some(
                    [("affected".into(), "Some(1)".into())]
                        .into_iter()
                        .collect()
                )
            )
        )]
    );
}

use std::env;

use ansilo_connectors_base::interface::LoggedQuery;
use ansilo_e2e::current_dir;
use itertools::Itertools;
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
        .query(
            r#"
            SELECT 
                max_age, 
                (
                    SELECT COUNT(*)::int FROM people
                    WHERE people.age < max_age
                ) as count
            FROM generate_series(0, 50, 5) AS max_age
            "#,
            &[],
        )
        .unwrap();

    let rows = rows
        .iter()
        .map(|r| (r.get::<_, i32>("max_age"), r.get::<_, i32>("count")))
        .collect_vec();

    assert_eq!(
        rows,
        vec![
            (0, 0),
            (5, 1),
            (10, 3),
            (15, 3),
            (20, 5),
            (25, 6),
            (30, 6),
            (35, 6),
            (40, 6),
            (45, 6),
            (50, 6)
        ]
    );

    // Query: main node -> peer
    assert_eq!(
        main_instance.log().get_from_memory().unwrap(),
        (0..=50).step_by(5).map(|x| (
            "peer".to_string(),
            LoggedQuery::new(
                r#"SELECT count(*) AS "c0" FROM "public"."people" AS "t1" WHERE (("t1"."age") < ($1))"#,
                vec![format!("value=Int32({x}) type=int8")],
                None
            )
        )).collect::<Vec<_>>()
    );
}

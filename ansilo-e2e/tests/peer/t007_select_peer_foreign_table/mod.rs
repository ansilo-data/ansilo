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
        .query(
            r#"
            SELECT * FROM people;
            "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>("name"), "John");
    assert_eq!(rows[0].get::<_, i64>("age"), 17);
    assert_eq!(rows[1].get::<_, String>("name"), "Mary");
    assert_eq!(rows[1].get::<_, i64>("age"), 18);

    // Query: main node -> peer
    assert_eq!(
        main_instance.log().get_from_memory().unwrap(),
        vec![(
            "peer".to_string(),
            LoggedQuery::new(
                [
                    r#"SELECT "t1"."name" AS "c0", "t1"."age" AS "c1" "#,
                    r#"FROM "public"."people" AS "t1""#,
                ]
                .join(""),
                vec![],
                None
            )
        )]
    );

    // Query: peer -> memory connector
    assert_eq!(
        peer_instance.log().get_from_memory().unwrap(),
        vec![(
            "memory".to_string(),
            LoggedQuery::new(
                [
                    r#"MemoryQuery { query: "#,
                    r#"Select(Select { cols: ["#,
                    r#"("c0", Attribute(AttributeId { entity_alias: "t1", attribute_id: "name" })), "#,
                    r#"("c1", Attribute(AttributeId { entity_alias: "t1", attribute_id: "age" }))"#,
                    r#"], from: "#,
                    r#"EntitySource { entity: EntityId { entity_id: "people" }, alias: "t1" }, "#,
                    r#"joins: [], where: [], group_bys: [], order_bys: [], row_limit: None, row_skip: 0, row_lock: None }), "#,
                    r#"params: [] }"#,
                ]
                .join(""),
                vec![],
                None
            )
        )]
    );
}

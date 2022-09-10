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
    let [(peer1_instance, _), (peer2_instance, _), (main_instance, mut main_client)] =
        ansilo_e2e::peer::run_instances([
            ("PEER1", current_dir!().join("peer-1-config.yml")),
            ("PEER2", current_dir!().join("peer-2-config.yml")),
            ("MAIN", current_dir!().join("main-config.yml")),
        ]);

    let rows = main_client
        .query(
            r#"
            SELECT 
                h.id as owner_id,
                h.name as owner_name,
                p.id as pet_id, 
                p.name as pet_name 
            FROM people h
            INNER JOIN pets p ON p.owner_id = h.id;
            "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows.len(), 4);

    let rows = rows
        .into_iter()
        .map(|r| {
            (
                r.get::<_, i64>("owner_id"),
                r.get::<_, String>("owner_name"),
                r.get::<_, i64>("pet_id"),
                r.get::<_, String>("pet_name"),
            )
        })
        .collect_vec();

    assert_eq!(
        rows,
        vec![
            (1, "John".to_string(), 1, "Pepper".to_string()),
            (2, "Mary".to_string(), 2, "Relish".to_string()),
            (3, "Emma".to_string(), 3, "Salt".to_string()),
            (3, "Emma".to_string(), 4, "Luna".to_string()),
        ]
    );

    // Query: main node -> peer1, peer2
    assert_eq!(
        main_instance.log().get_from_memory().unwrap(),
        vec![
            (
                "peer2_pets".to_string(),
                LoggedQuery::new(
                    [
                        r#"SELECT "t1"."id" AS "c0", "t1"."name" AS "c1", "t1"."owner_id" AS "c2" "#,
                        r#"FROM "public"."pets" AS "t1""#,
                    ]
                    .join(""),
                    vec![],
                    None
                )
            ),
            (
                "peer1_people".to_string(),
                LoggedQuery::new(
                    [
                        r#"SELECT "t1"."id" AS "c0", "t1"."name" AS "c1" "#,
                        r#"FROM "public"."people" AS "t1""#,
                    ]
                    .join(""),
                    vec![],
                    None
                )
            ),
        ]
    );

    // Query: peer1 -> memory connector
    assert_eq!(
        peer1_instance.log().get_from_memory().unwrap(),
        vec![(
            "memory".to_string(),
            LoggedQuery::new(
                [
                    r#"MemoryQuery { query: "#,
                    r#"Select(Select { cols: ["#,
                    r#"("c0", Attribute(AttributeId { entity_alias: "t1", attribute_id: "id" })), "#,
                    r#"("c1", Attribute(AttributeId { entity_alias: "t1", attribute_id: "name" }))"#,
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

    // Query: peer2 -> memory connector
    assert_eq!(
        peer2_instance.log().get_from_memory().unwrap(),
        vec![(
            "memory".to_string(),
            LoggedQuery::new(
                [
                    r#"MemoryQuery { query: "#,
                    r#"Select(Select { cols: ["#,
                    r#"("c0", Attribute(AttributeId { entity_alias: "t1", attribute_id: "id" })), "#,
                    r#"("c1", Attribute(AttributeId { entity_alias: "t1", attribute_id: "name" })), "#,
                    r#"("c2", Attribute(AttributeId { entity_alias: "t1", attribute_id: "owner_id" }))"#,
                    r#"], from: "#,
                    r#"EntitySource { entity: EntityId { entity_id: "pets" }, alias: "t1" }, "#,
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

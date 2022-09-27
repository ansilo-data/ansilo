use std::env;

use ansilo_connectors_base::interface::LoggedQuery;
use ansilo_connectors_native_mongodb::bson::{doc, Document};
use ansilo_e2e::current_dir;
use pretty_assertions::assert_eq;
use serde_json::json;
use serial_test::serial;

#[test]
#[serial]
fn test_insert_select_local_values() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mongodb::start_mongodb();
    let mongodb =
        ansilo_e2e::mongodb::init_mongodb(&containers, current_dir!().join("mongodb-js/*.json"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .execute(
            r#"
            INSERT INTO "t007__test_target" (doc)
            SELECT '{"_id": 1, "name": "Jack"}'::jsonb
            UNION ALL
            SELECT '{"_id": 2, "name": "Jerry"}'::jsonb
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 2);

    // Check data received on mongodb end
    let results = mongodb
        .client()
        .default_database()
        .unwrap()
        .collection::<Document>("t007__test_target")
        .find(None, None)
        .unwrap()
        .map(|r| r.unwrap())
        .collect::<Vec<_>>();

    assert_eq!(
        results,
        vec![
            doc! {
                "_id": 1,
                "name": "Jack"
            },
            doc! {
                "_id": 2,
                "name": "Jerry"
            },
        ],
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![
            ("mongodb".to_string(), LoggedQuery::new_query("BEGIN")),
            (
                "mongodb".to_string(),
                LoggedQuery::new(
                    serde_json::to_string_pretty(&json!({
                      "database": "db",
                      "collection": "t007__test_target",
                      "q": {
                        "InsertMany": {
                          "docs": [
                            { "_id": 1, "name": "Jack" },
                            { "_id": 2, "name": "Jerry" },
                          ]
                        }
                      },
                      "params": [
                        { "Dynamic": { "type": "JSON", "id": 1 } },
                        { "Dynamic": { "type": "JSON", "id": 2 } },
                      ]
                    }))
                    .unwrap(),
                    vec![],
                    Some(
                        [("affected".into(), "Some(2)".into())]
                            .into_iter()
                            .collect()
                    )
                )
            ),
            ("mongodb".to_string(), LoggedQuery::new_query("COMMIT")),
        ]
    );
}

#[test]
#[serial]
fn test_insert_select_from_remote_table() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mongodb::start_mongodb();
    let mongodb =
        ansilo_e2e::mongodb::init_mongodb(&containers, current_dir!().join("mongodb-js/*.json"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .execute(
            r#"
            INSERT INTO "t007__test_target" (doc)
            SELECT doc || '{"source": "remote"}'::jsonb
            FROM "t007__test_source"
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 3);

    // Check data received on mongodb end
    let results = mongodb
        .client()
        .default_database()
        .unwrap()
        .collection::<Document>("t007__test_target")
        .find(None, None)
        .unwrap()
        .map(|r| r.unwrap())
        .collect::<Vec<_>>();

    assert_eq!(
        results,
        vec![
            doc! {
                "_id": 1,
                "name": "John",
                "source": "remote"
            },
            doc! {
                "_id": 2,
                "name": "Emma",
                "source": "remote"
            },
            doc! {
                "_id": 3,
                "name": "Jane",
                "source": "remote"
            },
        ],
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![
            ("mongodb".to_string(), LoggedQuery::new_query("BEGIN")),
            (
                "mongodb".to_string(),
                LoggedQuery::new_query(
                    serde_json::to_string_pretty(&json!({
                        "database": "db",
                        "collection": "t007__test_source",
                        "q": {
                            "Find": {
                                "filter": null,
                                "sort": null,
                                "skip": null,
                                "limit": null
                            }
                        },
                        "params": []
                    }))
                    .unwrap()
                ),
            ),
            (
                "mongodb".to_string(),
                LoggedQuery::new(
                    serde_json::to_string_pretty(&json!({
                      "database": "db",
                      "collection": "t007__test_target",
                      "q": {
                        "InsertMany": {
                          "docs": [
                            { "_id": 1, "name": "John", "source": "remote" },
                            { "_id": 2, "name": "Emma", "source": "remote" },
                            { "_id": 3, "name": "Jane", "source": "remote" },
                          ]
                        }
                      },
                      "params": [
                        { "Dynamic": { "type": "JSON", "id": 1 } },
                        { "Dynamic": { "type": "JSON", "id": 2 } },
                        { "Dynamic": { "type": "JSON", "id": 3 } },
                      ]
                    }))
                    .unwrap(),
                    vec![],
                    Some(
                        [("affected".into(), "Some(3)".into())]
                            .into_iter()
                            .collect()
                    )
                )
            ),
            ("mongodb".to_string(), LoggedQuery::new_query("COMMIT")),
        ]
    );
}

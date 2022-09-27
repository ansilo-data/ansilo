use std::env;

use ansilo_connectors_base::interface::LoggedQuery;
use ansilo_connectors_native_mongodb::bson::{doc, Document};
use ansilo_e2e::current_dir;
use pretty_assertions::assert_eq;
use serde_json::json;
use serial_test::serial;

#[test]
#[serial]
fn test_update_where_remote() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mongodb::start_mongodb();
    let mongodb =
        ansilo_e2e::mongodb::init_mongodb(&containers, current_dir!().join("mongodb-js/*.json"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .execute(
            r#"
            UPDATE t008__test_col
            SET doc = '{"_id": 2, "name": "Updated"}'::jsonb
            WHERE doc->'_id' = '2'::jsonb
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 1);

    // Check data received on mongodb end
    let results = mongodb
        .client()
        .default_database()
        .unwrap()
        .collection::<Document>("t008__test_col")
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
            },
            doc! {
                "_id": 2,
                "name": "Updated",
            },
            doc! {
                "_id": 3,
                "name": "Jane",
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
                      "collection": "t008__test_col",
                      "q": {
                        "UpdateMany": {
                          "pipeline": [
                            {
                              "$replaceRoot": {
                                "newRoot": {
                                  "_id": 2,
                                  "name": "Updated"
                                }
                              }
                            }
                          ],
                          "filter": {
                            "$and": [
                              {
                                "_id": {
                                  "$eq": 2
                                }
                              }
                            ]
                          }
                        }
                      },
                      "params": []
                    }))
                    .unwrap(),
                    vec![],
                    Some(
                        [("affected".into(), "Some(1)".into())]
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
fn test_update_where_local() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mongodb::start_mongodb();
    let mongodb =
        ansilo_e2e::mongodb::init_mongodb(&containers, current_dir!().join("mongodb-js/*.json"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .execute(
            r#"
            UPDATE t008__test_col
            SET doc = doc || '{"name": "Updated!"}'::jsonb
            WHERE doc->'_id' = '2'::jsonb
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 1);

    // Check data received on mongodb end
    let results = mongodb
        .client()
        .default_database()
        .unwrap()
        .collection::<Document>("t008__test_col")
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
            },
            doc! {
                "_id": 2,
                "name": "Updated!",
            },
            doc! {
                "_id": 3,
                "name": "Jane",
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
                        "collection": "t008__test_col",
                        "q": {
                            "Find": {
                                "filter": { "$and": [ { "_id": { "$eq": 2 } } ] },
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
                      "collection": "t008__test_col",
                      "q": {
                        "UpdateMany": {
                          "pipeline": [
                            {
                              "$replaceRoot": {
                                "newRoot": {
                                  "_id": 2,
                                  "name": "Updated!",
                                }
                              }
                            }
                          ],
                          "filter": {
                              "$and": [ { "_id": { "$eq": 2 } } ]
                          },
                        },
                      },
                      "params": [
                        { "Dynamic": { "type": "JSON", "id": 1 } },
                        { "Dynamic": { "type": "JSON", "id": 2 } },
                      ]
                    }))
                    .unwrap(),
                    vec![],
                    Some(
                        [("affected".into(), "Some(1)".into())]
                            .into_iter()
                            .collect()
                    )
                )
            ),
            ("mongodb".to_string(), LoggedQuery::new_query("COMMIT")),
        ]
    );
}

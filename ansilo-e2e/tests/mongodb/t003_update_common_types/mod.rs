use std::env;

use ansilo_connectors_base::interface::LoggedQuery;
use ansilo_connectors_native_mongodb::bson::{doc, Binary, Bson, Document};
use ansilo_e2e::current_dir;
use pretty_assertions::assert_eq;
use serde_json::json;

#[test]
fn test() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mongodb::start_mongodb();
    let mongodb =
        ansilo_e2e::mongodb::init_mongodb(&containers, current_dir!().join("mongodb-js/*.json"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .execute(
            r#"
            UPDATE "t003__test_col"
            SET
                "doc" = "doc" || '{
                    "str": "🥑🚀",
                    "num": 1234,
                    "null": null,
                    "bool": true,
                    "bin": {
                        "$binary": {
                            "base64": "aGVsbG8=",
                            "subType": "FF"
                        }
                    }
                }'::jsonb
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 1);

    // Check data received on mongodb end
    let doc = mongodb
        .client()
        .default_database()
        .unwrap()
        .collection::<Document>("t003__test_col")
        .find_one(
            doc! {"_id": Bson::ObjectId("63324fce9e5a26419f67a502".parse().unwrap())},
            None,
        )
        .unwrap()
        .unwrap();

    assert_eq!(
        doc,
        doc! {
            "_id": Bson::ObjectId("63324fce9e5a26419f67a502".parse().unwrap()),
            "str": "🥑🚀",
            "num": 1234,
            "null": null,
            "bool": true,
            "bin": Bson::Binary(Binary {
                subtype: 255.into(),
                bytes: b"hello".to_vec()
            })
        }
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
                        "collection": "t003__test_col",
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
                      "collection": "t003__test_col",
                      "q": {
                        "UpdateMany": {
                          "pipeline": [
                            {
                              "$replaceRoot": {
                                "newRoot": {
                                  "_id": {
                                    "$oid": "63324fce9e5a26419f67a502"
                                  },
                                  "bin": {
                                    "$binary": {
                                      "base64": "aGVsbG8=",
                                      "subType": "ff"
                                    }
                                  },
                                  "num": 1234,
                                  "str": "🥑🚀",
                                  "bool": true,
                                  "null": null
                                }
                              }
                            }
                          ],
                          "filter": {
                            "$and": [
                              {
                                "_id": {
                                  "$eq": {
                                    "$oid": "63324fce9e5a26419f67a502"
                                  }
                                }
                              }
                            ]
                          }
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

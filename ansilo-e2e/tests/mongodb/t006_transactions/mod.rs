use std::env;

use ansilo_connectors_base::interface::LoggedQuery;
use ansilo_connectors_native_mongodb::bson::{doc, Bson, Document};
use ansilo_e2e::current_dir;
use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
fn test_transaction_commit() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mongodb::start_mongodb();
    let mongodb =
        ansilo_e2e::mongodb::init_mongodb(&containers, current_dir!().join("mongodb-js/*.json"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    client
        .batch_execute(
            r#"
        BEGIN;
        INSERT INTO "t006__test_col" (doc) VALUES ('{
            "_id": { "$oid": "6332f1f742a7a1f237b8efdb" },
            "hello": "world"
        }');
        COMMIT;
        "#,
        )
        .unwrap();

    // Check data received on mongodb end
    let results = mongodb
        .client()
        .default_database()
        .unwrap()
        .collection::<Document>("t006__test_col")
        .find(None, None)
        .unwrap()
        .map(|r| r.unwrap())
        .collect::<Vec<_>>();

    assert_eq!(
        results,
        vec![doc! {
            "_id": Bson::ObjectId("6332f1f742a7a1f237b8efdb".parse().unwrap()),
            "hello": "world"
        }]
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![
            ("mongodb".to_string(), LoggedQuery::new_query("BEGIN")),
            (
                "mongodb".to_string(),
                LoggedQuery::new(
                    serde_json::to_string_pretty(&serde_json::json!(
                        {
                            "database": "db",
                            "collection": "t006__test_col",
                            "q": {
                                "InsertMany": {
                                    "docs": [{
                                        "_id": { "$oid": "6332f1f742a7a1f237b8efdb" },
                                        "hello": "world"
                                    }]
                                }
                            },
                            "params": [
                                {
                                    "Dynamic": {
                                        "type": "JSON",
                                        "id": 1
                                    }
                                }
                            ]
                        }
                    ))
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
fn test_transaction_rollback() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mongodb::start_mongodb();
    let mongodb =
        ansilo_e2e::mongodb::init_mongodb(&containers, current_dir!().join("mongodb-js/*.json"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    client
        .batch_execute(
            r#"
        BEGIN;
        INSERT INTO "t006__test_col" (doc) VALUES ('{
            "_id": { "$oid": "6332f1f742a7a1f237b8efdb" },
            "hello": "world"
        }');
        ROLLBACK;
        "#,
        )
        .unwrap();

    // Check rolled back on mongodb side
    let results = mongodb
        .client()
        .default_database()
        .unwrap()
        .collection::<Document>("t006__test_col")
        .find(None, None)
        .unwrap()
        .map(|r| r.unwrap())
        .collect::<Vec<_>>();

    assert_eq!(results, vec![]);

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![
            ("mongodb".to_string(), LoggedQuery::new_query("BEGIN")),
            (
                "mongodb".to_string(),
                LoggedQuery::new(
                    serde_json::to_string_pretty(&serde_json::json!(
                        {
                            "database": "db",
                            "collection": "t006__test_col",
                            "q": {
                                "InsertMany": {
                                    "docs": [{
                                        "_id": { "$oid": "6332f1f742a7a1f237b8efdb" },
                                        "hello": "world"
                                    }]
                                }
                            },
                            "params": [
                                {
                                    "Dynamic": {
                                        "type": "JSON",
                                        "id": 1
                                    }
                                }
                            ]
                        }
                    ))
                    .unwrap(),
                    vec![],
                    Some(
                        [("affected".into(), "Some(1)".into())]
                            .into_iter()
                            .collect()
                    )
                )
            ),
            ("mongodb".to_string(), LoggedQuery::new_query("ROLLBACK")),
        ]
    );
}

#[test]
#[serial]
fn test_transaction_rollback_due_to_error() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mongodb::start_mongodb();
    let mongodb =
        ansilo_e2e::mongodb::init_mongodb(&containers, current_dir!().join("mongodb-js/*.json"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    client
        .batch_execute(
            r#"
        BEGIN;

        INSERT INTO "t006__test_col" (doc) VALUES ('{
            "_id": { "$oid": "6332f1f742a7a1f237b8efdb" },
            "hello": "world"
        }');

        DO $$BEGIN
            RAISE EXCEPTION "An error occurred!";
        END$$;
        "#,
        )
        .unwrap_err();

    // After the error the rollback occurs asynchronously from this thread
    // drop the client to ensure we wait for the connection to finish
    // processing and the rollback to be issued
    drop(client);

    // Check rolled back on mongodb side
    let results = mongodb
        .client()
        .default_database()
        .unwrap()
        .collection::<Document>("t006__test_col")
        .find(None, None)
        .unwrap()
        .map(|r| r.unwrap())
        .collect::<Vec<_>>();

    assert_eq!(results, vec![]);

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![
            ("mongodb".to_string(), LoggedQuery::new_query("BEGIN")),
            (
                "mongodb".to_string(),
                LoggedQuery::new(
                    serde_json::to_string_pretty(&serde_json::json!(
                        {
                            "database": "db",
                            "collection": "t006__test_col",
                            "q": {
                                "InsertMany": {
                                    "docs": [{
                                        "_id": { "$oid": "6332f1f742a7a1f237b8efdb" },
                                        "hello": "world"
                                    }]
                                }
                            },
                            "params": [
                                {
                                    "Dynamic": {
                                        "type": "JSON",
                                        "id": 1
                                    }
                                }
                            ]
                        }
                    ))
                    .unwrap(),
                    vec![],
                    Some(
                        [("affected".into(), "Some(1)".into())]
                            .into_iter()
                            .collect()
                    )
                )
            ),
            ("mongodb".to_string(), LoggedQuery::new_query("ROLLBACK")),
        ]
    );
}

use std::env;

use ansilo_connectors_base::interface::LoggedQuery;

use ansilo_connectors_native_mongodb::bson::Document;
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
        .execute(r#"DELETE FROM "t004__test_col""#, &[])
        .unwrap();

    assert_eq!(rows, 2);

    // Check documents removed on mongodb end
    let docs = mongodb
        .client()
        .default_database()
        .unwrap()
        .collection::<Document>("t004__test_col")
        .count_documents(None, None)
        .unwrap();

    assert_eq!(docs, 0);

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![
            ("mongodb".to_string(), LoggedQuery::new_query("BEGIN")),
            (
                "mongodb".to_string(),
                LoggedQuery::new(
                    serde_json::to_string_pretty(&json!({
                      "database": "db",
                      "collection": "t004__test_col",
                      "q": {
                        "DeleteMany": {
                          "filter": null
                        }
                      },
                      "params": []
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

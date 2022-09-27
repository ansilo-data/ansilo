use std::env;

use ansilo_connectors_base::interface::LoggedQuery;
use ansilo_e2e::current_dir;
use itertools::Itertools;
use pretty_assertions::assert_eq;
use serde_json::json;

#[test]
fn test() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mongodb::start_mongodb();
    ansilo_e2e::mongodb::init_mongodb(&containers, current_dir!().join("mongodb-js/*.json"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query("SELECT * FROM \"t001__test_col\"", &[])
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0]
            .columns()
            .into_iter()
            .map(|c| c.name())
            .collect_vec(),
        vec!["doc"]
    );
    assert_eq!(
        rows[0].get::<_, serde_json::Value>(0),
        json!({
            "_id": {
                "$oid": "63324fce9e5a26419f67a502",
            },
            "str": "🥑🚀",
            "num": 1234,
            "null": null,
            "bool": true,
            "bin": {
                "$binary": {
                    "base64": "aGVsbG8=",
                    "subType": "ff",
                },
            },
        })
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "mongodb".to_string(),
            LoggedQuery::new_query(
                serde_json::to_string_pretty(&json!({
                    "database": "db",
                    "collection": "t001__test_col",
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
            )
        )]
    );
}

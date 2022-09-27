use std::env;

use ansilo_connectors_base::interface::LoggedQuery;
use ansilo_e2e::current_dir;
use itertools::Itertools;
use pretty_assertions::assert_eq;
use serde_json::json;
use serial_test::serial;

#[test]
#[serial]
fn test() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mongodb::start_mongodb();
    ansilo_e2e::mongodb::init_mongodb(&containers, current_dir!().join("mongodb-js/*.json"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query(
            r#"
            SELECT (doc->'row')::int as num
            FROM "t005__test_col"
            WHERE doc->'row' <= '5'::jsonb
            ORDER BY doc->'row' DESC
            LIMIT 3 OFFSET 1
            "#,
            &[],
        )
        .unwrap();

    assert_eq!(
        rows.into_iter().map(|r| r.get::<_, i32>(0)).collect_vec(),
        vec![4, 3, 2]
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "mongodb".to_string(),
            LoggedQuery::new(
                serde_json::to_string_pretty(&json!({
                  "database": "db",
                  "collection": "t005__test_col",
                  "q": {
                    "Find": {
                      "filter": {
                        "$and": [
                          {
                            "row": {
                              "$lte": 5
                            }
                          }
                        ]
                      },
                      "sort": {
                        "row": -1
                      },
                      "skip": 1,
                      "limit": 3
                    }
                  },
                  "params": []
                }))
                .unwrap(),
                vec![],
                None
            )
        )]
    );
}

use std::env;

use ansilo_connectors_base::interface::{LoggedQuery, ResultSet};
use ansilo_core::{data::DataValue, err::Result};
use ansilo_e2e::current_dir;
use pretty_assertions::assert_eq;
use serial_test::serial;

use ansilo_e2e::util::assert::assert_rows_equal;

#[test]
#[serial]
fn test_remote_select() {
    ansilo_logging::init_for_tests();
    let (_sqlite, sqlite_path) =
        ansilo_e2e::sqlite::init_sqlite_sql(current_dir!().join("sqlite-sql/*.sql"));

    let (instance, mut client) = ansilo_e2e::util::main::run_instance_args(
        current_dir!().join("config.yml"),
        &[("SQLITE_PATH", sqlite_path.to_string_lossy())],
    );

    let rows = client
        .query(
            r#"
            SELECT *
            FROM remote_query(
                'sqlite',
                $$WITH RECURSIVE
cnt(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM cnt WHERE x < ?)
SELECT x, 'num:' || x FROM cnt;$$,
                5
            ) AS t(x INT, s TEXT)
        "#,
            &[],
        )
        .unwrap();

    let rows = rows
        .into_iter()
        .map(|r| (r.get::<_, i32>("x"), r.get::<_, String>("s")))
        .collect::<Vec<_>>();

    assert_eq!(
        rows,
        vec![
            (1, "num:1".to_string()),
            (2, "num:2".to_string()),
            (3, "num:3".to_string()),
            (4, "num:4".to_string()),
            (5, "num:5".to_string()),
        ],
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "sqlite".to_string(),
            LoggedQuery::new(
                [
                    r#"WITH RECURSIVE"#,
                    r#"cnt(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM cnt WHERE x < ?)"#,
                    r#"SELECT x, 'num:' || x FROM cnt;"#
                ]
                .join("\n"),
                vec!["value=Int32(5)".into()],
                None
            )
        ),]
    );
}

#[test]
#[serial]
fn test_remote_execute_insert() {
    ansilo_logging::init_for_tests();
    let (mut sqlite, sqlite_path) =
        ansilo_e2e::sqlite::init_sqlite_sql(current_dir!().join("sqlite-sql/*.sql"));

    let (instance, mut client) = ansilo_e2e::util::main::run_instance_args(
        current_dir!().join("config.yml"),
        &[("SQLITE_PATH", sqlite_path.to_string_lossy())],
    );

    client
        .batch_execute(
            r#"
        SELECT remote_execute(
            'sqlite',
            $$INSERT INTO "t008__test_tab" (data) VALUES (?);$$,
            'test-param'
        );
        "#,
        )
        .unwrap();

    // Check data received on sqlite end
    let results = sqlite
        .execute("SELECT * FROM t008__test_tab", vec![])
        .unwrap()
        .reader()
        .unwrap()
        .iter_rows()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_rows_equal(
        results,
        vec![vec![(
            "data".to_string(),
            DataValue::Utf8String("test-param".into()),
        )]
        .into_iter()
        .collect()],
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "sqlite".to_string(),
            LoggedQuery::new(
                [r#"INSERT INTO "t008__test_tab" "#, r#"(data) VALUES (?);"#].join(""),
                vec!["value=Utf8String(\"test-param\")".into()],
                Some(
                    [("affected".into(), "Some(1)".into())]
                        .into_iter()
                        .collect()
                )
            )
        ),]
    );
}

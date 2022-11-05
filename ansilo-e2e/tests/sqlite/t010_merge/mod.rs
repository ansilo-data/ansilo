use std::env;

use ansilo_connectors_base::interface::LoggedQuery;
use ansilo_e2e::current_dir;
use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
fn test_merge_all() {
    ansilo_logging::init_for_tests();
    let (_sqlite, sqlite_path) =
        ansilo_e2e::sqlite::init_sqlite_sql(current_dir!().join("sqlite-sql/*.sql"));

    let (instance, mut client) = ansilo_e2e::util::main::run_instance_args(
        current_dir!().join("config.yml"),
        &[("SQLITE_PATH", sqlite_path.to_string_lossy())],
    );

    let rows = client
        .execute(
            r#"
            MERGE INTO t010__target t
            USING t010__changes c
            ON t.id = c.id
            WHEN NOT MATCHED AND c.delta > 0 THEN
                INSERT VALUES (c.id, c.delta)
            WHEN MATCHED AND t.counter + c.delta > 0 THEN
                UPDATE SET counter = t.counter + c.delta
            WHEN MATCHED THEN
                DELETE;
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 4);

    let rows = client.query(r#"SELECT * FROM t010__target"#, &[]).unwrap();

    let rows = rows
        .into_iter()
        .map(|r| (r.get::<_, String>("id"), r.get::<_, i32>("counter")))
        .collect::<Vec<_>>();

    assert_eq!(
        rows,
        vec![
            ("b".to_string(), 3),
            ("c".to_string(), 7),
            ("d".to_string(), 1),
        ],
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "sqlite".to_string(),
            LoggedQuery::new(
                [r#"SELECT "t1"."delta" AS "c0", "t1"."id" AS "c1" FROM "t010__changes" AS "t1""#,]
                    .join(""),
                vec![],
                None
            )
        ),]
    );
}

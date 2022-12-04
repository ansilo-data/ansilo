use std::env;

use ansilo_connectors_base::interface::LoggedQuery;
use ansilo_e2e::current_dir;
use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
fn test_explain() {
    ansilo_logging::init_for_tests();
    let (_sqlite, sqlite_path) =
        ansilo_e2e::sqlite::init_sqlite_sql(current_dir!().join("sqlite-sql/*.sql"));

    let (instance, mut client) = ansilo_e2e::util::main::run_instance_args(
        current_dir!().join("config.yml"),
        &[("SQLITE_PATH", sqlite_path.to_string_lossy())],
    );

    let rows = client
        .query(r#"EXPLAIN SELECT * FROM t011__test_tab"#, &[])
        .unwrap();

    let rows = rows
        .into_iter()
        .map(|r| (r.get::<_, String>(0)))
        .collect::<Vec<_>>();

    assert_eq!(
        rows,
        vec![
            r#"Foreign Scan on t011__test_tab  (cost=100.00..100.02 rows=1 width=32)"#.to_string(),
            r#"  Remote Query: SELECT "t1"."data" AS "c0" FROM "t011__test_tab" AS "t1""#
                .to_string(),
        ],
    );

    assert_eq!(instance.log().get_from_memory().unwrap(), vec![]);
}

#[test]
#[serial]
fn test_explain_verbose() {
    ansilo_logging::init_for_tests();
    let (_sqlite, sqlite_path) =
        ansilo_e2e::sqlite::init_sqlite_sql(current_dir!().join("sqlite-sql/*.sql"));

    let (instance, mut client) = ansilo_e2e::util::main::run_instance_args(
        current_dir!().join("config.yml"),
        &[("SQLITE_PATH", sqlite_path.to_string_lossy())],
    );

    let rows = client
        .query(
            r#"EXPLAIN (verbose true) SELECT * FROM t011__test_tab"#,
            &[],
        )
        .unwrap();

    let rows = rows
        .into_iter()
        .map(|r| (r.get::<_, String>(0)))
        .collect::<Vec<_>>();

    assert_eq!(
        rows,
        vec![
            r#"Foreign Scan on public.t011__test_tab  (cost=100.00..100.02 rows=1 width=32)"#
                .to_string(),
            r#"  Output: data"#.to_string(),
            r#"  Remote Query: "#.to_string(),
            r#"    sql: SELECT "t1"."data" AS "c0" FROM "t011__test_tab" AS "t1""#.to_string(),
            r#"    params: "#.to_string(),
            r#"  Local Conds: "#.to_string(),
            r#"  Remote Conds: "#.to_string(),
            r#"  Remote Ops: "#.to_string(),
            r#"      AddColumn: "#.to_string(),
            r#"        0: c0"#.to_string(),
            r#"        1: "#.to_string(),
            r#"          @type: Attribute"#.to_string(),
            r#"          entity_alias: t1"#.to_string(),
            r#"          attribute_id: data"#.to_string(),
        ],
    );

    assert_eq!(instance.log().get_from_memory().unwrap(), vec![]);
}

#[test]
#[serial]
fn test_explain_analyze() {
    ansilo_logging::init_for_tests();
    let (_sqlite, sqlite_path) =
        ansilo_e2e::sqlite::init_sqlite_sql(current_dir!().join("sqlite-sql/*.sql"));

    let (instance, mut client) = ansilo_e2e::util::main::run_instance_args(
        current_dir!().join("config.yml"),
        &[("SQLITE_PATH", sqlite_path.to_string_lossy())],
    );

    let rows = client
        .query(r#"EXPLAIN ANALYZE SELECT * FROM t011__test_tab"#, &[])
        .unwrap();

    let rows = rows
        .into_iter()
        .map(|r| (r.get::<_, String>(0)))
        .collect::<Vec<_>>();

    dbg!(rows.clone());

    assert!(rows[0].starts_with(
        "Foreign Scan on t011__test_tab  (cost=100.00..100.02 rows=1 width=32) (actual time="
    ));
    assert_eq!(
        rows[1],
        r#"  Remote Query: SELECT "t1"."data" AS "c0" FROM "t011__test_tab" AS "t1""#.to_string(),
    );
    assert!(rows[2].starts_with("Planning Time: "));
    assert!(rows[3].starts_with("Execution Time: "));

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "sqlite".into(),
            LoggedQuery::new_query(r#"SELECT "t1"."data" AS "c0" FROM "t011__test_tab" AS "t1""#)
        )]
    );
}

#[test]
#[serial]
fn test_explain_analyze_verbose() {
    ansilo_logging::init_for_tests();
    let (_sqlite, sqlite_path) =
        ansilo_e2e::sqlite::init_sqlite_sql(current_dir!().join("sqlite-sql/*.sql"));

    let (instance, mut client) = ansilo_e2e::util::main::run_instance_args(
        current_dir!().join("config.yml"),
        &[("SQLITE_PATH", sqlite_path.to_string_lossy())],
    );

    let rows = client
        .query(
            r#"EXPLAIN ANALYZE VERBOSE SELECT * FROM t011__test_tab"#,
            &[],
        )
        .unwrap();

    let rows = rows
        .into_iter()
        .map(|r| (r.get::<_, String>(0)))
        .collect::<Vec<_>>();

    dbg!(rows.clone());

    assert!(rows[0].starts_with(
        "Foreign Scan on public.t011__test_tab  (cost=100.00..100.02 rows=1 width=32) (actual time="
    ));
    assert_eq!(rows[1], r#"  Output: data"#.to_string(),);
    assert_eq!(rows[2], r#"  Remote Query: "#.to_string(),);
    assert_eq!(
        rows[3],
        r#"    query: SELECT "t1"."data" AS "c0" FROM "t011__test_tab" AS "t1""#.to_string(),
    );
    assert_eq!(rows[4], r#"    params: "#.to_string(),);
    assert_eq!(rows[5], r#"    other: "#.to_string(),);
    assert!(rows[rows.len() - 2].starts_with("Planning Time: "));
    assert!(rows[rows.len() - 1].starts_with("Execution Time: "));

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "sqlite".into(),
            LoggedQuery::new_query(r#"SELECT "t1"."data" AS "c0" FROM "t011__test_tab" AS "t1""#)
        )]
    );
}

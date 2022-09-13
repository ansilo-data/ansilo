use std::env;

use ansilo_connectors_base::interface::LoggedQuery;
use ansilo_e2e::current_dir;
use itertools::Itertools;
use pretty_assertions::assert_eq;

#[test]
fn test() {
    ansilo_logging::init_for_tests();
    let (_sqlite_con, sqlite_path) =
        ansilo_e2e::sqlite::init_sqlite_sql(current_dir!().join("sqlite-sql/*.sql"));

    let (instance, mut client) = ansilo_e2e::util::main::run_instance_args(
        current_dir!().join("config.yml"),
        &[("SQLITE_PATH", sqlite_path.to_string_lossy())],
    );

    let rows = client.query("SELECT * FROM t001__test_tab", &[]).unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0]
            .columns()
            .into_iter()
            .map(|c| c.name())
            .sorted()
            .collect_vec(),
        vec![
            "col_char",
            "col_varchar",
            "col_decimal",
            "col_int8",
            "col_int16",
            "col_int32",
            "col_int64",
            "col_float",
            "col_double",
            "col_blob",
            "col_date",
            "col_time",
            "col_datetime",
            "col_timestamp",
            "col_null"
        ]
        .into_iter()
        .sorted()
        .collect_vec()
    );
    assert_eq!(rows[0].get::<_, String>(0), "A".to_string());
    assert_eq!(rows[0].get::<_, String>(1), "foobar".to_string());
    assert_eq!(rows[0].get::<_, String>(2), "123.456");
    assert_eq!(rows[0].get::<_, i64>(3), 88);
    assert_eq!(rows[0].get::<_, i64>(4), 5432);
    assert_eq!(rows[0].get::<_, i64>(5), 123456);
    assert_eq!(rows[0].get::<_, i64>(6), -9876543210);
    assert_eq!(rows[0].get::<_, f64>(7), 11.22_f64);
    assert_eq!(rows[0].get::<_, f64>(8), 33.44_f64);
    assert_eq!(rows[0].get::<_, Vec<u8>>(9), b"BLOB".to_vec());
    assert_eq!(rows[0].get::<_, String>(10), "2020-12-23",);
    assert_eq!(rows[0].get::<_, String>(11), "01:02:03");
    assert_eq!(rows[0].get::<_, String>(12), "2018-02-01 01:02:03");
    assert_eq!(rows[0].get::<_, String>(13), "1999-01-15T11:00:00+00:00");
    assert_eq!(rows[0].get::<_, Option<String>>(14), None);

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "sqlite".to_string(),
            LoggedQuery::new_query(
                [
                    r#"SELECT "t1"."col_char" AS "c0", "#,
                    r#""t1"."col_varchar" AS "c1", "#,
                    r#""t1"."col_decimal" AS "c2", "#,
                    r#""t1"."col_int8" AS "c3", "#,
                    r#""t1"."col_int16" AS "c4", "#,
                    r#""t1"."col_int32" AS "c5", "#,
                    r#""t1"."col_int64" AS "c6", "#,
                    r#""t1"."col_float" AS "c7", "#,
                    r#""t1"."col_double" AS "c8", "#,
                    r#""t1"."col_blob" AS "c9", "#,
                    r#""t1"."col_date" AS "c10", "#,
                    r#""t1"."col_time" AS "c11", "#,
                    r#""t1"."col_datetime" AS "c12", "#,
                    r#""t1"."col_timestamp" AS "c13", "#,
                    r#""t1"."col_null" AS "c14" "#,
                    r#"FROM "t001__test_tab" AS "t1""#,
                ]
                .join("")
            )
        )]
    );
}

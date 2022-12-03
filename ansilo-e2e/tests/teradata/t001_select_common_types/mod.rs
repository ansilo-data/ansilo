use std::env;

use ansilo_connectors_base::interface::LoggedQuery;
use ansilo_e2e::current_dir;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use itertools::Itertools;
use pretty_assertions::assert_eq;
use rust_decimal::Decimal;
use serde_json::json;

#[test]
fn test() {
    ansilo_logging::init_for_tests();
    ansilo_e2e::teradata::start_teradata();
    let _teradata =
        ansilo_e2e::teradata::init_teradata_sql(current_dir!().join("teradata-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query("SELECT * FROM \"t001__test_tab\"", &[])
        .unwrap();

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
            "col_clob",
            "col_decimal",
            "col_int8",
            "col_int16",
            "col_int32",
            "col_int64",
            "col_double",
            "col_blob",
            "col_json",
            "col_jsonb",
            "col_date",
            "col_time",
            "col_timestamp",
            "col_timestamp_tz",
            "col_null"
        ]
        .into_iter()
        .sorted()
        .collect_vec()
    );
    assert_eq!(rows[0].get::<_, String>(0), "a".to_string());
    assert_eq!(rows[0].get::<_, String>(1), "foobar".to_string());
    assert_eq!(rows[0].get::<_, String>(2), "🥑🚀".to_string());
    assert_eq!(rows[0].get::<_, Decimal>(3), Decimal::new(123456, 3));
    assert_eq!(rows[0].get::<_, i16>(4), 123);
    assert_eq!(rows[0].get::<_, i16>(5), 5432);
    assert_eq!(rows[0].get::<_, i32>(6), 123456);
    assert_eq!(rows[0].get::<_, i64>(7), -9876543210);
    assert_eq!(rows[0].get::<_, f64>(8), 33.44_f64);
    assert_eq!(rows[0].get::<_, Vec<u8>>(9), b"BLOB".to_vec());
    assert_eq!(
        rows[0].get::<_, serde_json::Value>(10),
        json!({"hello": "🥑"})
    );
    assert_eq!(
        rows[0].get::<_, serde_json::Value>(11),
        json!({"foo": "bar"})
    );
    assert_eq!(
        rows[0].get::<_, NaiveDate>(12),
        NaiveDate::from_ymd_opt(2020, 12, 23).unwrap()
    );
    assert_eq!(
        rows[0].get::<_, NaiveTime>(13),
        NaiveTime::from_hms_opt(1, 2, 3).unwrap()
    );
    assert_eq!(
        rows[0].get::<_, NaiveDateTime>(14),
        NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2018, 2, 1).unwrap(),
            NaiveTime::from_hms_opt(1, 2, 3).unwrap()
        )
    );
    assert_eq!(
        rows[0].get::<_, DateTime<FixedOffset>>(15),
        DateTime::<FixedOffset>::parse_from_rfc3339("1999-01-15T06:00:00+00:00").unwrap()
    );
    assert_eq!(rows[0].get::<_, Option<String>>(16), None);

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "teradata".to_string(),
            LoggedQuery::new_query(
                [
                    r#"SELECT "t1"."col_char" AS "c0", "#,
                    r#""t1"."col_varchar" AS "c1", "#,
                    r#""t1"."col_clob" AS "c2", "#,
                    r#""t1"."col_decimal" AS "c3", "#,
                    r#""t1"."col_int8" AS "c4", "#,
                    r#""t1"."col_int16" AS "c5", "#,
                    r#""t1"."col_int32" AS "c6", "#,
                    r#""t1"."col_int64" AS "c7", "#,
                    r#""t1"."col_double" AS "c8", "#,
                    r#""t1"."col_blob" AS "c9", "#,
                    r#""t1"."col_json" AS "c10", "#,
                    r#""t1"."col_jsonb" AS "c11", "#,
                    r#""t1"."col_date" AS "c12", "#,
                    r#""t1"."col_time" AS "c13", "#,
                    r#""t1"."col_timestamp" AS "c14", "#,
                    r#""t1"."col_timestamp_tz" AS "c15", "#,
                    r#""t1"."col_null" AS "c16" "#,
                    r#"FROM "testdb"."t001__test_tab" AS "t1""#,
                ]
                .join("")
            )
        )]
    );
}

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
    let containers = ansilo_e2e::oracle::start_oracle();
    ansilo_e2e::oracle::init_oracle_sql(&containers, current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query("SELECT * FROM \"T002__TEST_TAB\"", &[])
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
            "COL_CHAR",
            "COL_NCHAR",
            "COL_VARCHAR2",
            "COL_NVARCHAR2",
            "COL_NUMBER",
            "COL_FLOAT",
            "COL_INT8",
            "COL_INT16",
            "COL_INT32",
            "COL_INT64",
            "COL_BINARY_FLOAT",
            "COL_BINARY_DOUBLE",
            "COL_RAW",
            "COL_LONG_RAW",
            "COL_BLOB",
            "COL_CLOB",
            "COL_NCLOB",
            "COL_JSON",
            "COL_DATE",
            "COL_TIMESTAMP",
            "COL_TIMESTAMP_TZ",
            "COL_TIMESTAMP_LTZ",
            "COL_NULL"
        ]
        .into_iter()
        .sorted()
        .collect_vec()
    );
    assert_eq!(rows[0].get::<_, String>(0), "A".to_string());
    assert_eq!(rows[0].get::<_, String>(1), "🔥".to_string());
    assert_eq!(rows[0].get::<_, String>(2), "foobar".to_string());
    assert_eq!(rows[0].get::<_, String>(3), "🥑🚀".to_string());
    assert_eq!(rows[0].get::<_, Decimal>(4), Decimal::new(123456, 3));
    assert_eq!(rows[0].get::<_, Decimal>(5), Decimal::new(56789, 2));
    assert_eq!(rows[0].get::<_, i16>(6), 88);
    assert_eq!(rows[0].get::<_, i16>(7), 5432);
    assert_eq!(rows[0].get::<_, i32>(8), 123456);
    assert_eq!(rows[0].get::<_, i64>(9), -9876543210);
    assert_eq!(rows[0].get::<_, f32>(10), 11.22_f32);
    assert_eq!(rows[0].get::<_, f64>(11), 33.44_f64);
    assert_eq!(rows[0].get::<_, Vec<u8>>(12), b"RAW".to_vec());
    assert_eq!(rows[0].get::<_, Vec<u8>>(13), b"LONG RAW".to_vec());
    assert_eq!(rows[0].get::<_, Vec<u8>>(14), b"BLOB".to_vec());
    assert_eq!(rows[0].get::<_, String>(15), "CLOB".to_string());
    assert_eq!(rows[0].get::<_, String>(16), "🧑‍🚀NCLOB".to_string());
    assert_eq!(
        rows[0].get::<_, serde_json::Value>(17),
        json!({"foo": "bar"})
    );
    assert_eq!(
        rows[0].get::<_, NaiveDate>(18),
        NaiveDate::from_ymd(2020, 12, 23)
    );
    assert_eq!(
        rows[0].get::<_, NaiveDateTime>(19),
        NaiveDateTime::new(
            NaiveDate::from_ymd(2018, 2, 1),
            NaiveTime::from_hms(1, 2, 3)
        )
    );
    assert_eq!(
        rows[0].get::<_, DateTime<FixedOffset>>(20),
        DateTime::<FixedOffset>::parse_from_rfc3339("1999-01-15T11:00:00-05:00").unwrap()
    );
    assert_eq!(
        rows[0].get::<_, DateTime<FixedOffset>>(21),
        DateTime::<FixedOffset>::parse_from_rfc3339("1997-01-31T09:26:56.888+02:00").unwrap()
    );
    assert_eq!(rows[0].get::<_, Option<String>>(22), None);

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "oracle".to_string(),
            LoggedQuery::new_query(
                [
                    r#"SELECT "t1"."COL_CHAR" AS "c0", "#,
                    r#""t1"."COL_NCHAR" AS "c1", "#,
                    r#""t1"."COL_VARCHAR2" AS "c2", "#,
                    r#""t1"."COL_NVARCHAR2" AS "c3", "#,
                    r#""t1"."COL_NUMBER" AS "c4", "#,
                    r#""t1"."COL_FLOAT" AS "c5", "#,
                    r#""t1"."COL_INT8" AS "c6", "#,
                    r#""t1"."COL_INT16" AS "c7", "#,
                    r#""t1"."COL_INT32" AS "c8", "#,
                    r#""t1"."COL_INT64" AS "c9", "#,
                    r#""t1"."COL_BINARY_FLOAT" AS "c10", "#,
                    r#""t1"."COL_BINARY_DOUBLE" AS "c11", "#,
                    r#""t1"."COL_RAW" AS "c12", "#,
                    r#""t1"."COL_LONG_RAW" AS "c13", "#,
                    r#""t1"."COL_BLOB" AS "c14", "#,
                    r#""t1"."COL_CLOB" AS "c15", "#,
                    r#""t1"."COL_NCLOB" AS "c16", "#,
                    r#""t1"."COL_JSON" AS "c17", "#,
                    r#""t1"."COL_DATE" AS "c18", "#,
                    r#""t1"."COL_TIMESTAMP" AS "c19", "#,
                    r#""t1"."COL_TIMESTAMP_TZ" AS "c20", "#,
                    r#""t1"."COL_TIMESTAMP_LTZ" AS "c21", "#,
                    r#""t1"."COL_NULL" AS "c22" "#,
                    r#"FROM "ANSILO_ADMIN"."T002__TEST_TAB" "t1""#
                ]
                .join("")
            )
        )]
    );
}

use std::env;

use ansilo_connectors_base::interface::LoggedQuery;
use ansilo_main::{
    args::{Args, Command},
    Ansilo, RemoteQueryLog,
};
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use itertools::Itertools;
use rust_decimal::Decimal;
use serde_json::json;

#[test]
fn test() {
    ansilo_logging::init_for_tests();
    let containers = super::common::start_oracle();
    super::common::init_oracle_sql(&containers, crate::current_dir!().join("oracle-sql/*.sql"));

    let instance = Ansilo::start(
        Command::Run(Args::testing(crate::current_dir!().join("config.yml"))),
        Some(RemoteQueryLog::store_in_memory()),
    )
    .unwrap();

    let mut client = crate::common::connect(60002);

    let rows = client
        .query("SELECT * FROM \"ANSILO_ADMIN.T002__TEST_TAB\"", &[])
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
    assert_eq!(rows[0].get::<_, f32>(6), 11.22_f32);
    assert_eq!(rows[0].get::<_, f64>(7), 33.44_f64);
    assert_eq!(rows[0].get::<_, Vec<u8>>(8), b"RAW".to_vec());
    assert_eq!(rows[0].get::<_, Vec<u8>>(9), b"LONG RAW".to_vec());
    assert_eq!(rows[0].get::<_, Vec<u8>>(10), b"BLOB".to_vec());
    assert_eq!(rows[0].get::<_, String>(11), "CLOB".to_string());
    assert_eq!(rows[0].get::<_, String>(12), "🧑‍🚀NCLOB".to_string());
    assert_eq!(
        rows[0].get::<_, serde_json::Value>(13),
        json!({"foo": "bar"})
    );
    assert_eq!(
        rows[0].get::<_, NaiveDate>(14),
        NaiveDate::from_ymd(2020, 12, 23)
    );
    assert_eq!(
        rows[0].get::<_, NaiveDateTime>(15),
        NaiveDateTime::new(
            NaiveDate::from_ymd(2018, 2, 1),
            NaiveTime::from_hms(1, 2, 3)
        )
    );
    assert_eq!(
        rows[0].get::<_, DateTime<FixedOffset>>(16),
        DateTime::<FixedOffset>::parse_from_rfc3339("1999-01-15T11:00:00-05:00").unwrap()
    );
    assert_eq!(
        rows[0].get::<_, DateTime<FixedOffset>>(17),
        DateTime::<FixedOffset>::parse_from_rfc3339("1997-01-31T09:26:56.888+02:00").unwrap()
    );
    assert_eq!(rows[0].get::<_, Option<String>>(18), None);

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "oracle".to_string(),
            LoggedQuery::query(
                r#"SELECT "t1"."COL_CHAR" AS "c0", "t1"."COL_NCHAR" AS "c1", "t1"."COL_VARCHAR2" AS "c2", "t1"."COL_NVARCHAR2" AS "c3", "t1"."COL_NUMBER" AS "c4", "t1"."COL_FLOAT" AS "c5", "t1"."COL_BINARY_FLOAT" AS "c6", "t1"."COL_BINARY_DOUBLE" AS "c7", "t1"."COL_RAW" AS "c8", "t1"."COL_LONG_RAW" AS "c9", "t1"."COL_BLOB" AS "c10", "t1"."COL_CLOB" AS "c11", "t1"."COL_NCLOB" AS "c12", "t1"."COL_JSON" AS "c13", "t1"."COL_DATE" AS "c14", "t1"."COL_TIMESTAMP" AS "c15", "t1"."COL_TIMESTAMP_TZ" AS "c16", "t1"."COL_TIMESTAMP_LTZ" AS "c17", "t1"."COL_NULL" AS "c18" FROM "ANSILO_ADMIN"."T002__TEST_TAB" "t1""#
            )
        )]
    );
}

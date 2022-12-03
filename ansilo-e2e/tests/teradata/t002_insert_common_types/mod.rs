use std::{env, str::FromStr};

use ansilo_connectors_base::interface::{LoggedQuery, ResultSet};
use ansilo_core::{
    data::{chrono_tz::Tz, DataValue, DateTimeWithTZ},
    err::Result,
};
use ansilo_e2e::current_dir;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use pretty_assertions::assert_eq;
use rust_decimal::Decimal;

use ansilo_e2e::util::assert::assert_rows_equal;
use serial_test::serial;

#[test]
#[serial]
fn test_values() {
    ansilo_logging::init_for_tests();
    ansilo_e2e::teradata::start_teradata();
    let mut teradata =
        ansilo_e2e::teradata::init_teradata_sql(current_dir!().join("teradata-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .execute(
            r#"
            INSERT INTO "t002__test_tab" (
                col_char,
                col_varchar,
                col_clob,
                col_decimal,
                col_int8,
                col_int16,
                col_int32,
                col_int64,
                col_double,
                col_blob,
                col_json,
                col_jsonb,
                col_date,
                col_time,
                col_timestamp,
                col_timestamp_tz,
                col_null
            ) VALUES (
                'Q',
                'foobar',
                '🥑🚀',
                123.456,
                -123,
                5432,
                123456,
                -9876543210,
                33.44,
                'BLOB',
                '{"foo": "bar"}',
                '{"hello": "world"}',
                DATE '2020-12-23',
                TIME '01:02:03',
                TIMESTAMP '2018-02-01 01:02:03',
                TIMESTAMP WITH TIME ZONE '1999-01-15 11:00:00 +08:00',
                NULL
            )
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 1);

    // Check data received on teradata end
    let results = teradata
        .execute("SELECT * FROM t002__test_tab", vec![])
        .unwrap()
        .reader()
        .unwrap()
        .iter_rows()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_rows_equal(
        results,
        vec![vec![
            ("col_char".to_string(), DataValue::Utf8String("Q".into())),
            (
                "col_varchar".to_string(),
                DataValue::Utf8String("foobar".into()),
            ),
            ("col_clob".to_string(), DataValue::Utf8String("🥑🚀".into())),
            (
                "col_decimal".to_string(),
                DataValue::Decimal(Decimal::new(12345600, 5)),
            ),
            ("col_int8".to_string(), DataValue::Int8(-123)),
            ("col_int16".to_string(), DataValue::Int16(5432)),
            ("col_int32".to_string(), DataValue::Int32(123456)),
            ("col_int64".to_string(), DataValue::Int64(-9876543210i64)),
            ("col_double".to_string(), DataValue::Float64(33.44)),
            ("col_blob".to_string(), DataValue::Binary(b"BLOB".to_vec())),
            (
                "col_json".to_string(),
                DataValue::JSON("{\"foo\":\"bar\"}".into()),
            ),
            (
                "col_jsonb".to_string(),
                DataValue::JSON("{\"hello\":\"world\"}".into()),
            ),
            (
                "col_date".to_string(),
                DataValue::Date(NaiveDate::from_ymd_opt(2020, 12, 23).unwrap()),
            ),
            (
                "col_time".to_string(),
                DataValue::Time(NaiveTime::from_hms_opt(1, 2, 3).unwrap()),
            ),
            (
                "col_timestamp".to_string(),
                DataValue::DateTime(NaiveDateTime::from_str("2018-02-01T01:02:03").unwrap()),
            ),
            (
                "col_timestamp_tz".to_string(),
                DataValue::DateTimeWithTZ(DateTimeWithTZ::new(
                    NaiveDateTime::from_str("1999-01-15T03:00:00").unwrap(),
                    Tz::UTC,
                )),
            ),
            ("col_null".to_string(), DataValue::Null),
        ]
        .into_iter()
        .collect()],
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![
            ("teradata".to_string(), LoggedQuery::new_query("BEGIN")),
            (
                "teradata".to_string(),
                LoggedQuery::new(
                    [
                        r#"INSERT INTO "testdb"."t002__test_tab" "#,
                        r#"("col_char", "col_varchar", "col_clob", "col_decimal", "col_int8", "col_int16", "col_int32", "col_int64", "col_double", "col_blob", "col_json", "col_jsonb", "col_date", "col_time", "col_timestamp", "col_timestamp_tz", "col_null")"#,
                        r#" VALUES "#,
                        r#"(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#
                    ].join(""),
                    vec![
                        "LoggedParam [index=1, method=setString, value=Q]".into(),
                        "LoggedParam [index=2, method=setString, value=foobar]".into(),
                        "LoggedParam [index=3, method=setString, value=🥑🚀]".into(),
                        "LoggedParam [index=4, method=setBigDecimal, value=123.456]".into(),
                        "LoggedParam [index=5, method=setShort, value=-123]".into(),
                        "LoggedParam [index=6, method=setShort, value=5432]".into(),
                        "LoggedParam [index=7, method=setInt, value=123456]".into(),
                        "LoggedParam [index=8, method=setLong, value=-9876543210]".into(),
                        "LoggedParam [index=9, method=setDouble, value=33.44]".into(),
                        "LoggedParam [index=10, method=setBinaryStream, value=java.io.ByteArrayInputStream]".into(),
                        "LoggedParam [index=11, method=setString, value={\"foo\":\"bar\"}]".into(),
                        "LoggedParam [index=12, method=setString, value={\"hello\":\"world\"}]".into(),
                        "LoggedParam [index=13, method=setDate, value=2020-12-23]".into(),
                        "LoggedParam [index=14, method=setTime, value=01:02:03]".into(),
                        "LoggedParam [index=15, method=setTimestamp, value=2018-02-01 01:02:03.0]".into(),
                        "LoggedParam [index=16, method=setTimestamp, value=1999-01-15 03:00:00.0]".into(),
                        "LoggedParam [index=17, method=setNull, value=null]".into(),
                    ],
                    Some([("affected".into(), "Some(1)".into())]
                    .into_iter()
                    .collect())
                )
            ),
            ("teradata".to_string(), LoggedQuery::new_query("COMMIT")),
        ] 
    );
}

#[test]
#[serial]
fn test_insert_nulls() {
    ansilo_logging::init_for_tests();
    ansilo_e2e::teradata::start_teradata();
    let mut teradata =
        ansilo_e2e::teradata::init_teradata_sql(current_dir!().join("teradata-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .execute(
            r#"
            INSERT INTO "t002__test_tab" (
                col_char,
                col_varchar,
                col_clob,
                col_decimal,
                col_int8,
                col_int16,
                col_int32,
                col_int64,
                col_double,
                col_blob,
                col_json,
                col_jsonb,
                col_date,
                col_time,
                col_timestamp,
                col_timestamp_tz,
                col_null
            ) VALUES (
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL
            )
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 1);

    // Check data received on teradata end
    let results = teradata
        .execute("SELECT * FROM t002__test_tab", vec![])
        .unwrap()
        .reader()
        .unwrap()
        .iter_rows()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_rows_equal(
        results,
        vec![vec![
            ("col_char".to_string(), DataValue::Null),
            ("col_varchar".to_string(), DataValue::Null),
            ("col_clob".to_string(), DataValue::Null),
            ("col_decimal".to_string(), DataValue::Null),
            ("col_int8".to_string(), DataValue::Null),
            ("col_int16".to_string(), DataValue::Null),
            ("col_int32".to_string(), DataValue::Null),
            ("col_int64".to_string(), DataValue::Null),
            ("col_double".to_string(), DataValue::Null),
            ("col_blob".to_string(), DataValue::Null),
            ("col_json".to_string(), DataValue::Null),
            ("col_jsonb".to_string(), DataValue::Null),
            ("col_date".to_string(), DataValue::Null),
            ("col_time".to_string(), DataValue::Null),
            ("col_timestamp".to_string(), DataValue::Null),
            ("col_timestamp_tz".to_string(), DataValue::Null),
            ("col_null".to_string(), DataValue::Null),
        ]
        .into_iter()
        .collect()],
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![
            ("teradata".to_string(), LoggedQuery::new_query("BEGIN")),
            (
                "teradata".to_string(),
                LoggedQuery::new(
                    [
                        r#"INSERT INTO "testdb"."t002__test_tab" "#,
                        r#"("col_char", "col_varchar", "col_clob", "col_decimal", "col_int8", "col_int16", "col_int32", "col_int64", "col_double", "col_blob", "col_json", "col_jsonb", "col_date", "col_time", "col_timestamp", "col_timestamp_tz", "col_null")"#,
                        r#" VALUES "#,
                        r#"(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#
                    ].join(""),
                    vec![
                        "LoggedParam [index=1, method=setNull, value=null]".into(),
                        "LoggedParam [index=2, method=setNull, value=null]".into(),
                        "LoggedParam [index=3, method=setNull, value=null]".into(),
                        "LoggedParam [index=4, method=setNull, value=null]".into(),
                        "LoggedParam [index=5, method=setNull, value=null]".into(),
                        "LoggedParam [index=6, method=setNull, value=null]".into(),
                        "LoggedParam [index=7, method=setNull, value=null]".into(),
                        "LoggedParam [index=8, method=setNull, value=null]".into(),
                        "LoggedParam [index=9, method=setNull, value=null]".into(),
                        "LoggedParam [index=10, method=setNull, value=null]".into(),
                        "LoggedParam [index=11, method=setNull, value=null]".into(),
                        "LoggedParam [index=12, method=setNull, value=null]".into(),
                        "LoggedParam [index=13, method=setNull, value=null]".into(),
                        "LoggedParam [index=14, method=setNull, value=null]".into(),
                        "LoggedParam [index=15, method=setNull, value=null]".into(),
                        "LoggedParam [index=16, method=setNull, value=null]".into(),
                        "LoggedParam [index=17, method=setNull, value=null]".into(),
                    ],
                    Some([("affected".into(), "Some(1)".into())]
                    .into_iter()
                    .collect())
                )
            ),
            ("teradata".to_string(), LoggedQuery::new_query("COMMIT")),
        ]
    );
}

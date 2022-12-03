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
fn test() {
    ansilo_logging::init_for_tests();
    ansilo_e2e::teradata::start_teradata();
    let mut teradata =
        ansilo_e2e::teradata::init_teradata_sql(current_dir!().join("teradata-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .execute(
            r#"
            UPDATE "t003__test_tab"
            SET
                col_char = 'f',
                col_varchar = 'foobar',
                col_clob = '🥑🚀',
                col_decimal = 123.456,
                col_int8 = 123,
                col_int16 = 5432,
                col_int32 = 123456,
                col_int64 = -9876543210,
                col_double = 33.44,
                col_blob = 'BLOB',
                col_json = '{"foo": "bar"}',
                col_jsonb = '{"hello": "world"}',
                col_date = DATE '2020-12-23',
                col_time = TIME '01:02:03',
                col_timestamp = TIMESTAMP '2018-02-01 01:02:03',
                col_timestamp_tz = TIMESTAMP WITH TIME ZONE '1999-01-15 11:00:00 +08:00',
                col_null = NULL
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 1);

    // Check data received on teradata end
    let results = teradata
        .execute("SELECT * FROM t003__test_tab", vec![])
        .unwrap()
        .reader()
        .unwrap()
        .iter_rows()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_rows_equal(
        results,
        vec![vec![
            ("col_char".to_string(), DataValue::Utf8String("f".into())),
            (
                "col_varchar".to_string(),
                DataValue::Utf8String("foobar".into()),
            ),
            ("col_clob".to_string(), DataValue::Utf8String("🥑🚀".into())),
            (
                "col_decimal".to_string(),
                DataValue::Decimal(Decimal::new(12345600, 5)),
            ),
            ("col_int8".to_string(), DataValue::Int8(123)),
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
                        r#"UPDATE "testdb"."t003__test_tab" SET "#,
                        r#""col_char" = ?, "#,
                        r#""col_varchar" = ?, "#,
                        r#""col_clob" = ?, "#,
                        r#""col_decimal" = ?, "#,
                        r#""col_int8" = ?, "#,
                        r#""col_int16" = ?, "#,
                        r#""col_int32" = ?, "#,
                        r#""col_int64" = ?, "#,
                        r#""col_double" = ?, "#,
                        r#""col_blob" = ?, "#,
                        r#""col_json" = ?, "#,
                        r#""col_jsonb" = ?, "#,
                        r#""col_date" = ?, "#,
                        r#""col_time" = ?, "#,
                        r#""col_timestamp" = ?, "#,
                        r#""col_timestamp_tz" = ?, "#,
                        r#""col_null" = ?"#
                    ].join(""),
                    vec![
                        "LoggedParam [index=1, method=setString, value=f]".into(),
                        "LoggedParam [index=2, method=setString, value=foobar]".into(),
                        "LoggedParam [index=3, method=setString, value=🥑🚀]".into(),
                        "LoggedParam [index=4, method=setBigDecimal, value=123.456]".into(),
                        "LoggedParam [index=5, method=setShort, value=123]".into(),
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
            ("teradata".to_string(), LoggedQuery::new_query("COMMIT")),]
    );
}

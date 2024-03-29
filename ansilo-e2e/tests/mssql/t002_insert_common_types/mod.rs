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

#[test]
fn test() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mssql::start_mssql();
    let mut mssql =
        ansilo_e2e::mssql::init_mssql_sql(&containers, current_dir!().join("mssql-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .execute(
            r#"
            INSERT INTO "t002__test_tab" (
                col_char,
                col_nchar,
                col_varchar,
                col_nvarchar,
                col_decimal,
                col_uint8,
                col_int16,
                col_int32,
                col_int64,
                col_float,
                col_double,
                col_binary,
                col_date,
                col_time,
                col_datetime,
                col_datetimeoffset,
                col_uuid,
                col_text,
                col_null
            ) VALUES (
                'A',
                '🔥',
                'foobar',
                '🥑🚀',
                123.456,
                254,
                5432,
                123456,
                -9876543210,
                11.22,
                33.44,
                'BLOB',
                DATE '2020-12-23',
                TIME '01:02:03',
                TIMESTAMP '2018-02-01 01:02:03',
                TIMESTAMPTZ '1999-01-15 11:00:00+06:00',
                'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
                'text',
                NULL
            )
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 1);

    // Check data received on mssql end
    let results = mssql
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
            ("col_char".to_string(), DataValue::Utf8String("A".into())),
            ("col_nchar".to_string(), DataValue::Utf8String("🔥".into())),
            (
                "col_varchar".to_string(),
                DataValue::Utf8String("foobar".into()),
            ),
            (
                "col_nvarchar".to_string(),
                DataValue::Utf8String("🥑🚀".into()),
            ),
            (
                "col_decimal".to_string(),
                DataValue::Decimal(Decimal::new(12345600, 5)),
            ),
            ("col_uint8".to_string(), DataValue::UInt8(254)),
            ("col_int16".to_string(), DataValue::Int16(5432)),
            ("col_int32".to_string(), DataValue::Int32(123456)),
            ("col_int64".to_string(), DataValue::Int64(-9876543210i64)),
            ("col_float".to_string(), DataValue::Float32(11.22)),
            ("col_double".to_string(), DataValue::Float64(33.44)),
            (
                "col_binary".to_string(),
                DataValue::Binary(b"BLOB".to_vec()),
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
                "col_datetime".to_string(),
                DataValue::DateTime(NaiveDateTime::from_str("2018-02-01T01:02:03").unwrap()),
            ),
            (
                "col_datetimeoffset".to_string(),
                DataValue::DateTimeWithTZ(DateTimeWithTZ::new(
                    NaiveDateTime::from_str("1999-01-15T05:00:00").unwrap(),
                    Tz::UTC,
                )),
            ),
            (
                "col_uuid".to_string(),
                DataValue::Uuid("A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11".parse().unwrap()),
            ),
            ("col_text".to_string(), DataValue::Utf8String("text".into())),
            ("col_null".to_string(), DataValue::Null),
        ]
        .into_iter()
        .collect()],
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![
            ("mssql".to_string(), LoggedQuery::new_query("BEGIN")),
            (
                "mssql".to_string(),
                LoggedQuery::new(
                    [
                        r#"INSERT INTO [dbo].[t002__test_tab] "#,
                        r#"([col_char], [col_nchar], [col_varchar], [col_nvarchar], [col_decimal], [col_uint8], [col_int16], [col_int32], [col_int64], [col_float], [col_double], [col_binary], [col_date], [col_time], [col_datetime], [col_datetimeoffset], [col_uuid], [col_text], [col_null])"#,
                        r#" VALUES "#,
                        r#"(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#
                    ].join(""),
                    vec![
                        "LoggedParam [index=1, method=setNString, value=A]".into(),
                        "LoggedParam [index=2, method=setNString, value=🔥]".into(),
                        "LoggedParam [index=3, method=setNString, value=foobar]".into(),
                        "LoggedParam [index=4, method=setNString, value=🥑🚀]".into(),
                        "LoggedParam [index=5, method=setBigDecimal, value=123.456]".into(),
                        "LoggedParam [index=6, method=setShort, value=254]".into(),
                        "LoggedParam [index=7, method=setShort, value=5432]".into(),
                        "LoggedParam [index=8, method=setInt, value=123456]".into(),
                        "LoggedParam [index=9, method=setLong, value=-9876543210]".into(),
                        "LoggedParam [index=10, method=setFloat, value=11.22]".into(),
                        "LoggedParam [index=11, method=setDouble, value=33.44]".into(),
                        "LoggedParam [index=12, method=setBinaryStream, value=java.io.ByteArrayInputStream]".into(),
                        "LoggedParam [index=13, method=setDate, value=2020-12-23]".into(),
                        "LoggedParam [index=14, method=setTime, value=01:02:03]".into(),
                        "LoggedParam [index=15, method=setTimestamp, value=2018-02-01 01:02:03.0]".into(),
                        "LoggedParam [index=16, method=setTimestamp, value=1999-01-15 05:00:00.0]".into(),
                        "LoggedParam [index=17, method=setString, value=a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11]".into(),
                        "LoggedParam [index=18, method=setNString, value=text]".into(),
                        "LoggedParam [index=19, method=setNull, value=null]".into(),
                    ],
                    Some([("affected".into(), "Some(1)".into())]
                    .into_iter()
                    .collect())
                )
            ),
            ("mssql".to_string(), LoggedQuery::new_query("COMMIT")),
        ]
    );
}

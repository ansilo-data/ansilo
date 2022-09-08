use std::{env, str::FromStr};

use ansilo_connectors_base::interface::{LoggedQuery, ResultSet};
use ansilo_core::{
    data::{chrono_tz::Tz, uuid::Uuid, DataValue, DateTimeWithTZ},
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
    let containers = ansilo_e2e::postgres::start_postgres();
    let mut postgres = ansilo_e2e::postgres::init_postgres_sql(
        &containers,
        current_dir!().join("postgres-sql/*.sql"),
    );

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .execute(
            r#"
            UPDATE "t003__test_tab"
            SET
                col_char = '🔥',
                col_varchar = 'foobar',
                col_text = '🥑🚀',
                col_decimal = 123.456,
                col_bool = true,
                col_int16 = 5432,
                col_int32 = 123456,
                col_int64 = -9876543210,
                col_float = 11.22,
                col_double = 33.44,
                col_bytea = 'BLOB',
                col_json = '{"foo": "bar"}',
                col_jsonb = '{"hello": "world"}',
                col_date = DATE '2020-12-23',
                col_time = TIME '01:02:03',
                col_timestamp = TIMESTAMP '2018-02-01 01:02:03',
                col_timestamp_tz = TIMESTAMP WITH TIME ZONE '1999-01-15 11:00:00 +08:00',
                col_uuid = 'b4c52a77-44c5-4f5e-a1a3-95b6dac1b9d0',
                col_null = NULL
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 1);

    // Check data received on postgres end
    let results = postgres
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
            ("col_char".to_string(), DataValue::Utf8String("🔥".into())),
            (
                "col_varchar".to_string(),
                DataValue::Utf8String("foobar".into()),
            ),
            ("col_text".to_string(), DataValue::Utf8String("🥑🚀".into())),
            (
                "col_decimal".to_string(),
                DataValue::Decimal(Decimal::new(12345600, 5)),
            ),
            ("col_bool".to_string(), DataValue::Boolean(true)),
            ("col_int16".to_string(), DataValue::Int16(5432)),
            ("col_int32".to_string(), DataValue::Int32(123456)),
            ("col_int64".to_string(), DataValue::Int64(-9876543210i64)),
            ("col_float".to_string(), DataValue::Float32(11.22)),
            ("col_double".to_string(), DataValue::Float64(33.44)),
            ("col_bytea".to_string(), DataValue::Binary(b"BLOB".to_vec())),
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
                DataValue::Date(NaiveDate::from_ymd(2020, 12, 23)),
            ),
            (
                "col_time".to_string(),
                DataValue::Time(NaiveTime::from_hms(1, 2, 3)),
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
            (
                "col_uuid".to_string(),
                DataValue::Uuid(Uuid::from_str("b4c52a77-44c5-4f5e-a1a3-95b6dac1b9d0").unwrap()),
            ),
            ("col_null".to_string(), DataValue::Null),
        ]
        .into_iter()
        .collect()],
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "postgres".to_string(),
            LoggedQuery::new(
                [
                    r#"UPDATE "public"."t003__test_tab" SET "#,
                    r#""col_char" = $1, "#,
                    r#""col_varchar" = $2, "#,
                    r#""col_text" = $3, "#,
                    r#""col_decimal" = $4, "#,
                    r#""col_bool" = $5, "#,
                    r#""col_int16" = $6, "#,
                    r#""col_int32" = $7, "#,
                    r#""col_int64" = $8, "#,
                    r#""col_float" = $9, "#,
                    r#""col_double" = $10, "#,
                    r#""col_bytea" = $11, "#,
                    r#""col_json" = $12, "#,
                    r#""col_jsonb" = $13, "#,
                    r#""col_date" = $14, "#,
                    r#""col_time" = $15, "#,
                    r#""col_timestamp" = $16, "#,
                    r#""col_timestamp_tz" = $17, "#,
                    r#""col_uuid" = $18, "#,
                    r#""col_null" = $19"#
                ].join(""),
                vec![
                    "value=Utf8String(\"🔥\") type=bpchar".into(),
                    "value=Utf8String(\"foobar\") type=varchar".into(),
                    "value=Utf8String(\"🥑🚀\") type=text".into(),
                    "value=Decimal(123.456) type=numeric".into(),
                    "value=Boolean(true) type=bool".into(),
                    "value=Int16(5432) type=int2".into(),
                    "value=Int32(123456) type=int4".into(),
                    "value=Int64(-9876543210) type=int8".into(),
                    "value=Float32(11.22) type=float4".into(),
                    "value=Float64(33.44) type=float8".into(),
                    "value=Binary([66, 76, 79, 66]) type=bytea".into(),
                    "value=JSON(\"{\\\"foo\\\": \\\"bar\\\"}\") type=json".into(),
                    "value=JSON(\"{\\\"hello\\\": \\\"world\\\"}\") type=jsonb".into(),
                    "value=Date(2020-12-23) type=date".into(),
                    "value=Time(01:02:03) type=time".into(),
                    "value=DateTime(2018-02-01T01:02:03) type=timestamp".into(),
                    "value=DateTimeWithTZ(DateTimeWithTZ { dt: 1999-01-15T03:00:00, tz: UTC }) type=timestamptz".into(),
                    "value=Uuid(b4c52a77-44c5-4f5e-a1a3-95b6dac1b9d0) type=uuid".into(),
                    "value=Null type=bpchar".into(),
                ],
                Some([("affected".into(), "Some(1)".into())]
                .into_iter()
                .collect())
            )
        )]
    );
}

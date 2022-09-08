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
            INSERT INTO "t002__test_tab" (
                col_char,
                col_varchar,
                col_text,
                col_decimal,
                col_bool,
                col_int16,
                col_int32,
                col_int64,
                col_float,
                col_double,
                col_bytea,
                col_json,
                col_jsonb,
                col_date,
                col_time,
                col_timestamp,
                col_timestamp_tz,
                col_uuid,
                col_null
            ) VALUES (
                '🔥',
                'foobar',
                '🥑🚀',
                123.456,
                true,
                5432,
                123456,
                -9876543210,
                11.22,
                33.44,
                'BLOB',
                '{"foo": "bar"}',
                '{"hello": "world"}',
                DATE '2020-12-23',
                TIME '01:02:03',
                TIMESTAMP '2018-02-01 01:02:03',
                TIMESTAMP WITH TIME ZONE '1999-01-15 11:00:00 +08:00',
                'b4c52a77-44c5-4f5e-a1a3-95b6dac1b9d0',
                NULL
            )
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 1);

    // Check data received on postgres end
    let results = postgres
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
                    r#"INSERT INTO "public"."t002__test_tab" "#,
                    r#"("col_char", "col_varchar", "col_text", "col_decimal", "col_bool", "col_int16", "col_int32", "col_int64", "col_float", "col_double", "col_bytea", "col_json", "col_jsonb", "col_date", "col_time", "col_timestamp", "col_timestamp_tz", "col_uuid", "col_null")"#,
                    r#" VALUES "#,
                    r#"($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)"#
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

#[test]
#[serial]
fn test_insert_nulls() {
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
            INSERT INTO "t002__test_tab" (
                col_char,
                col_varchar,
                col_text,
                col_decimal,
                col_bool,
                col_int16,
                col_int32,
                col_int64,
                col_float,
                col_double,
                col_bytea,
                col_json,
                col_jsonb,
                col_date,
                col_time,
                col_timestamp,
                col_timestamp_tz,
                col_uuid,
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
                NULL,
                NULL,
                NULL
            )
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 1);

    // Check data received on postgres end
    let results = postgres
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
            ("col_text".to_string(), DataValue::Null),
            ("col_decimal".to_string(), DataValue::Null),
            ("col_bool".to_string(), DataValue::Null),
            ("col_int16".to_string(), DataValue::Null),
            ("col_int32".to_string(), DataValue::Null),
            ("col_int64".to_string(), DataValue::Null),
            ("col_float".to_string(), DataValue::Null),
            ("col_double".to_string(), DataValue::Null),
            ("col_bytea".to_string(), DataValue::Null),
            ("col_json".to_string(), DataValue::Null),
            ("col_jsonb".to_string(), DataValue::Null),
            ("col_date".to_string(), DataValue::Null),
            ("col_time".to_string(), DataValue::Null),
            ("col_timestamp".to_string(), DataValue::Null),
            ("col_timestamp_tz".to_string(), DataValue::Null),
            ("col_uuid".to_string(), DataValue::Null),
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
                    r#"INSERT INTO "public"."t002__test_tab" "#,
                    r#"("col_char", "col_varchar", "col_text", "col_decimal", "col_bool", "col_int16", "col_int32", "col_int64", "col_float", "col_double", "col_bytea", "col_json", "col_jsonb", "col_date", "col_time", "col_timestamp", "col_timestamp_tz", "col_uuid", "col_null")"#,
                    r#" VALUES "#,
                    r#"($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)"#
                ].join(""),
                vec![
                    "value=Null type=bpchar".into(),
                    "value=Null type=varchar".into(),
                    "value=Null type=text".into(),
                    "value=Null type=numeric".into(),
                    "value=Null type=bool".into(),
                    "value=Null type=int2".into(),
                    "value=Null type=int4".into(),
                    "value=Null type=int8".into(),
                    "value=Null type=float4".into(),
                    "value=Null type=float8".into(),
                    "value=Null type=bytea".into(),
                    "value=Null type=json".into(),
                    "value=Null type=jsonb".into(),
                    "value=Null type=date".into(),
                    "value=Null type=time".into(),
                    "value=Null type=timestamp".into(),
                    "value=Null type=timestamptz".into(),
                    "value=Null type=uuid".into(),
                    "value=Null type=bpchar".into(),
                ],
                Some([("affected".into(), "Some(1)".into())]
                .into_iter()
                .collect())
            )
        )]
    );
}

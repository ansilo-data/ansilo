use std::env;

use ansilo_connectors_base::interface::{LoggedQuery, ResultSet};
use ansilo_core::{data::DataValue, err::Result};
use ansilo_e2e::current_dir;
use pretty_assertions::assert_eq;

use ansilo_e2e::util::assert::assert_rows_equal;
use serial_test::serial;

#[test]
#[serial]
fn test() {
    ansilo_logging::init_for_tests();
    let (mut sqlite, sqlite_path) =
        ansilo_e2e::sqlite::init_sqlite_sql(current_dir!().join("sqlite-sql/*.sql"));

    let (instance, mut client) = ansilo_e2e::util::main::run_instance_args(
        current_dir!().join("config.yml"),
        &[("SQLITE_PATH", sqlite_path.to_string_lossy())],
    );

    let rows = client
        .execute(
            r#"
            INSERT INTO "t002__test_tab" (
                col_char,
                col_varchar,
                col_decimal,
                col_int8,
                col_int16,
                col_int32,
                col_int64,
                col_float,
                col_double,
                col_blob,
                col_date,
                col_time,
                col_timestamp,
                col_timestamp_tz,
                col_null
            ) VALUES (
                '🔥',
                'foobar',
                123.456,
                123,
                5432,
                123456,
                -9876543210,
                11.22,
                33.44,
                'BLOB'::bytea,
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

    // Check data received on sqlite end
    let results = sqlite
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
            (
                "col_decimal".to_string(),
                DataValue::Utf8String("123.456".into()),
            ),
            ("col_int8".to_string(), DataValue::Int64(123)),
            ("col_int16".to_string(), DataValue::Int64(5432)),
            ("col_int32".to_string(), DataValue::Int64(123456)),
            ("col_int64".to_string(), DataValue::Int64(-9876543210i64)),
            ("col_float".to_string(), DataValue::Float64(11.22)),
            ("col_double".to_string(), DataValue::Float64(33.44)),
            ("col_blob".to_string(), DataValue::Binary(b"BLOB".to_vec())),
            (
                "col_date".to_string(),
                DataValue::Utf8String("2020-12-23".into()),
            ),
            (
                "col_time".to_string(),
                DataValue::Utf8String("01:02:03".into()),
            ),
            (
                "col_timestamp".to_string(),
                DataValue::Utf8String("2018-02-01 01:02:03".into()),
            ),
            (
                "col_timestamp_tz".to_string(),
                DataValue::Utf8String("1999-01-15 03:00:00+00".into()),
            ),
            ("col_null".to_string(), DataValue::Null),
        ]
        .into_iter()
        .collect()],
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "sqlite".to_string(),
            LoggedQuery::new(
                [
                    r#"INSERT INTO "t002__test_tab" "#,
                    r#"("col_char", "col_varchar", "col_decimal", "col_int8", "col_int16", "col_int32", "col_int64", "col_float", "col_double", "col_blob", "col_date", "col_time", "col_timestamp", "col_timestamp_tz", "col_null")"#,
                    r#" VALUES "#,
                    r#"(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)"#
                ].join(""),
                vec![
                    "value=Utf8String(\"🔥\")".into(),
                    "value=Utf8String(\"foobar\")".into(),
                    "value=Utf8String(\"123.456\")".into(),
                    "value=Int64(123)".into(),
                    "value=Int64(5432)".into(),
                    "value=Int64(123456)".into(),
                    "value=Int64(-9876543210)".into(),
                    "value=Float64(11.22)".into(),
                    "value=Float64(33.44)".into(),
                    "value=Binary([66, 76, 79, 66])".into(),
                    "value=Utf8String(\"2020-12-23\")".into(),
                    "value=Utf8String(\"01:02:03\")".into(),
                    "value=Utf8String(\"2018-02-01 01:02:03\")".into(),
                    "value=Utf8String(\"1999-01-15 03:00:00+00\")".into(),
                    "value=Null".into(),
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
    let (mut sqlite, sqlite_path) =
        ansilo_e2e::sqlite::init_sqlite_sql(current_dir!().join("sqlite-sql/*.sql"));

    let (instance, mut client) = ansilo_e2e::util::main::run_instance_args(
        current_dir!().join("config.yml"),
        &[("SQLITE_PATH", sqlite_path.to_string_lossy())],
    );

    let rows = client
        .execute(
            r#"
            INSERT INTO "t002__test_tab" (
                col_char,
                col_varchar,
                col_decimal,
                col_int8,
                col_int16,
                col_int32,
                col_int64,
                col_float,
                col_double,
                col_blob,
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
                NULL
            )
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 1);

    // Check data received on sqlite end
    let results = sqlite
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
            ("col_decimal".to_string(), DataValue::Null),
            ("col_int8".to_string(), DataValue::Null),
            ("col_int16".to_string(), DataValue::Null),
            ("col_int32".to_string(), DataValue::Null),
            ("col_int64".to_string(), DataValue::Null),
            ("col_float".to_string(), DataValue::Null),
            ("col_double".to_string(), DataValue::Null),
            ("col_blob".to_string(), DataValue::Null),
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
        vec![(
            "sqlite".to_string(),
            LoggedQuery::new(
                [
                    r#"INSERT INTO "t002__test_tab" "#,
                    r#"("col_char", "col_varchar", "col_decimal", "col_int8", "col_int16", "col_int32", "col_int64", "col_float", "col_double", "col_blob", "col_date", "col_time", "col_timestamp", "col_timestamp_tz", "col_null")"#,
                    r#" VALUES "#,
                    r#"(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)"#
                ].join(""),
                vec![
                    "value=Null".into(),
                    "value=Null".into(),
                    "value=Null".into(),
                    "value=Null".into(),
                    "value=Null".into(),
                    "value=Null".into(),
                    "value=Null".into(),
                    "value=Null".into(),
                    "value=Null".into(),
                    "value=Null".into(),
                    "value=Null".into(),
                    "value=Null".into(),
                    "value=Null".into(),
                    "value=Null".into(),
                    "value=Null".into(),
                ],
                Some([("affected".into(), "Some(1)".into())]
                .into_iter()
                .collect())
            )
        )]
    );
}

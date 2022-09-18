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

    // ansilo_e2e::util::debug::debug(&instance);
    let rows = client
        .execute(
            r#"
            UPDATE "t003__test_tab"
            SET
                col_char = '🔥',
                col_varchar = 'foobar',
                col_decimal = 123.456,
                col_int8 = 88,
                col_int16 = 5432,
                col_int32 = 123456,
                col_int64 = -9876543210,
                col_float = 11.22,
                col_double = 33.44,
                col_blob = 'BLOB',
                col_date = DATE '2020-12-23',
                col_time = TIME '01:02:03',
                col_timestamp = TIMESTAMP '2018-02-01 01:02:03',
                col_timestamp_tz = TIMESTAMP WITH TIME ZONE '1999-01-15 03:00:00 +08:00',
                col_null = NULL
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 1);

    // Check data received on sqlite end
    let results = sqlite
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
            (
                "col_decimal".to_string(),
                DataValue::Utf8String("123.456".into()),
            ),
            ("col_int8".to_string(), DataValue::Int64(88)),
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
                DataValue::Utf8String("2018-02-01T01:02:03".into()),
            ),
            (
                "col_timestamp_tz".to_string(),
                DataValue::Utf8String("1999-01-14T19:00:00+00:00".into()),
            ),
            ("col_null".to_string(), DataValue::Null),
        ]
        .into_iter()
        .collect()],
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![
            ("sqlite".to_string(), LoggedQuery::new_query("BEGIN")),
            (
                "sqlite".to_string(),
                LoggedQuery::new(
                    [
                        r#"UPDATE "t003__test_tab" SET "#,
                        r#""col_char" = ?1, "#,
                        r#""col_varchar" = ?2, "#,
                        r#""col_decimal" = ?3, "#,
                        r#""col_int8" = ?4, "#,
                        r#""col_int16" = ?5, "#,
                        r#""col_int32" = ?6, "#,
                        r#""col_int64" = ?7, "#,
                        r#""col_float" = ?8, "#,
                        r#""col_double" = ?9, "#,
                        r#""col_blob" = ?10, "#,
                        r#""col_date" = ?11, "#,
                        r#""col_time" = ?12, "#,
                        r#""col_timestamp" = ?13, "#,
                        r#""col_timestamp_tz" = ?14, "#,
                        r#""col_null" = ?15"#
                    ]
                    .join(""),
                    vec![
                        "value=Utf8String(\"🔥\")".into(),
                        "value=Utf8String(\"foobar\")".into(),
                        "value=Utf8String(\"123.456\")".into(),
                        "value=Int64(88)".into(),
                        "value=Int64(5432)".into(),
                        "value=Int64(123456)".into(),
                        "value=Int64(-9876543210)".into(),
                        "value=Float64(11.22)".into(),
                        "value=Float64(33.44)".into(),
                        "value=Binary([66, 76, 79, 66])".into(),
                        "value=Utf8String(\"2020-12-23\")".into(),
                        "value=Utf8String(\"01:02:03\")".into(),
                        "value=Utf8String(\"2018-02-01T01:02:03\")".into(),
                        "value=Utf8String(\"1999-01-14T19:00:00+00:00\")".into(),
                        "value=Null".into(),
                    ],
                    Some(
                        [("affected".into(), "Some(1)".into())]
                            .into_iter()
                            .collect()
                    )
                )
            ),
            ("sqlite".to_string(), LoggedQuery::new_query("COMMIT")),
        ]
    );
}

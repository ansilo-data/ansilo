use std::{env, str::FromStr};

use ansilo_connectors_base::interface::{LoggedQuery, ResultSet};
use ansilo_core::{
    data::{chrono_tz::Tz, DataValue, DateTimeWithTZ},
    err::Result,
};
use chrono::NaiveDateTime;
use itertools::Itertools;
use pretty_assertions::assert_eq;
use rust_decimal::Decimal;

#[test]
fn test() {
    ansilo_logging::init_for_tests();
    let containers = super::common::start_oracle();
    let mut oracle =
        super::common::init_oracle_sql(&containers, crate::current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) =
        crate::main::run_instance(crate::current_dir!().join("config.yml"));

    let _rows = client
        .execute(
            r#"
            UPDATE "ANSILO_ADMIN.T004__TEST_TAB"
            SET
                "COL_CHAR" = 'A',
                "COL_NCHAR" = '🔥',
                "COL_VARCHAR2" = 'foobar',
                "COL_NVARCHAR2" = '🚀',
                "COL_NUMBER" = 123.456,
                "COL_FLOAT" = 567.89,
                "COL_BINARY_FLOAT" = 11.22,
                "COL_BINARY_DOUBLE" = 33.44,
                "COL_RAW" = 'RAW'::bytea,
                "COL_LONG_RAW" = 'LONG RAW'::bytea,
                "COL_BLOB" = 'BLOB'::bytea,
                "COL_CLOB" = 'CLOB',
                "COL_NCLOB" = '🥑NCLOB',
                "COL_JSON" = '{"foo": "bar"}',
                "COL_DATE" = DATE '2020-12-23',
                "COL_TIMESTAMP" = TIMESTAMP '2018-02-01 01:02:03',
                "COL_TIMESTAMP_TZ" = TIMESTAMP WITH TIME ZONE '1999-01-15 11:00:00 -5:00',
                "COL_TIMESTAMP_LTZ" = TIMESTAMP WITH TIME ZONE '1997-01-31 09:26:56.888 +02:00',
                "COL_NULL" = NULL
        "#,
            &[],
        )
        .unwrap();

    // TODO: implement row count reporting for update / delete
    // assert_eq!(rows, 1);

    // Check data received on oracle end
    let results = oracle
        .execute("SELECT * FROM T004__TEST_TAB", vec![])
        .unwrap()
        .reader()
        .unwrap()
        .iter_rows()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_eq!(
        results[0]
            .clone()
            .into_iter()
            .sorted_by_key(|i| i.0.clone())
            .collect_vec(),
        vec![
            ("COL_CHAR".to_string(), DataValue::Utf8String("A".into())),
            ("COL_NCHAR".to_string(), DataValue::Utf8String("🔥".into())),
            (
                "COL_VARCHAR2".to_string(),
                DataValue::Utf8String("foobar".into())
            ),
            (
                "COL_NVARCHAR2".to_string(),
                DataValue::Utf8String("🚀".into())
            ),
            (
                "COL_NCLOB".to_string(),
                DataValue::Utf8String("🥑NCLOB".into())
            ),
            ("COL_CLOB".to_string(), DataValue::Utf8String("CLOB".into())),
            (
                "COL_BLOB".to_string(),
                DataValue::Binary([66, 76, 79, 66].to_vec())
            ),
            (
                "COL_RAW".to_string(),
                DataValue::Binary([82, 65, 87].to_vec())
            ),
            (
                "COL_LONG_RAW".to_string(),
                DataValue::Binary([76, 79, 78, 71, 32, 82, 65, 87].to_vec())
            ),
            (
                "COL_NUMBER".to_string(),
                DataValue::Decimal(Decimal::new(123456, 3))
            ),
            (
                "COL_FLOAT".to_string(),
                DataValue::Decimal(Decimal::new(56789, 2))
            ),
            ("COL_BINARY_FLOAT".to_string(), DataValue::Float32(11.22)),
            ("COL_BINARY_DOUBLE".to_string(), DataValue::Float64(33.44)),
            (
                "COL_DATE".to_string(),
                DataValue::DateTime(NaiveDateTime::from_str("2020-12-23T00:00:00").unwrap())
            ),
            (
                "COL_TIMESTAMP_TZ".to_string(),
                DataValue::DateTimeWithTZ(DateTimeWithTZ::new(
                    NaiveDateTime::from_str("1999-01-15T16:00:00").unwrap(),
                    Tz::UTC
                ))
            ),
            (
                "COL_TIMESTAMP".to_string(),
                DataValue::DateTime(NaiveDateTime::from_str("2018-02-01T01:02:03").unwrap())
            ),
            (
                "COL_TIMESTAMP_LTZ".to_string(),
                DataValue::DateTimeWithTZ(DateTimeWithTZ::new(
                    NaiveDateTime::from_str("1997-01-31T07:26:56.888").unwrap(),
                    Tz::UTC
                ))
            ),
            (
                "COL_JSON".to_string(),
                DataValue::JSON("{\"foo\":\"bar\"}".into())
            ),
            ("COL_NULL".to_string(), DataValue::Null),
        ]
        .into_iter()
        .sorted_by_key(|i| i.0.clone())
        .collect_vec()
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "oracle".to_string(),
            LoggedQuery::new(
                [
                    r#"UPDATE "ANSILO_ADMIN"."T004__TEST_TAB" SET "#,
                    r#""COL_CHAR" = ?, "#,
                    r#""COL_NCHAR" = ?, "#,
                    r#""COL_VARCHAR2" = ?, "#,
                    r#""COL_NVARCHAR2" = ?, "#,
                    r#""COL_NUMBER" = ?, "#,
                    r#""COL_FLOAT" = ?, "#,
                    r#""COL_BINARY_FLOAT" = ?, "#,
                    r#""COL_BINARY_DOUBLE" = ?, "#,
                    r#""COL_RAW" = ?, "#,
                    r#""COL_LONG_RAW" = ?, "#,
                    r#""COL_BLOB" = ?, "#,
                    r#""COL_CLOB" = ?, "#,
                    r#""COL_NCLOB" = ?, "#,
                    r#""COL_JSON" = ?, "#,
                    r#""COL_DATE" = ?, "#,
                    r#""COL_TIMESTAMP" = ?, "#,
                    r#""COL_TIMESTAMP_TZ" = ?, "#,
                    r#""COL_TIMESTAMP_LTZ" = ?, "#,
                    r#""COL_NULL" = ?"#
                ].join(""),
                vec![
                    "LoggedParam [index=1, method=setNString, value=A]".into(),
                    "LoggedParam [index=2, method=setNString, value=🔥]".into(),
                    "LoggedParam [index=3, method=setNString, value=foobar]".into(),
                    "LoggedParam [index=4, method=setNString, value=🚀]".into(),
                    "LoggedParam [index=5, method=setBigDecimal, value=123.456]".into(),
                    "LoggedParam [index=6, method=setBigDecimal, value=567.89]".into(),
                    "LoggedParam [index=7, method=setFloat, value=11.22]".into(),
                    "LoggedParam [index=8, method=setDouble, value=33.44]".into(),
                    "LoggedParam [index=9, method=setBinaryStream, value=java.io.ByteArrayInputStream]".into(),
                    "LoggedParam [index=10, method=setBinaryStream, value=java.io.ByteArrayInputStream]".into(),
                    "LoggedParam [index=11, method=setBinaryStream, value=java.io.ByteArrayInputStream]".into(),
                    "LoggedParam [index=12, method=setNString, value=CLOB]".into(),
                    "LoggedParam [index=13, method=setNString, value=🥑NCLOB]".into(),
                    "LoggedParam [index=14, method=setNString, value={\"foo\": \"bar\"}]".into(),
                    "LoggedParam [index=15, method=setDate, value=2020-12-23]".into(),
                    "LoggedParam [index=16, method=setTimestamp, value=2018-02-01 01:02:03.0]".into(),
                    "LoggedParam [index=17, method=setTimestamp, value=1999-01-15 16:00:00.0]".into(),
                    "LoggedParam [index=18, method=setTimestamp, value=1997-01-31 07:26:56.888]".into(),
                    "LoggedParam [index=19, method=setNull, value=null]".into(),
                ],
                None
            )
        )]
    );
}

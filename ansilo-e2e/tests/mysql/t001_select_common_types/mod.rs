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
    let containers = ansilo_e2e::mysql::start_mysql();
    ansilo_e2e::mysql::init_mysql_sql(&containers, current_dir!().join("mysql-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query("SELECT * FROM \"t001__test_tab\"", &[])
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
            "col_char",
            "col_nchar",
            "col_varchar",
            "col_nvarchar",
            "col_decimal",
            "col_int8",
            "col_int16",
            "col_int32",
            "col_int64",
            "col_uint8",
            "col_uint16",
            "col_uint32",
            "col_uint64",
            "col_float",
            "col_double",
            "col_blob",
            "col_json",
            "col_date",
            "col_time",
            "col_datetime",
            "col_timestamp",
            "col_null"
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
    assert_eq!(rows[0].get::<_, i16>(5), 88);
    assert_eq!(rows[0].get::<_, i16>(6), 5432);
    assert_eq!(rows[0].get::<_, i32>(7), 123456);
    assert_eq!(rows[0].get::<_, i64>(8), -9876543210);
    assert_eq!(rows[0].get::<_, i16>(9), 188);
    assert_eq!(rows[0].get::<_, i32>(10), 55432);
    assert_eq!(rows[0].get::<_, i64>(11), 1123456);
    assert_eq!(rows[0].get::<_, Decimal>(12), Decimal::new(19876543210, 0));
    assert_eq!(rows[0].get::<_, f32>(13), 11.22_f32);
    assert_eq!(rows[0].get::<_, f64>(14), 33.44_f64);
    assert_eq!(rows[0].get::<_, Vec<u8>>(15), b"BLOB".to_vec());
    assert_eq!(
        rows[0].get::<_, serde_json::Value>(16),
        json!({"foo": "bar"})
    );
    assert_eq!(
        rows[0].get::<_, NaiveDate>(17),
        NaiveDate::from_ymd(2020, 12, 23)
    );
    assert_eq!(
        rows[0].get::<_, NaiveTime>(18),
        NaiveTime::from_hms(1, 2, 3)
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
        DateTime::<FixedOffset>::parse_from_rfc3339("1999-01-15T11:00:00+00:00").unwrap()
    );
    assert_eq!(rows[0].get::<_, Option<String>>(21), None);

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "mysql".to_string(),
            LoggedQuery::new_query(
                [
                    r#"SELECT `t1`.`col_char` AS `c0`, "#,
                    r#"`t1`.`col_nchar` AS `c1`, "#,
                    r#"`t1`.`col_varchar` AS `c2`, "#,
                    r#"`t1`.`col_nvarchar` AS `c3`, "#,
                    r#"`t1`.`col_decimal` AS `c4`, "#,
                    r#"`t1`.`col_int8` AS `c5`, "#,
                    r#"`t1`.`col_int16` AS `c6`, "#,
                    r#"`t1`.`col_int32` AS `c7`, "#,
                    r#"`t1`.`col_int64` AS `c8`, "#,
                    r#"`t1`.`col_uint8` AS `c9`, "#,
                    r#"`t1`.`col_uint16` AS `c10`, "#,
                    r#"`t1`.`col_uint32` AS `c11`, "#,
                    r#"`t1`.`col_uint64` AS `c12`, "#,
                    r#"`t1`.`col_float` AS `c13`, "#,
                    r#"`t1`.`col_double` AS `c14`, "#,
                    r#"`t1`.`col_blob` AS `c15`, "#,
                    r#"`t1`.`col_json` AS `c16`, "#,
                    r#"`t1`.`col_date` AS `c17`, "#,
                    r#"`t1`.`col_time` AS `c18`, "#,
                    r#"`t1`.`col_datetime` AS `c19`, "#,
                    r#"`t1`.`col_timestamp` AS `c20`, "#,
                    r#"`t1`.`col_null` AS `c21` "#,
                    r#"FROM `db`.`t001__test_tab` AS `t1`"#,
                ]
                .join("")
            )
        )]
    );
}

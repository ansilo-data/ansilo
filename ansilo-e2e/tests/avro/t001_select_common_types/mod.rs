use std::{env, str::FromStr};

use ansilo_core::data::chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use ansilo_core::data::uuid::Uuid;
use ansilo_e2e::current_dir;
use itertools::Itertools;
use pretty_assertions::assert_eq;

#[test]
fn test() {
    ansilo_logging::init_for_tests();
    let (_arvo, avro_path) =
        ansilo_e2e::avro::init_avro_files(current_dir!().join("avro-init/*.json"));

    let (_instance, mut client) = ansilo_e2e::util::main::run_instance_args(
        current_dir!().join("config.yml"),
        &[("AVRO_PATH", avro_path.to_string_lossy())],
    );

    let rows = client
        .query(r#"SELECT * FROM "test_data.avro""#, &[])
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
            "int",
            "long",
            "string",
            "bool",
            "float",
            "double",
            "bytes",
            "uuid",
            "date",
            "time_micros",
            "timestamp_micros",
            "null"
        ]
        .into_iter()
        .sorted()
        .collect_vec()
    );
    assert_eq!(rows[0].get::<_, i32>(0), 123);
    assert_eq!(rows[0].get::<_, i64>(1), 123456);
    assert_eq!(rows[0].get::<_, String>(2), "hello");
    assert_eq!(rows[0].get::<_, bool>(3), true);
    assert_eq!(rows[0].get::<_, f32>(4), 11.22_f32);
    assert_eq!(rows[0].get::<_, f64>(5), 33.44_f64);
    assert_eq!(rows[0].get::<_, Vec<u8>>(6), b"BLOB".to_vec());
    assert_eq!(
        rows[0].get::<_, Uuid>(7),
        Uuid::from_str("8fba61eb-5c96-4e66-9aa5-0e32d094e3e7").unwrap()
    );
    assert_eq!(
        rows[0].get::<_, NaiveDate>(8),
        NaiveDate::from_ymd_opt(1973, 5, 19).unwrap()
    );
    assert_eq!(
        rows[0].get::<_, NaiveTime>(9),
        NaiveTime::from_hms_micro_opt(16, 40, 0, 1).unwrap()
    );
    assert_eq!(
        rows[0].get::<_, NaiveDateTime>(10),
        NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2022, 9, 30).unwrap(),
            NaiveTime::from_hms_micro_opt(4, 3, 50, 1).unwrap()
        )
    );
}

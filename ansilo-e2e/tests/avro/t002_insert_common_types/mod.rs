use std::{env, fs::OpenOptions};

use ansilo_connectors_file_avro::apache_avro::{self, types::Value as AvroValue};
use ansilo_e2e::current_dir;
use pretty_assertions::assert_eq;

use serial_test::serial;

#[test]
#[serial]
fn test() {
    ansilo_logging::init_for_tests();
    let (_avro, avro_path) =
        ansilo_e2e::avro::init_avro_files(current_dir!().join("avro-init/*.json"));

    let (_instance, mut client) = ansilo_e2e::util::main::run_instance_args(
        current_dir!().join("config.yml"),
        &[("AVRO_PATH", avro_path.to_string_lossy())],
    );

    let rows = client
        .execute(
            r#"
            INSERT INTO "test_data.avro" (
                int,
                long,
                string,
                bool,
                float,
                double,
                bytes,
                uuid,
                date,
                time_micros,
                timestamp_micros,
                "null"
            ) VALUES (
                123,
                -9876543210,
                'hello',
                TRUE,
                11.22,
                33.44,
                'BLOB'::bytea,
                '8fba61eb-5c96-4e66-9aa5-0e32d094e3e7',
                DATE '2020-12-23',
                TIME '01:02:03',
                TIMESTAMP '2018-02-01 01:02:03',
                NULL
            )
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 1);

    // Check data received on avro end
    let mut results = apache_avro::Reader::new(
        OpenOptions::new()
            .read(true)
            .open(avro_path.join("test_data.avro"))
            .unwrap(),
    )
    .unwrap();

    assert_eq!(
        results.next().unwrap().unwrap(),
        AvroValue::Record(vec![
            ("int".into(), AvroValue::Int(123)),
            ("long".into(), AvroValue::Long(-9876543210)),
            ("string".into(), AvroValue::String("hello".into())),
            ("bool".into(), AvroValue::Boolean(true)),
            ("float".into(), AvroValue::Float(11.22)),
            ("double".into(), AvroValue::Double(33.44)),
            ("bytes".into(), AvroValue::Bytes(b"BLOB".to_vec())),
            (
                "uuid".into(),
                AvroValue::Uuid("8fba61eb-5c96-4e66-9aa5-0e32d094e3e7".parse().unwrap())
            ),
            ("date".into(), AvroValue::Date(18619)),
            ("time_micros".into(), AvroValue::TimeMicros(3723000000)),
            (
                "timestamp_micros".into(),
                AvroValue::TimestampMicros(1517446923000000)
            ),
            ("null".into(), AvroValue::Null),
        ])
    );
}

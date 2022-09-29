use std::{env, fs::OpenOptions};

use ansilo_e2e::current_dir;
use pretty_assertions::assert_eq;

#[test]
fn test() {
    ansilo_logging::init_for_tests();
    let (_avro, avro_path) =
        ansilo_e2e::avro::init_avro_files(current_dir!().join("avro-init/*.json"));

    let (_instance, mut client) = ansilo_e2e::util::main::run_instance_args(
        current_dir!().join("config.yml"),
        &[("AVRO_PATH", avro_path.to_string_lossy())],
    );

    client
        .execute(r#"DELETE FROM "test_data.avro""#, &[])
        .unwrap();

    // Check file truncated
    let file = OpenOptions::new()
        .read(true)
        .open(avro_path.join("test_data.avro"))
        .unwrap();

    let meta = file.metadata().unwrap();
    assert_eq!(meta.len(), 0);
}

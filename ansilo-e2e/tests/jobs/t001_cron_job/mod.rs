use std::{thread, time::Duration};

use ansilo_e2e::current_dir;
use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
fn test() {
    ansilo_logging::init_for_tests();
    let (_instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    thread::sleep(Duration::from_secs(3));

    let row = client.query_one(r#"SELECT * FROM jobs"#, &[]).unwrap();

    let runs: i32 = row.get("runs");
    let user: String = row.get("usr");

    assert_eq!(user, "ansiloadmin");
    dbg!(runs);
    assert!(runs >= 1);
}

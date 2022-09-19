use std::thread;

use ansilo_e2e::current_dir;
use postgres::NoTls;
use serial_test::serial;

#[test]
#[serial]
fn test() {
    ansilo_logging::init_for_tests();
    let (_instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let cancel_token = client.cancel_token();
    let query_thread = thread::spawn(move || {
        let err = client.batch_execute("SELECT pg_sleep(30)").unwrap_err();

        dbg!(err.to_string());
        assert!(err
            .to_string()
            .contains("canceling statement due to user request"))
    });

    cancel_token.cancel_query(NoTls).unwrap();
    query_thread.join().unwrap();
}

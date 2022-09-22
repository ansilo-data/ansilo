use ansilo_e2e::current_dir;
use serial_test::serial;

#[test]
#[serial]
fn test() {
    ansilo_logging::init_for_tests();
    let (_instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let should_be_denied = [
        "SELECT remote_query('src', 'query')",
        "SELECT remote_query('src', 'query', 'param')",
        "SELECT remote_execute('src', 'query')",
        "SELECT remote_execute('src', 'query', 'param')",
    ];

    for query in should_be_denied {
        let res = client.batch_execute(query).unwrap_err();

        dbg!(query);
        dbg!(res.to_string());
        assert!(res.to_string().contains("permission denied"));
    }
}

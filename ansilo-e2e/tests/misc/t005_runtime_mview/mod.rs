use ansilo_e2e::current_dir;
use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
fn test() {
    ansilo_logging::init_for_tests();
    let (_instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let res = client.query_one("SELECT * FROM people_mat", &[]).unwrap();

    assert_eq!(res.get::<_, String>("name"), "Mary");
}

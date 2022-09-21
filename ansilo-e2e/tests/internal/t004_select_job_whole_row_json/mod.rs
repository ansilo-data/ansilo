use ansilo_e2e::current_dir;
use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
fn test() {
    ansilo_logging::init_for_tests();
    let (_instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query(
            r#"SELECT row_to_json(j)::text FROM ansilo_catalog.jobs j"#,
            &[],
        )
        .unwrap();

    let rows: Vec<_> = rows.into_iter().map(|r| r.get::<_, String>(0)).collect();

    assert_eq!(rows, vec![r#"{"id":"cron_job","name":null,"description":null,"service_user_id":null,"sql":"SQL"}"#.to_string()]);
}

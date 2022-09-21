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
        .query(r#"SELECT * FROM ansilo_catalog.job_triggers"#, &[])
        .unwrap();

    let rows: Vec<_> = rows
        .into_iter()
        .map(|r| {
            (
                r.get::<_, Option<String>>("job_id"),
                r.get::<_, Option<String>>("cron"),
            )
        })
        .collect();

    assert_eq!(
        rows,
        vec![
            (Some("cron_job".into()), Some("1 * * * * *".into())),
            (Some("cron_job".into()), Some("2 * * * * *".into()))
        ]
    );
}

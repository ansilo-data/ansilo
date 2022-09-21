use ansilo_e2e::current_dir;
use itertools::Itertools;
use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
fn test() {
    ansilo_logging::init_for_tests();
    let (_instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    // Single stage build should
    let rows = client.query(r#"SELECT * FROM build"#, &[]).unwrap();

    let rows = rows
        .iter()
        .map(|r| (r.get::<_, String>("file"), r.get::<_, String>("usr")))
        .collect_vec();

    assert_eq!(
        rows,
        vec![
            ("001_stage".into(), "ansiloadmin".into()),
            ("002_stage".into(), "svc".into())
        ]
    );
}

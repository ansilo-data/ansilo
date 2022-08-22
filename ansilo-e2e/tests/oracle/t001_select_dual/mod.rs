use std::{env, thread};

use ansilo_main::args::{Args, Command};

#[test]
fn main() {
    ansilo_logging::init_for_tests();
    let containers = super::common::start_oracle();

    env::set_var(
        "ORACLE_IP",
        containers.get("oracle").unwrap().ip.to_string(),
    );

    ansilo_main::run(Command::Build(Args::config(
        crate::current_dir!().join("config.yml"),
    )))
    .unwrap();

    // TODO: better proc management
    thread::spawn(|| {
        ansilo_main::run(Command::Run(Args::config(
            crate::current_dir!().join("config.yml"),
        )))
        .unwrap();
    });
    thread::sleep_ms(5000);

    let mut client = crate::common::connect(65432);

    let rows = client.query("SELECT * FROM \"SYS.DUAL\"", &[]).unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0]
            .columns()
            .into_iter()
            .map(|c| c.name())
            .collect::<Vec<_>>(),
        vec!["DUMMY"]
    );
    assert_eq!(rows[0].get::<_, String>(0), "X".to_string());
}

use std::env;

use ansilo_main::{
    args::{Args, Command},
    Ansilo,
};

#[test]
fn main() {
    ansilo_logging::init_for_tests();
    let containers = super::common::start_oracle();

    env::set_var(
        "ORACLE_IP",
        containers.get("oracle").unwrap().ip.to_string(),
    );

    let _ansilo = Ansilo::start(Command::Run(Args::config(
        crate::current_dir!().join("config.yml"),
    )))
    .unwrap();

    let mut client = crate::common::connect(65432);

    let rows = client.query("SELECT * FROM \"SYS.DUAL\"", &[]).unwrap();

    // TODO: remote query log
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

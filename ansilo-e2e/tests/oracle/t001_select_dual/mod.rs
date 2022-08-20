use std::env;

use ansilo_main::args::{Args, Command};

#[test]
fn main() {
    ansilo_logging::init_for_tests();
    let containers = super::common::start_oracle();

    env::set_var(
        "ORACLE_IP",
        containers.get("oracle").unwrap().ip.to_string(),
    );
    ansilo_main::run(Command::Run(Args::config(
        crate::current_dir!().join("config.yml"),
    )))
    .unwrap();
}

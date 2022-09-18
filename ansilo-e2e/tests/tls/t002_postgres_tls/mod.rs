use std::{fs, time::Duration};

use ansilo_e2e::current_dir;
use native_tls::TlsConnector;
use postgres::{config::SslMode, Client};
use postgres_native_tls::MakeTlsConnector;
use serial_test::serial;

#[test]
#[serial]
fn test() {
    ansilo_logging::init_for_tests();
    let (_instance, port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    let mut conf = Client::configure();

    conf.connect_timeout(Duration::from_secs(30))
        .host("localhost")
        .port(port)
        .ssl_mode(SslMode::Require)
        .user("app")
        .password("pass")
        .dbname("postgres");

    let tls = TlsConnector::builder()
        .add_root_certificate(
            native_tls::Certificate::from_pem(
                fs::read(current_dir!().join("keys/rootCA.crt"))
                    .unwrap()
                    .as_slice(),
            )
            .unwrap(),
        )
        .build()
        .unwrap();

    let mut con = conf.connect(MakeTlsConnector::new(tls)).unwrap();

    con.batch_execute("SELECT 1").unwrap();
}

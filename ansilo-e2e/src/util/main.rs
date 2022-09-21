use std::{path::PathBuf, thread, time::Duration};

use ansilo_core::{data::uuid::Uuid, err::Result};
use ansilo_logging::info;
use ansilo_main::{
    args::{Args, Command},
    Ansilo, RemoteQueryLog,
};
use postgres::{Client, Config, NoTls};

/// Runs an instance of ansilo using the supplied config
pub fn run_instance(config_path: PathBuf) -> (Ansilo, Client) {
    let (instance, port) = run_instance_without_connect(config_path);

    let client = connect(port);

    (instance, client)
}

/// Runs an instance of ansilo using the supplied config
pub fn run_instance_args(
    config_path: PathBuf,
    args: &[(impl Into<String> + Clone, impl Into<String> + Clone)],
) -> (Ansilo, Client) {
    let (instance, port) = run_instance_without_connect_args(Args {
        config: Some(config_path),
        config_args: args
            .iter()
            .cloned()
            .map(|(k, v)| (k.into(), v.into()))
            .collect(),
        force_build: true,
    });

    let client = connect(port);

    (instance, client)
}

/// Runs an instance of ansilo using the supplied config
pub fn run_instance_without_connect(config_path: PathBuf) -> (Ansilo, u16) {
    run_instance_without_connect_args(Args {
        config: Some(config_path),
        config_args: vec![],
        force_build: true,
    })
}

/// Runs an instance of ansilo using the supplied args
pub fn run_instance_without_connect_args(mut args: Args) -> (Ansilo, u16) {
    args.config_args.push((
        "TEMP_DIR".into(),
        format!("/tmp/ansilo-e2e/{}", Uuid::new_v4()),
    ));
    let instance =
        Ansilo::start(Command::Run(args), Some(RemoteQueryLog::store_in_memory())).unwrap();

    let port = loop {
        let addrs = instance.subsystems().unwrap().proxy().addrs().unwrap();

        if addrs.is_empty() {
            thread::sleep(Duration::from_millis(10));
            continue;
        }

        break addrs[0].port();
    };

    (instance, port)
}

/// Connects to the ansilo instance running on the supplied port
/// Authenticates using "app" / "pass" as a convention
pub fn connect(port: u16) -> Client {
    info!("Connection to local instance on localhost:{}", port);

    connect_opts("app", "pass", port, |_| ()).unwrap()
}

/// Connects to the ansilo instance running on the supplied port
pub fn connect_to_as_user(instance: &Ansilo, user: &str, pass: &str) -> Client {
    let addrs = instance.subsystems().unwrap().proxy().addrs().unwrap();

    connect_opts(user, pass, addrs[0].port(), |_| ()).unwrap()
}

/// Connects to the ansilo instance running on the supplied port
pub fn connect_opts(
    user: &str,
    pass: &str,
    port: u16,
    mut cb: impl FnMut(&mut Config),
) -> Result<Client> {
    info!("Connection to local instance on localhost:{}", port);

    let mut conf = Client::configure();

    conf.connect_timeout(Duration::from_secs(30))
        .port(port)
        .host("localhost")
        .user(user)
        .password(pass)
        .dbname("postgres");

    cb(&mut conf);

    Ok(conf.connect(NoTls)?)
}

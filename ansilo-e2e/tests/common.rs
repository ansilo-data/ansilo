use std::{fs, path::PathBuf, thread, time::Duration};

use ansilo_logging::info;
use ansilo_main::{
    args::{Args, Command},
    Ansilo, RemoteQueryLog,
};
use postgres::{Client, NoTls};

/// Runs an instance of ansilo using the supplied config
pub fn run_instance(config_path: PathBuf) -> (Ansilo, Client) {
    let config: serde_yaml::Value =
        serde_yaml::from_str(&fs::read_to_string(config_path.clone()).unwrap()).unwrap();
    let port = config["networking"].as_mapping().unwrap()["port"]
        .as_u64()
        .unwrap() as u16;

    let instance = Ansilo::start(
        Command::Run(Args::testing(config_path)),
        Some(RemoteQueryLog::store_in_memory()),
    )
    .unwrap();

    let client = connect(port);

    (instance, client)
}

/// Connects to the ansilo instance running on the supplied port
fn connect(port: u16) -> Client {
    // TODO: auth
    info!("Connection to local instance on localhost:{}", port);

    Client::configure()
        .connect_timeout(Duration::from_secs(30))
        .port(port)
        .host("localhost")
        .user("ansiloapp")
        .dbname("postgres")
        .connect(NoTls)
        .unwrap()
}

pub fn debug(instance: &Ansilo) {
    fs::write("/dev/tty", "== Halting test for debugging ==\n").unwrap();
    fs::write(
        "/dev/tty",
        format!(
            "Run: psql -h localhost -p {} -U ansiloapp -d postgres\n",
            instance.conf().node.networking.port
        ),
    )
    .unwrap();
    loop {
        thread::sleep(Duration::from_secs(3600));
    }
}

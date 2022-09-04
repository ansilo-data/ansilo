use std::{
    env,
    path::PathBuf,
    sync::{
        atomic::{AtomicU16, Ordering},
        Mutex,
    },
    time::Duration,
};

use ansilo_core::err::Result;
use ansilo_logging::info;
use ansilo_main::{
    args::{Args, Command},
    Ansilo, RemoteQueryLog,
};
use postgres::{Client, NoTls};

static PORT: AtomicU16 = AtomicU16::new(60000);

static LOCK: Mutex<()> = Mutex::new(());

/// Runs an instance of ansilo using the supplied config
pub fn run_instance(config_path: PathBuf) -> (Ansilo, Client) {
    let (instance, port) = run_instance_without_connect(config_path);

    let client = connect(port);

    (instance, client)
}

/// Runs an instance of ansilo using the supplied config
pub fn run_instance_without_connect(config_path: PathBuf) -> (Ansilo, u16) {
    let _ = LOCK.lock().unwrap();
    let port = PORT.fetch_add(1, Ordering::SeqCst);

    // Allow port to be referenced in config file
    env::set_var("ANSILO_PORT", port.to_string());

    let instance = Ansilo::start(
        Command::Run(Args::testing(config_path)),
        Some(RemoteQueryLog::store_in_memory()),
    )
    .unwrap();

    (instance, port)
}

/// Connects to the ansilo instance running on the supplied port
/// Authenticates using "app" / "pass" as a convention
pub fn connect(port: u16) -> Client {
    info!("Connection to local instance on localhost:{}", port);

    connect_opts("app", "pass", port).unwrap()
}

/// Connects to the ansilo instance running on the supplied port
pub fn connect_opts(user: &str, pass: &str, port: u16) -> Result<Client> {
    info!("Connection to local instance on localhost:{}", port);

    Ok(Client::configure()
        .connect_timeout(Duration::from_secs(30))
        .port(port)
        .host("localhost")
        .user(user)
        .password(pass)
        .dbname("postgres")
        .connect(NoTls)?)
}

use std::{
    env,
    path::PathBuf,
    sync::{atomic::{AtomicU16, Ordering}, Mutex},
    time::Duration,
};

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
    let _ = LOCK.lock().unwrap(); 
    let port = PORT.fetch_add(1, Ordering::SeqCst);

    // Allow port to be referenced in config file
    env::set_var("ANSILO_PORT", port.to_string());

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

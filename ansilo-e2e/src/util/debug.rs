use std::{fs, thread, time::Duration};

use ansilo_main::Ansilo;

pub fn debug(instance: &Ansilo) {
    let user = &instance.conf().node.auth.users[0];
    let username = user.username.clone();
    let password = user
        .r#type
        .as_password()
        .map(|p| p.password.clone())
        .unwrap_or_default();
    let port = instance.subsystems().unwrap().proxy().addrs().unwrap()[0].port();

    fs::write("/dev/tty", "== Halting test for debugging ==\n").unwrap();
    fs::write(
        "/dev/tty",
        format!(
            "Run: PGPASSWORD={password} psql -h localhost -p {port} -U {username} -d postgres\n",
        ),
    )
    .unwrap();

    loop {
        thread::sleep(Duration::from_secs(3600));
    }
}

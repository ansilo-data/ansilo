use std::{fs, thread, time::Duration};

use ansilo_main::Ansilo;

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

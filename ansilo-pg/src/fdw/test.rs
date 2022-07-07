use std::{
    fs,
    os::unix::net::{UnixListener, UnixStream},
    thread,
};

use super::channel::{IpcClientChannel, IpcServerChannel};

pub(crate) fn create_tmp_ipc_channel(name: &'static str) -> (IpcClientChannel, IpcServerChannel) {
    let path = format!("/tmp/ansilo-ipc-{name}.sock");
    let _ = fs::remove_file(path.clone());
    let listener = UnixListener::bind(path.clone()).unwrap();

    let listen_thread = thread::spawn(move || listener.accept().unwrap().0);

    let client_stream = UnixStream::connect(path).unwrap();
    let server_stream = listen_thread.join().unwrap();

    (
        IpcClientChannel::new(client_stream),
        IpcServerChannel::new(server_stream),
    )
}

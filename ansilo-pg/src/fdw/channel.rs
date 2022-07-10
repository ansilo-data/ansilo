use std::{io::Write, os::unix::net::UnixStream};

use ansilo_core::err::{Context, Result};
use ansilo_logging::error;

use super::{
    bincode::bincode_conf,
    proto::{ClientMessage, ServerMessage},
};

/// A request-response channel used for IPC between postgres and ansilo
pub struct IpcClientChannel {
    /// The underlying unix socket
    sock: UnixStream,
    /// The binconde config used for serialisation
    conf: bincode::config::Configuration,
    /// Whether the connection has been closed
    closed: bool,
}

/// A request-response channel used for IPC between postgres and ansilo
pub(crate) struct IpcServerChannel {
    /// The underlying unix socket
    sock: UnixStream,
    /// The binconde config used for serialisation
    conf: bincode::config::Configuration,
}

impl IpcClientChannel {
    pub fn new(sock: UnixStream) -> Self {
        Self {
            sock,
            conf: bincode_conf(),
            closed: false,
        }
    }

    /// Sends the supplied message and waits for the response
    pub fn send(&mut self, req: ClientMessage) -> Result<ServerMessage> {
        bincode::encode_into_std_write::<ClientMessage, _, _>(
            req,
            &mut self.sock,
            self.conf.clone(),
        )
        .context("Failed to send message")?;

        self.sock.flush().context("Failed to flush sock")?;

        let res =
            bincode::decode_from_std_read::<ServerMessage, _, _>(&mut self.sock, self.conf.clone())
                .context("Failed to read message")?;

        Ok(res)
    }

    /// Sends the a close message to the server
    pub fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }

        bincode::encode_into_std_write::<ClientMessage, _, _>(
            ClientMessage::Close,
            &mut self.sock,
            self.conf.clone(),
        )
        .context("Failed to send message")?;

        self.sock.flush().context("Failed to flush sock")?;
        self.closed = true;
        Ok(())
    }
}

impl Drop for IpcClientChannel {
    fn drop(&mut self) {
        if let Err(err) = self.close() {
            error!("Failed to close ipc channel: {}", err);
        }
    }
}

impl IpcServerChannel {
    pub fn new(sock: UnixStream) -> Self {
        Self {
            sock,
            conf: bincode_conf(),
        }
    }

    /// Receives the next message from the client, passing it to the supplied closure
    pub fn recv<F>(&mut self, cb: F) -> Result<Option<ServerMessage>>
    where
        F: FnOnce(ClientMessage) -> Result<Option<ServerMessage>>,
    {
        let req =
            bincode::decode_from_std_read::<ClientMessage, _, _>(&mut self.sock, self.conf.clone())
                .context("Failed to receive message")?;

        let res = cb(req)?;

        if res.is_none() {
            return Ok(None);
        }

        let res = res.unwrap();
        bincode::encode_into_std_write::<ServerMessage, _, _>(
            res.clone(),
            &mut self.sock,
            self.conf.clone(),
        )
        .context("Failed to send message")?;

        self.sock.flush().context("Failed to flush sock")?;

        Ok(Some(res))
    }
}

#[cfg(test)]
mod tests {
    use std::{os::unix::prelude::AsRawFd, thread};

    use nix::libc::close;

    use crate::fdw::test::create_tmp_ipc_channel;

    use super::*;

    #[test]
    fn test_ipc_channel_send_recv() {
        let (mut client, mut server) = create_tmp_ipc_channel("send_recv");

        let server_thread = thread::spawn(move || {
            server
                .recv(|req| {
                    assert_eq!(req, ClientMessage::AuthDataSource("AUTH".to_string()));
                    Ok(Some(ServerMessage::AuthAccepted))
                })
                .unwrap();
        });

        let res = client
            .send(ClientMessage::AuthDataSource("AUTH".to_string()))
            .unwrap();

        assert_eq!(res, ServerMessage::AuthAccepted);
        server_thread.join().unwrap();
    }

    #[test]
    fn test_ipc_channel_send_recv_multiple() {
        let (mut client, mut server) = create_tmp_ipc_channel("send_recv_multiple");

        let server_thread = thread::spawn(move || {
            for _ in 1..100 {
                server
                    .recv(|req| {
                        assert_eq!(req, ClientMessage::Execute);
                        Ok(Some(ServerMessage::AuthAccepted))
                    })
                    .unwrap();
            }
        });

        for _ in 1..100 {
            let res = client.send(ClientMessage::Execute).unwrap();
            assert_eq!(res, ServerMessage::AuthAccepted);
        }

        server_thread.join().unwrap();
    }

    #[test]
    fn test_ipc_channel_send_recv_large() {
        let (mut client, mut server) = create_tmp_ipc_channel("send_recv_large");
        let param_buff = [8u8; 10240];
        let result_buff = [16u8; 10240];

        let server_thread = thread::spawn(move || {
            for _ in 1..10 {
                server
                    .recv(|req| {
                        assert_eq!(req, ClientMessage::WriteParams(param_buff.to_vec()));
                        Ok(Some(ServerMessage::ResultData(result_buff.to_vec())))
                    })
                    .unwrap();
            }
        });

        for _ in 1..10 {
            let res = client
                .send(ClientMessage::WriteParams(param_buff.to_vec()))
                .unwrap();
            assert_eq!(res, ServerMessage::ResultData(result_buff.to_vec()));
        }

        server_thread.join().unwrap();
    }

    #[test]
    fn test_ipc_channel_client_unexpected_close() {
        let (client, mut server) = create_tmp_ipc_channel("client_unexpected_close");

        let server_thread = thread::spawn(move || {
            server.recv(|_req| unreachable!()).unwrap_err();
        });

        unsafe {
            let fd = client.sock.as_raw_fd();
            close(fd);
            std::mem::forget(client);
        }
        server_thread.join().unwrap();
    }

    #[test]
    fn test_ipc_channel_server_unexpected_close() {
        let (mut client, server) = create_tmp_ipc_channel("server_unexpected_close");

        drop(server);

        client.send(ClientMessage::Prepare).unwrap_err();
    }

    #[test]
    fn test_ipc_channel_graceful_close() {
        let (mut client, mut server) = create_tmp_ipc_channel("graceful_close");

        let server_thread = thread::spawn(move || {
            server
                .recv(|req| {
                    assert_eq!(req, ClientMessage::Close);
                    Ok(None)
                })
                .unwrap();
        });

        client.close().unwrap();

        server_thread.join().unwrap();
    }

    #[test]
    fn test_ipc_channel_graceful_close_via_drop() {
        let (client, mut server) = create_tmp_ipc_channel("graceful_close_drop");

        let server_thread = thread::spawn(move || {
            server
                .recv(|req| {
                    assert_eq!(req, ClientMessage::Close);
                    Ok(None)
                })
                .unwrap();
        });

        drop(client);

        server_thread.join().unwrap();
    }
}

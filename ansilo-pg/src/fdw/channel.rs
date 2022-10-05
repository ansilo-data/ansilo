use std::{
    fmt,
    io::{self, Read, Write},
    mem::size_of,
    os::unix::net::UnixStream,
};

use ansilo_core::err::{Context, Result};
use ansilo_logging::{error, trace};
use bincode::{Decode, Encode};

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
        send_message(&mut self.sock, req, &self.conf)?;

        let res = recv_message(&mut self.sock, &self.conf)?;

        Ok(res)
    }

    /// Sends the a close message to the server
    pub fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }

        send_message(&mut self.sock, ClientMessage::Close, &self.conf)?;

        self.closed = true;
        Ok(())
    }
}

impl Drop for IpcClientChannel {
    fn drop(&mut self) {
        if let Err(err) = self.close() {
            error!("Failed to close ipc channel: {:?}", err);
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
        self.recv_with_return(move |msg| {
            let res = cb(msg)?;

            Ok((res.clone(), res))
        })
    }

    /// Receives the next message from the client, passing it to the supplied closure
    pub fn recv_with_return<F, R>(&mut self, cb: F) -> Result<R>
    where
        F: FnOnce(ClientMessage) -> Result<(Option<ServerMessage>, R)>,
    {
        let req = recv_message(&mut self.sock, &self.conf)?;
        trace!("Received from postgres: {:?} [{:?}]", req, self.sock);

        let (res, ret) = cb(req)?;

        if res.is_none() {
            return Ok(ret);
        }

        let res = res.unwrap();
        trace!("Response to postgres: {:?} [{:?}]", res, self.sock);

        send_message(&mut self.sock, res, &self.conf)?;

        Ok(ret)
    }
}

fn send_message<T: Encode>(
    sock: &mut UnixStream,
    msg: T,
    conf: &bincode::config::Configuration,
) -> Result<()> {
    // let buff = io::Cursor::new(vec![0u8; size_of::<usize>()]);
    // bincode::encode_into_std_write::<T, _>(msg, &mut buff, conf.clone())
    //     .context("Failed to encode message")?;
    // let buff = buff.into_inner();
    // let len = buff.len() - size_of::<usize>();
    // buff[..size_of::<usize>()].copy_from_slice(len.to_be_bytes());
    let buff =
        bincode::encode_to_vec::<T, _>(msg, conf.clone()).context("Failed to encode message")?;
    let len = buff.len();

    sock.write_all(&len.to_be_bytes())
        .and_then(|_| sock.write_all(buff.as_slice()))
        .context("Failed to send message")?;
    sock.flush().context("Failed to flush sock")?;

    Ok(())
}

fn recv_message<T: Decode>(
    sock: &mut UnixStream,
    conf: &bincode::config::Configuration,
) -> Result<T> {
    let mut len = [0u8; size_of::<usize>()];
    sock.read_exact(&mut len)
        .context("Failed to read message size")?;
    let len = usize::from_be_bytes(len);

    let mut buff = vec![0u8; len];
    sock.read_exact(&mut buff[..len])
        .context("Failed to read message")?;

    let msg = bincode::decode_from_std_read::<T, _, _>(&mut io::Cursor::new(buff), conf.clone())
        .context("Failed to decode message")?;

    Ok(msg)
}

impl fmt::Debug for IpcClientChannel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IpcClientChannel")
            .field("sock", &self.sock)
            .field("closed", &self.closed)
            .finish()
    }
}

impl fmt::Debug for IpcServerChannel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IpcServerChannel")
            .field("sock", &self.sock)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::{os::unix::prelude::AsRawFd, thread};

    use nix::libc::close;

    use crate::fdw::{
        proto::{AuthDataSource, ClientQueryMessage, ServerQueryMessage},
        test::create_tmp_ipc_channel,
    };

    use super::*;

    #[test]
    fn test_ipc_channel_send_recv() {
        let (mut client, mut server) = create_tmp_ipc_channel("send_recv");

        let server_thread = thread::spawn(move || {
            server
                .recv(|req| {
                    assert_eq!(
                        req,
                        ClientMessage::AuthDataSource(AuthDataSource::new(None, "DATA_SOURCE"))
                    );
                    Ok(Some(ServerMessage::AuthAccepted))
                })
                .unwrap();
        });

        let res = client
            .send(ClientMessage::AuthDataSource(AuthDataSource::new(
                None,
                "DATA_SOURCE",
            )))
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
                        assert_eq!(req, ClientMessage::Close);
                        Ok(Some(ServerMessage::AuthAccepted))
                    })
                    .unwrap();
            }
        });

        for _ in 1..100 {
            let res = client.send(ClientMessage::Close).unwrap();
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
                        assert_eq!(
                            req,
                            ClientMessage::Query(
                                0,
                                ClientQueryMessage::WriteParams(param_buff.to_vec())
                            )
                        );
                        Ok(Some(ServerMessage::Query(ServerQueryMessage::ReadData(
                            result_buff.to_vec(),
                        ))))
                    })
                    .unwrap();
            }
        });

        for _ in 1..10 {
            let res = client
                .send(ClientMessage::Query(
                    0,
                    ClientQueryMessage::WriteParams(param_buff.to_vec()),
                ))
                .unwrap();
            assert_eq!(
                res,
                ServerMessage::Query(ServerQueryMessage::ReadData(result_buff.to_vec()))
            );
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

        client
            .send(ClientMessage::Query(0, ClientQueryMessage::Prepare))
            .unwrap_err();
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

use std::{
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::{self, JoinHandle},
};

use ansilo_core::err::{bail, Context, Result};
use ansilo_logging::{debug, error, warn};
use socket2::{Domain, Socket};

use crate::conf::ProxyConf;

/// The multi-protocol proxy server
pub struct ProxyServer {
    conf: &'static ProxyConf,
    listeners: Option<Vec<(JoinHandle<()>, SocketAddr)>>,
    terminated: Arc<AtomicBool>,
}

impl ProxyServer {
    pub fn new(conf: &'static ProxyConf) -> Self {
        Self {
            conf,
            listeners: None,
            terminated: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Starts the proxy server
    pub fn start(&mut self) -> Result<()> {
        if self.listeners.is_some() {
            bail!("Server already listening");
        }

        self.listeners = Some(
            self.conf
                .addrs
                .iter()
                .cloned()
                .map(|addr| {
                    ProxyListener::start(self.conf, addr, Arc::clone(&self.terminated))
                        .map(|l| (l, addr))
                })
                .collect::<Result<Vec<_>>>()?,
        );

        Ok(())
    }

    /// Terminates the proxy server
    pub fn terminate(mut self) -> Result<()> {
        self.terminate_mut()
    }

    /// Terminates the proxy server
    fn terminate_mut(&mut self) -> Result<()> {
        if self.listeners.is_none() {
            bail!("Server not listening");
        }

        if self.terminated.load(Ordering::SeqCst) {
            return Ok(());
        }

        self.terminated.store(true, Ordering::SeqCst);

        // Trigger a TCP connection to each of the listeners to unblock them
        // in order to terminate
        for (listener, addr) in self.listeners.take().unwrap().into_iter() {
            thread::spawn(move || {
                if let Err(err) = TcpStream::connect(addr.clone()) {
                    debug!(
                        "Failed to connect to {:?} while terminating: {:?}",
                        addr, err
                    );
                }
            });

            if let Err(_) = listener.join() {
                warn!("Failed to join listener thread while terminating");
            }
        }

        Ok(())
    }
}

/// Binds to a socket and accepts new connections
pub struct ProxyListener {
    conf: &'static ProxyConf,
    listener: TcpListener,
    terminated: Arc<AtomicBool>,
}

impl ProxyListener {
    pub fn start(
        conf: &'static ProxyConf,
        addr: SocketAddr,
        terminated: Arc<AtomicBool>,
    ) -> Result<JoinHandle<()>> {
        let socket = Socket::new(
            Domain::for_address(addr),
            socket2::Type::STREAM,
            Some(socket2::Protocol::TCP),
        )?;

        socket
            .set_reuse_address(true)
            .context("Failed to set SO_REUSEADDR")?;

        socket
            .bind(&addr.into())
            .context("Failed to bind to address")?;
        socket.listen(128)?;

        let mut listener = Self {
            conf,
            listener: socket.into(),
            terminated,
        };

        Ok(thread::spawn(move || {
            if let Err(err) = listener.accept() {
                error!("Error while listening on addr {}: {:?}", addr, err)
            }
        }))
    }

    fn accept(&mut self) -> Result<()> {
        loop {
            let (_con, _) = self.listener.accept().context("Failed to listen to addr")?;

            if self.terminated.load(Ordering::SeqCst) {
                return Ok(());
            }
        }
    }
}

impl Drop for ProxyServer {
    fn drop(&mut self) {
        if self.listeners.is_some() {
            if let Err(err) = self.terminate_mut() {
                warn!("Failed to terminate proxy server: {:?}", err);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::Write,
        net::{Ipv4Addr, SocketAddrV4},
        sync::atomic::AtomicU16,
    };

    use super::*;

    static PORT: AtomicU16 = AtomicU16::new(61000);

    fn create_server(conf: ProxyConf) -> ProxyServer {
        let conf = Box::leak(Box::new(conf));

        ProxyServer::new(conf)
    }

    fn mock_config() -> ProxyConf {
        let port = PORT.fetch_add(1, Ordering::Relaxed);

        ProxyConf {
            addrs: vec![SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))],
            tls: None,
            auth: vec![],
        }
    }

    #[test]
    fn test_server_new_and_drop() {
        let server = create_server(mock_config());

        assert!(server.listeners.is_none());
        assert_eq!(server.terminated.load(Ordering::SeqCst), false);

        TcpStream::connect(server.conf.addrs[0]).unwrap_err();
    }

    #[test]
    fn test_server_start_and_connect_then_terminate() {
        let mut server = create_server(mock_config());

        server.start().unwrap();
        assert_eq!(server.listeners.as_ref().unwrap().len(), 1);

        let mut con = TcpStream::connect(server.conf.addrs[0]).unwrap();

        // Connection should be writable
        con.write_all(&[1]).unwrap();
        con.flush().unwrap();

        server.terminate_mut().unwrap();

        assert!(server.listeners.is_none());
        assert_eq!(server.terminated.load(Ordering::SeqCst), true);

        // Connection should now fail
        con.write_all(&[1]).unwrap_err();
    }

    #[test]
    fn test_server_start_and_connect_then_drop() {
        let mut server = create_server(mock_config());

        server.start().unwrap();
        assert_eq!(server.listeners.as_ref().unwrap().len(), 1);

        let mut con = TcpStream::connect(server.conf.addrs[0]).unwrap();

        // Connection should be writable
        con.write_all(&[1]).unwrap();

        drop(server);

        // Connection should now fail
        con.write_all(&[1]).unwrap_err();
    }
}

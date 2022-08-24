use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use ansilo_core::err::{bail, Context, Result};
use ansilo_logging::{debug, error, info, warn};
use socket2::{Domain, Socket};
use tokio::{
    net::{TcpListener, TcpStream},
    runtime::Runtime,
};

use crate::{conf::ProxyConf, connection::Connection};

/// The multi-protocol proxy server
pub struct ProxyServer {
    conf: &'static ProxyConf,
    runtime: Option<Runtime>,
    terminated: Arc<AtomicBool>,
}

impl ProxyServer {
    pub fn new(conf: &'static ProxyConf) -> Self {
        Self {
            conf,
            runtime: None,
            terminated: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Starts the proxy server
    pub fn start(&mut self) -> Result<()> {
        if self.runtime.is_some() {
            bail!("Server already listening");
        }

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .thread_name("proxy-server")
            .enable_io()
            .build()
            .context("Failed to create tokio runtime")?;

        let listeners = self
            .conf
            .addrs
            .iter()
            .cloned()
            .map(|addr| {
                runtime.block_on(ProxyListener::start(
                    self.conf,
                    addr,
                    Arc::clone(&self.terminated),
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        for mut listener in listeners {
            runtime.spawn(async move {
                if let Err(err) = listener.accept().await {
                    error!("Failed to listen on addr: {:?}", err)
                }
            });
        }

        self.runtime = Some(runtime);

        Ok(())
    }

    /// Terminates the proxy server
    pub fn terminate(mut self) -> Result<()> {
        self.terminate_mut()
    }

    /// Terminates the proxy server
    fn terminate_mut(&mut self) -> Result<()> {
        if self.runtime.is_none() {
            bail!("Server not listening");
        }

        if self.terminated.load(Ordering::SeqCst) {
            return Ok(());
        }

        self.terminated.store(true, Ordering::SeqCst);

        // Trigger a TCP connection to each of the listeners to unblock them
        // in order to terminate
        for addr in self.conf.addrs.iter().cloned() {
            self.runtime.as_ref().unwrap().spawn(async move {
                if let Err(err) = TcpStream::connect(addr).await {
                    debug!(
                        "Failed to connect to {:?} while terminating: {:?}",
                        addr, err
                    );
                }
            });

            self.runtime
                .take()
                .unwrap()
                .shutdown_timeout(Duration::from_secs(3));
        }

        Ok(())
    }
}

/// Binds to a socket and accepts new connections
struct ProxyListener {
    conf: &'static ProxyConf,
    listener: TcpListener,
    terminated: Arc<AtomicBool>,
}

impl ProxyListener {
    async fn start(
        conf: &'static ProxyConf,
        addr: SocketAddr,
        terminated: Arc<AtomicBool>,
    ) -> Result<Self> {
        let socket = Socket::new(
            Domain::for_address(addr),
            socket2::Type::STREAM,
            Some(socket2::Protocol::TCP),
        )?;

        socket
            .set_reuse_address(true)
            .context("Failed to set SO_REUSEADDR")?;

        socket
            .set_read_timeout(Some(Duration::from_secs(30)))
            .context("Failed to set socket read timeout")?;
        socket
            .set_write_timeout(Some(Duration::from_secs(30)))
            .context("Failed to set socket write timeout")?;

        socket
            .bind(&addr.into())
            .context("Failed to bind to address")?;
        socket.listen(128)?;

        socket
            .set_nonblocking(true)
            .context("Failed to set socket to non-blocking mode")?;

        let listener = Self {
            conf,
            listener: TcpListener::from_std(socket.into())?,
            terminated,
        };

        Ok(listener)
    }

    /// Accepts new connections
    async fn accept(&mut self) -> Result<()> {
        info!("Listening on {}", self.listener.local_addr()?);
        
        loop {
            let (con, _) = self
                .listener
                .accept()
                .await
                .context("Failed to listen to addr")?;

            if self.terminated.load(Ordering::SeqCst) {
                return Ok(());
            }

            tokio::spawn(Connection::new(self.conf, con).handle());
        }
    }
}

impl Drop for ProxyServer {
    fn drop(&mut self) {
        if self.runtime.is_some() {
            if let Err(err) = self.terminate_mut() {
                warn!("Failed to terminate proxy server: {:?}", err);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Write, net::TcpStream};

    use crate::test::mock_config_no_tls;

    use super::*;

    fn create_server(conf: &'static ProxyConf) -> ProxyServer {
        ProxyServer::new(conf)
    }

    #[test]
    fn test_server_new_and_drop() {
        let server = create_server(mock_config_no_tls());

        assert!(server.runtime.is_none());
        assert_eq!(server.terminated.load(Ordering::SeqCst), false);

        TcpStream::connect(server.conf.addrs[0]).unwrap_err();
    }

    #[test]
    fn test_server_start_and_connect_then_terminate() {
        let mut server = create_server(mock_config_no_tls());

        server.start().unwrap();
        assert_eq!(server.runtime.is_some(), true);

        let mut con = TcpStream::connect(server.conf.addrs[0]).unwrap();

        // Connection should be writable
        con.write_all(&[1]).unwrap();
        con.flush().unwrap();

        server.terminate_mut().unwrap();

        assert!(server.runtime.is_none());
        assert_eq!(server.terminated.load(Ordering::SeqCst), true);

        // Connection should now fail
        con.write_all(&[1]).and_then(|_| con.flush()).unwrap_err();
    }

    #[test]
    fn test_server_start_and_connect_then_drop() {
        let mut server = create_server(mock_config_no_tls());

        server.start().unwrap();
        assert_eq!(server.runtime.is_some(), true);

        let mut con = TcpStream::connect(server.conf.addrs[0]).unwrap();

        // Connection should be writable
        con.write_all(&[1]).unwrap();

        drop(server);

        // Connection should now fail
        con.write_all(&[1]).and_then(|_| con.flush()).unwrap_err();
    }
}

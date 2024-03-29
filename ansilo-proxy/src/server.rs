use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use ansilo_core::err::{Context, Error, Result};
use ansilo_logging::{debug, error, info, trace, warn};
use socket2::{Domain, Socket};
use tokio::{
    net::TcpListener,
    sync::broadcast::{self, Receiver, Sender},
    task::JoinHandle,
};

use crate::{conf::ProxyConf, connection::Connection};

/// The multi-protocol proxy server
pub struct ProxyServer {
    conf: &'static ProxyConf,
    addrs: Arc<Mutex<Vec<SocketAddr>>>,
    listeners: Vec<JoinHandle<()>>,
    terminator: Option<(Sender<()>, Receiver<()>)>,
}

impl ProxyServer {
    pub fn new(conf: &'static ProxyConf) -> Self {
        Self {
            conf,
            addrs: Arc::new(Mutex::new(vec![])),
            listeners: vec![],
            terminator: Some(broadcast::channel(1)),
        }
    }

    /// Starts the proxy server
    pub async fn start(&mut self) -> Result<()> {
        let listeners = self
            .conf
            .addrs
            .iter()
            .cloned()
            .map(|addr| {
                ProxyListener::start(
                    self.conf,
                    Arc::clone(&self.addrs),
                    addr,
                    self.terminator.as_ref().unwrap().0.subscribe(),
                )
            })
            .collect::<Vec<_>>();

        let listeners = futures::future::try_join_all(listeners).await?;

        for mut listener in listeners {
            self.listeners.push(tokio::spawn(async move {
                if let Err(err) = listener.accept().await {
                    error!("Failed to listen on addr: {:?}", err)
                }
            }));
        }

        Ok(())
    }

    /// Gets the socket addresses the server is listening on
    pub fn addrs(&self) -> Result<Vec<SocketAddr>> {
        Ok(self
            .addrs
            .lock()
            .map_err(|_| Error::msg("Failed to lock addrs"))?
            .clone())
    }

    /// Checks whether the proxy server is healthy
    pub fn healthy(&self) -> bool {
        self.listeners.iter().all(|l| !l.is_finished())
    }

    /// Terminates the proxy server
    pub fn terminate(mut self) -> Result<()> {
        self.terminate_mut()
    }

    /// Terminates the proxy server
    fn terminate_mut(&mut self) -> Result<()> {
        if self.terminator.is_none() {
            return Ok(());
        }

        // Drop the terminator sender to trigger all listeners to shutdown
        self.terminator.take().unwrap();

        Ok(())
    }
}

/// Binds to a socket and accepts new connections
struct ProxyListener {
    conf: &'static ProxyConf,
    addrs: Arc<Mutex<Vec<SocketAddr>>>,
    listener: Option<TcpListener>,
    terminator: Receiver<()>,
}

impl ProxyListener {
    async fn start(
        conf: &'static ProxyConf,
        addrs: Arc<Mutex<Vec<SocketAddr>>>,
        addr: SocketAddr,
        terminator: Receiver<()>,
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
            .with_context(|| format!("Failed to bind to address: {}", addr))?;
        socket.listen(128)?;

        socket
            .set_nonblocking(true)
            .context("Failed to set socket to non-blocking mode")?;

        let listener = Self {
            conf,
            addrs,
            listener: Some(TcpListener::from_std(socket.into())?),
            terminator,
        };

        Ok(listener)
    }

    /// Accepts new connections
    async fn accept(&mut self) -> Result<()> {
        let listen_addr = self.listener.as_ref().unwrap().local_addr()?;
        info!("Listening on {}", listen_addr);

        {
            let mut addrs = self
                .addrs
                .lock()
                .map_err(|_| Error::msg("Failed to lock addrs"))?;
            addrs.push(listen_addr)
        }

        loop {
            let (con, _) = tokio::select! {
                con = self.listener.as_mut().unwrap().accept()  => con.context("Failed to accept connection")?,
                _ = self.terminator.recv() => {
                    debug!("Shutting down listener");
                    self.listener.take().unwrap();
                    return Ok(());
                }
            };

            trace!("Received connection from {:?}", con.peer_addr().ok());

            let conf = self.conf;
            tokio::spawn(async move {
                if let Err(err) = Connection::new(conf, con).handle().await {
                    warn!("Error while handling connection: {:?}", err)
                }
            });
        }
    }
}

impl Drop for ProxyServer {
    fn drop(&mut self) {
        if let Err(err) = self.terminate_mut() {
            warn!("Failed to terminate proxy server: {:?}", err);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Write, net::TcpStream};

    use tokio::task::yield_now;

    use crate::test::{mock_config_no_tls, mock_config_no_tls_with_port};

    use super::*;

    fn create_server(conf: &'static ProxyConf) -> ProxyServer {
        ProxyServer::new(conf)
    }

    #[tokio::test]
    async fn test_server_new_and_drop() {
        let server = create_server(mock_config_no_tls());

        assert!(server.terminator.is_some());
        assert!(server.healthy());

        TcpStream::connect(server.conf.addrs[0]).unwrap_err();
    }

    #[tokio::test]
    async fn test_server_start_and_connect_then_terminate() {
        ansilo_logging::init_for_tests();
        let mut server = create_server(mock_config_no_tls());

        server.start().await.unwrap();
        assert!(server.healthy());

        let mut con = TcpStream::connect(server.conf.addrs[0]).unwrap();

        // Connection should be writable
        con.write_all(&[1]).unwrap();
        con.flush().unwrap();

        server.terminate_mut().unwrap();
        yield_now().await;

        assert!(server.terminator.is_none());
        assert!(!server.healthy());

        // Connection should now fail
        con.write_all(&[1]).and_then(|_| con.flush()).unwrap_err();
    }

    #[tokio::test]
    async fn test_server_start_and_connect_then_drop() {
        ansilo_logging::init_for_tests();
        let mut server = create_server(mock_config_no_tls());

        server.start().await.unwrap();

        let mut con = TcpStream::connect(server.conf.addrs[0]).unwrap();

        // Connection should be writable
        con.write_all(&[1]).unwrap();

        drop(server);
        yield_now().await;

        // Connection should now fail
        con.write_all(&[1]).and_then(|_| con.flush()).unwrap_err();
    }

    #[tokio::test]
    async fn test_server_start_and_get_listening_addrs() {
        ansilo_logging::init_for_tests();
        let mut server = create_server(mock_config_no_tls_with_port(12345));

        server.start().await.unwrap();
        yield_now().await;

        assert_eq!(
            server.addrs().unwrap(),
            vec!["127.0.0.1:12345".parse().unwrap()]
        )
    }

    #[tokio::test]
    async fn test_server_start_and_get_listening_with_port_0() {
        ansilo_logging::init_for_tests();
        let mut server = create_server(mock_config_no_tls_with_port(0));

        server.start().await.unwrap();
        yield_now().await;

        assert_eq!(server.addrs().unwrap().len(), 1);
        // Should return kernel allocated port
        assert_ne!(
            server.addrs().unwrap(),
            vec!["127.0.0.1:0".parse().unwrap()]
        )
    }
}

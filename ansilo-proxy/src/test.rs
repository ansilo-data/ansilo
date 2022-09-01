use std::{
    fs, io, net::SocketAddr, os::unix::net::UnixStream as StdUnixStream,
    os::unix::prelude::FromRawFd, path::Path, sync::atomic::Ordering,
};

use ansilo_core::err::Result;
use async_trait::async_trait;
use nix::sys::socket::{socketpair, AddressFamily, SockFlag, SockType};
use tokio::net::UnixStream;
use tokio_native_tls::native_tls::Certificate;

use crate::{
    conf::{ProxyConf, TlsConf},
    peekable::Peekable,
};

use std::{
    net::{Ipv4Addr, SocketAddrV4},
    sync::{atomic::AtomicU16, Mutex},
};

use crate::{conf::HandlerConf, handler::ConnectionHandler, stream::IOStream};

static PORT: AtomicU16 = AtomicU16::new(61000);

pub struct MockConnectionHandler {
    pub received: Mutex<Vec<Box<dyn IOStream>>>,
}

impl MockConnectionHandler {
    pub fn new() -> Self {
        Self {
            received: Mutex::new(vec![]),
        }
    }

    pub fn from_boxed(i: &Box<dyn ConnectionHandler>) -> &Self {
        i.as_any().downcast_ref().unwrap()
    }

    pub fn num_received(&self) -> usize {
        self.received.lock().unwrap().len()
    }
}

#[async_trait]
impl ConnectionHandler for MockConnectionHandler {
    async fn handle(&self, con: Box<dyn IOStream>) -> Result<()> {
        let mut received = self.received.lock().unwrap();
        received.push(con);
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub fn mock_config_no_tls() -> &'static ProxyConf {
    let port = PORT.fetch_add(1, Ordering::Relaxed);

    let conf = ProxyConf {
        addrs: vec![SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))],
        tls: None,
        handlers: HandlerConf::new(
            MockConnectionHandler::new(),
            MockConnectionHandler::new(),
            MockConnectionHandler::new(),
        ),
    };

    Box::leak(Box::new(conf))
}

pub fn mock_config_tls() -> &'static ProxyConf {
    let port = PORT.fetch_add(1, Ordering::Relaxed);

    let conf = ProxyConf {
        addrs: vec![SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))],
        tls: Some(
            TlsConf::new(
                &Path::new(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/src/mock-certs/mock.test-key.pem"
                )),
                &Path::new(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/src/mock-certs/mock.test.pem"
                )),
            )
            .unwrap(),
        ),
        handlers: HandlerConf::new(
            MockConnectionHandler::new(),
            MockConnectionHandler::new(),
            MockConnectionHandler::new(),
        ),
    };

    Box::leak(Box::new(conf))
}

pub fn mock_tls_connector() -> tokio_native_tls::TlsConnector {
    let mut builder = tokio_native_tls::native_tls::TlsConnector::builder();

    rustls_pemfile::certs(&mut io::BufReader::new(
        fs::File::open(Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/mock-certs/rootCA.pem"
        )))
        .unwrap(),
    ))
    .unwrap()
    .into_iter()
    .map(|c| Certificate::from_der(c.as_slice()).unwrap())
    .for_each(|c| {
        builder.add_root_certificate(c);
    });

    builder.build().unwrap().into()
}

pub fn create_socket_pair() -> (UnixStream, UnixStream) {
    let (fd1, fd2) = socketpair(
        AddressFamily::Unix,
        SockType::Stream,
        None,
        SockFlag::empty(),
    )
    .unwrap();

    let (s1, s2) = unsafe {
        (
            StdUnixStream::from_raw_fd(fd1),
            StdUnixStream::from_raw_fd(fd2),
        )
    };

    s1.set_nonblocking(true).unwrap();
    s2.set_nonblocking(true).unwrap();

    let (s1, s2) = (
        UnixStream::from_std(s1).unwrap(),
        UnixStream::from_std(s2).unwrap(),
    );

    (s1, s2)
}

impl From<Vec<u8>> for Peekable<io::Cursor<Vec<u8>>> {
    fn from(data: Vec<u8>) -> Self {
        Peekable::new(io::Cursor::new(data))
    }
}

// ansilo-proxy is a multi-protocol proxy that clients will connect to.
// It supports http/2 and postgres wire protocol.

pub mod conf;
pub(crate) mod connection;
pub mod handler;
pub(crate) mod peekable;
pub(crate) mod proto;
pub mod server;
pub mod stream;

#[cfg(test)]
pub(crate) mod test;

/// The protocols which are supported by the proxy
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Protocol {
    Http1,
    Http2,
    Postgres,
}

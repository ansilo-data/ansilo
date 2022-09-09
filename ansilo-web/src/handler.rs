use ansilo_core::err::{Result, Error};
use ansilo_proxy::stream::IOStream;
use tokio::sync::mpsc;

use crate::proto::HttpMode;

/// Handler for incoming requests
#[derive(Clone)]
pub struct HttpApiHandler {
    pub(crate) http1_queue: mpsc::Sender<Result<Box<dyn IOStream>>>,
    pub(crate) http2_queue: mpsc::Sender<Result<Box<dyn IOStream>>>,
}

impl HttpApiHandler {
    /// Handles an incoming request
    pub async fn serve(&self, proto: HttpMode, io: Box<dyn IOStream>) -> Result<()> {
        let tx = match proto {
            HttpMode::Http1 => &self.http1_queue,
            HttpMode::Http2 => &self.http2_queue,
        };

        tx.send(Ok(io))
            .await
            .map_err(|_| Error::msg("Request queue is full"))
    }
}

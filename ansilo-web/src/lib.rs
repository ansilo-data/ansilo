use ansilo_core::err::{Context, Result};
use ansilo_logging::{error, warn};
use ansilo_proxy::stream::IOStream;
use axum::{routing::IntoMakeService, Extension, Router};
use hyper::server::accept::from_stream;
use tokio::{
    runtime::Handle,
    sync::{
        broadcast,
        mpsc::{self},
    },
    task::JoinHandle,
};
use tokio_stream::wrappers::ReceiverStream;

mod handler;
mod proto;
mod state;
mod v1;

pub use handler::*;
pub use proto::*;
pub use state::*;

/// The main http api application
pub struct HttpApi {
    http1_srv: Option<JoinHandle<Result<()>>>,
    http2_srv: Option<JoinHandle<Result<()>>>,
    shutdown_tx: broadcast::Sender<()>,
    handler: HttpApiHandler,
    rt_handle: Handle,
}

impl HttpApi {
    /// Starts the http api server
    pub async fn start(state: HttpApiState) -> Result<Self> {
        let rt_handle = tokio::runtime::Handle::current();
        let service = Self::app(state).into_make_service();

        let (http1_queue, http1_rx) = mpsc::channel(128);
        let (http2_queue, http2_rx) = mpsc::channel(128);
        let (shutdown_tx, _) = broadcast::channel(1);

        let http1_srv = Self::server(
            http1_rx,
            HttpMode::Http1,
            service.clone(),
            shutdown_tx.subscribe(),
        );
        let http2_srv = Self::server(
            http2_rx,
            HttpMode::Http2,
            service.clone(),
            shutdown_tx.subscribe(),
        );

        Ok(Self {
            http1_srv: Some(http1_srv),
            http2_srv: Some(http2_srv),
            handler: HttpApiHandler {
                http1_queue,
                http2_queue,
            },
            shutdown_tx,
            rt_handle,
        })
    }

    fn app(state: HttpApiState) -> Router {
        Router::new()
            .nest("/v1", v1::router())
            .layer(Extension(state))
    }

    fn server(
        rx: mpsc::Receiver<Result<Box<dyn IOStream>>>,
        mode: HttpMode,
        svc: IntoMakeService<Router>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> JoinHandle<Result<()>> {
        let server = axum::Server::builder(from_stream(ReceiverStream::new(rx)))
            .http1_only(mode.is_http1())
            .http2_only(mode.is_http2())
            .serve(svc)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.recv().await;
            });

        tokio::spawn(async move {
            if let Err(err) = server.await {
                error!("Http server error: {:?}", err);
                return Err(err).context("Error");
            }

            Ok(())
        })
    }

    /// Gets the incoming request handler
    pub fn handler(&self) -> HttpApiHandler {
        self.handler.clone()
    }

    /// Terminates the http api server
    pub fn terminate(mut self) -> Result<()> {
        self.terminate_mut()
    }

    fn terminate_mut(&mut self) -> Result<()> {
        self.shutdown_tx.send(())?;
        let (http1_srv, http2_srv) = (
            self.http1_srv.take().unwrap(),
            self.http2_srv.take().unwrap(),
        );
        let _ = self
            .rt_handle
            .block_on(async move { tokio::try_join!(http1_srv, http2_srv) })?;

        Ok(())
    }
}

impl Drop for HttpApi {
    fn drop(&mut self) {
        if let Err(err) = self.terminate_mut() {
            warn!("Error while dropping http server: {:?}", err);
        }
    }
}

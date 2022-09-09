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

mod api;
mod handler;
mod healthcheck;
mod proto;
mod state;

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
            .nest("/api", api::router())
            .nest("/health", healthcheck::router())
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
        if self.http1_srv.is_none() && self.http2_srv.is_none() {
            return Ok(());
        }

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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ansilo_auth::Authenticator;
    use ansilo_core::config::NodeConfig;
    use ansilo_pg::{
        conf::PostgresConf,
        connection::PostgresConnectionPool,
        low_level::multi_pool::{
            MultiUserPostgresConnectionPool, MultiUserPostgresConnectionPoolConfig,
        },
        PostgresConnectionPools,
    };
    use hyper::{Body, Request, StatusCode};
    use tower::ServiceExt;

    use crate::{HttpApi, HttpApiState};

    fn mock_state() -> HttpApiState {
        let conf = Box::leak(Box::new(NodeConfig::default()));
        let pg = Box::leak(Box::new(PostgresConf {
            install_dir: "unused".into(),
            postgres_conf_path: None,
            data_dir: "unused".into(),
            socket_dir_path: "unused".into(),
            fdw_socket_path: "unused".into(),
            app_users: vec![],
            init_db_sql: vec![],
        }));

        HttpApiState::new(
            conf,
            PostgresConnectionPools::new(
                PostgresConnectionPool::new(pg, "unused", "unused", 0, Duration::from_secs(1))
                    .unwrap(),
                MultiUserPostgresConnectionPool::new(MultiUserPostgresConnectionPoolConfig {
                    pg,
                    users: vec![],
                    database: "unused".into(),
                    max_cons_per_user: 10,
                    connect_timeout: Duration::from_secs(1),
                })
                .unwrap(),
            ),
            Authenticator::init(&conf.auth).unwrap(),
        )
    }

    #[test]
    fn test_init_and_terminate() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut api = rt.block_on(HttpApi::start(mock_state())).unwrap();

        assert_eq!(api.http1_srv.as_ref().unwrap().is_finished(), false);
        assert_eq!(api.http2_srv.as_ref().unwrap().is_finished(), false);

        api.terminate_mut().unwrap();

        assert_eq!(api.http1_srv.is_none(), true);
        assert_eq!(api.http2_srv.is_none(), true);
    }

    #[tokio::test]
    async fn test_health_check() {
        let router = HttpApi::app(mock_state());

        let res = router
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);

        let body = hyper::body::to_bytes(res.into_body()).await.unwrap();
        assert_eq!(&body[..], b"Ok");
    }

    #[tokio::test]
    async fn test_non_existant_endpoint() {
        let router = HttpApi::app(mock_state());

        let res = router
            .oneshot(
                Request::builder()
                    .uri("/non-existant")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }
}

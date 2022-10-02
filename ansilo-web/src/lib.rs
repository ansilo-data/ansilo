use std::{env, io, sync::Arc, time::Duration};

use ansilo_core::err::{Context, Result};
use ansilo_logging::{error, warn};
use ansilo_proxy::stream::IOStream;
use axum::{
    body::Bytes,
    error_handling::HandleErrorLayer,
    http::HeaderValue,
    response::IntoResponse,
    routing::{get_service, IntoMakeService},
    Router,
};
use hyper::{header, server::accept::from_stream, StatusCode};
use tokio::{
    runtime::Handle,
    sync::{
        broadcast,
        mpsc::{self},
    },
    task::JoinHandle,
};
use tokio_stream::wrappers::ReceiverStream;

pub mod api;
mod handler;
mod middleware;
mod proto;
mod state;

pub use handler::*;
pub use proto::*;
pub use state::*;
use tower::{BoxError, ServiceBuilder};
use tower_http::{
    cors::{Any, CorsLayer},
    services::ServeDir,
    trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer},
    LatencyUnit, ServiceBuilderExt,
};

/// The main http api application
pub struct HttpApi {
    http1_srv: Option<JoinHandle<Result<()>>>,
    http2_srv: Option<JoinHandle<Result<()>>>,
    shutdown_tx: broadcast::Sender<()>,
    handler: HttpApiHandler,
    rt_handle: Handle,
}

impl HttpApi {
    /// The main api router
    fn router(state: HttpApiState) -> Router<HttpApiState> {
        let state = Arc::new(state);

        // Build our middleware stack
        let middleware = ServiceBuilder::new()
            .sensitive_request_headers(vec![header::AUTHORIZATION, header::COOKIE].into())
            .layer(
                TraceLayer::new_for_http()
                    .on_body_chunk(|chunk: &Bytes, latency: Duration, _: &tracing::Span| {
                        tracing::trace!(size_bytes = chunk.len(), latency = ?latency, "sending body chunk")
                    })
                    .make_span_with(DefaultMakeSpan::new().include_headers(true))
                    .on_response(DefaultOnResponse::new().include_headers(true).latency_unit(LatencyUnit::Micros)),
            )
            .layer(HandleErrorLayer::new(Self::handle_errors))
            .timeout(Duration::from_secs(180))
            .compression()
            .insert_response_header_if_not_present(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/octet-stream"),
            )
            .layer(if let Ok(origin) = env::var("ANSILO_CORS_ALLOWED_ORIGIN") {
                CorsLayer::new().allow_origin(origin.parse::<HeaderValue>().unwrap()).allow_headers(Any)
            } else {
                CorsLayer::new()
            });

        Router::with_state_arc(state.clone())
            .nest("/api", api::router(state.clone()))
            .fallback_service(
                get_service(ServeDir::new(Self::get_frontend_path()))
                    .handle_error(Self::handle_file_error),
            )
            .layer(middleware)
    }

    /// Starts the http api server
    pub async fn start(state: HttpApiState) -> Result<Self> {
        let rt_handle = tokio::runtime::Handle::current();
        let service = Self::router(state).into_make_service();

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

    /// Checks whether http server is running
    pub fn healthy(&self) -> bool {
        match (&self.http1_srv, &self.http2_srv) {
            (Some(http1), Some(http2)) => !http1.is_finished() && !http2.is_finished(),
            _ => false,
        }
    }

    fn get_frontend_path() -> String {
        if let Ok(path) = env::var("ANSILO_FRONTEND_PATH") {
            return path;
        }

        std::env::current_exe()
            .unwrap()
            .parent()
            .unwrap()
            .join("frontend")
            .to_string_lossy()
            .to_string()
    }

    async fn handle_errors(err: BoxError) -> impl IntoResponse {
        if err.is::<tower::timeout::error::Elapsed>() {
            (
                StatusCode::REQUEST_TIMEOUT,
                "Request took too long".to_string(),
            )
        } else {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Unhandled internal error: {}", err),
            )
        }
    }

    async fn handle_file_error(_err: io::Error) -> impl IntoResponse {
        (StatusCode::INTERNAL_SERVER_ERROR, "Something went wrong...")
    }

    fn server(
        rx: mpsc::Receiver<Result<Box<dyn IOStream>>>,
        mode: HttpMode,
        svc: IntoMakeService<Router<HttpApiState>>,
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
    use ansilo_core::{
        config::{NodeConfig, ResourceConfig},
        data::chrono::{DateTime, Utc},
    };
    use ansilo_pg::{
        conf::PostgresConf,
        connection::PostgresConnectionPool,
        handler::PostgresConnectionHandler,
        low_level::multi_pool::{
            MultiUserPostgresConnectionPool, MultiUserPostgresConnectionPoolConfig,
        },
        PostgresConnectionPools,
    };
    use ansilo_util_health::Health;
    use hyper::{Body, Request, StatusCode};
    use tower::ServiceExt;

    use crate::{HttpApi, HttpApiState, VersionInfo};

    fn mock_state() -> HttpApiState {
        let conf = Box::leak(Box::new(NodeConfig::default()));
        let pg = Box::leak(Box::new(PostgresConf {
            resources: ResourceConfig::default(),
            install_dir: "unused".into(),
            postgres_conf_path: None,
            data_dir: "unused".into(),
            socket_dir_path: "unused".into(),
            fdw_socket_path: "unused".into(),
            app_users: vec![],
            init_db_sql: vec![],
        }));

        let pools = PostgresConnectionPools::new(
            pg,
            PostgresConnectionPool::new(pg, "unused", "unused", 0, Duration::from_secs(1)).unwrap(),
            MultiUserPostgresConnectionPool::new(MultiUserPostgresConnectionPoolConfig {
                pg,
                users: vec![],
                database: "unused".into(),
                max_cons_per_user: 10,
                connect_timeout: Duration::from_secs(1),
            })
            .unwrap(),
        );
        let authenticator = Authenticator::init(&conf.auth).unwrap();

        HttpApiState::new(
            conf,
            pools.clone(),
            PostgresConnectionHandler::new(authenticator, pools),
            Health::new(),
            VersionInfo::new("test", DateTime::<Utc>::MIN_UTC),
        )
    }

    #[test]
    fn test_init_and_terminate() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut api = rt.block_on(HttpApi::start(mock_state())).unwrap();

        assert_eq!(api.http1_srv.as_ref().unwrap().is_finished(), false);
        assert_eq!(api.http2_srv.as_ref().unwrap().is_finished(), false);
        assert_eq!(api.healthy(), true);

        api.terminate_mut().unwrap();

        assert_eq!(api.http1_srv.is_none(), true);
        assert_eq!(api.http2_srv.is_none(), true);
        assert_eq!(api.healthy(), false);
    }

    #[tokio::test]
    async fn test_health_check() {
        let router = HttpApi::router(mock_state());

        let res = router
            .oneshot(
                Request::builder()
                    .uri("/api/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);

        let body = hyper::body::to_bytes(res.into_body()).await.unwrap();
        assert_eq!(&body[..], r#"{"subsystems":{}}"#.as_bytes());
    }

    #[tokio::test]
    async fn test_non_existant_endpoint() {
        let router = HttpApi::router(mock_state());

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

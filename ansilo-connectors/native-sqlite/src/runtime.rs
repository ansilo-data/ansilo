use lazy_static::lazy_static;
use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};

lazy_static! {
    static ref RUNTIME: Arc<Runtime> = {
        let runtime = Builder::new_multi_thread()
            .enable_all()
            .thread_name("ansilo-connector-native-postgres")
            .worker_threads(4)
            .build()
            .expect("Failed to build tokio runtime");

        Arc::new(runtime)
    };
}

pub(crate) fn runtime() -> Arc<Runtime> {
    Arc::clone(&RUNTIME)
}

pub fn postgres_connector_runtime() -> Arc<Runtime> {
    runtime()
}

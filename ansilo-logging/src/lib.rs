use std::sync::atomic::{AtomicBool, Ordering};

use ansilo_core::err::Result;
pub use env_logger::{init, init_from_env};
pub use log::*;

pub mod limiting;

static TEST_MODE: AtomicBool = AtomicBool::new(false);

/// Configures the logger
pub fn init_logging() -> Result<()> {
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );
    Ok(())
}

/// Logging init function for tests
pub fn init_for_tests() {
    TEST_MODE.store(true, Ordering::Relaxed);

    let res = env_logger::builder()
        .filter_module("ansilo", LevelFilter::Trace)
        .is_test(true)
        .try_init();
    if let Err(err) = res {
        eprintln!("Failed to init logging: {:?}", err);
    }
}

pub fn test_mode() -> bool {
    TEST_MODE.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use ansilo_core::config::NodeConfig;

    use crate::init_logging;

    #[test]
    fn test_init_logging() {
        let _conf = NodeConfig::default();

        let res = init_logging();

        assert!(res.is_ok());
    }
}

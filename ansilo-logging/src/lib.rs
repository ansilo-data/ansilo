use ansilo_core::err::Result;
pub use env_logger::{init, init_from_env};
pub use log::*;

/// Configures the logger for this ansilo node
pub fn init_logging() -> Result<()> {
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );
    Ok(())
}

/// Logging init function for tests
pub fn init_for_tests() {
    let res = env_logger::builder()
        .filter_module("ansilo", LevelFilter::Trace)
        .is_test(true)
        .try_init();
    if let Err(err) = res {
        eprintln!("Failed to init logging: {}", err);
    }
}

#[cfg(test)]
mod tests {
    use ansilo_core::config::NodeConfig;

    use crate::init_logging;

    #[test]
    fn test_init_logging() {
        let conf = NodeConfig::default();

        let res = init_logging();

        assert!(res.is_ok());
    }
}

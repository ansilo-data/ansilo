use ansilo_core::{config::NodeConfig, err::Result};
pub use log::*;
pub use env_logger::{init, init_from_env};

/// Configures the logger for this ansilo node
/// Currently this is a null-op by may implement different logging settings in future
pub fn init_logging(_config: &NodeConfig) -> Result<()> {
    Ok(())
}

/// Logging init function for tests
pub fn init_for_tests() {
    let res = env_logger::builder().is_test(true).try_init();
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

        let res = init_logging(&conf);
        
        assert!(res.is_ok());
    }
}

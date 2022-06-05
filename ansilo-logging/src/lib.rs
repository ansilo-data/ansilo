use ansilo_core::{config::NodeConfig, err::Result};
pub use log::*;

/// Configures the logger for this ansilo node
/// Currently this is a null-op by may implement different logging settings in future
pub fn init_logging(_config: &NodeConfig) -> Result<()> {
    Ok(())
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

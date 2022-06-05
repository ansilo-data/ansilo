use ansilo_core::err::Result;

use crate::loader::ConfigLoader;

use super::ConfigProcessor;

/// Interpolates configuration using environment variables
#[derive(Default)]
pub struct EnvConfigProcessor {}

impl ConfigProcessor for EnvConfigProcessor {
    fn display_name(&self) -> &str {
        "environment"
    }

    fn process(&self, loader: &ConfigLoader, conf: &mut serde_yaml::Value) -> Result<()> {
        todo!()
    }
}

use ansilo_core::err::Result;
use serde_yaml::Value;

use crate::loader::ConfigLoader;

use super::ConfigProcessor;

/// Interpolates configuration using environment variables
#[derive(Default)]
pub struct EnvConfigProcessor {}

impl ConfigProcessor for EnvConfigProcessor {
    fn display_name(&self) -> &str {
        "environment"
    }

    fn process(&self, loader: &ConfigLoader, conf: &mut Value) -> Result<()> {
        match conf {
            Value::Null => todo!(),
            Value::Bool(_) => todo!(),
            Value::Number(_) => todo!(),
            Value::String(_) => todo!(),
            Value::Sequence(_) => todo!(),
            Value::Mapping(_) => todo!(),
        }
    }
}

use ansilo_core::err::Result;
use serde_yaml::Value;

use crate::loader::ConfigLoader;

use super::{ConfigExprProcessor, ConfigStringExpr, ConfigExprResult};

/// Interpolates configuration using environment variables
#[derive(Default)]
pub struct EnvConfigProcessor {}

impl ConfigExprProcessor for EnvConfigProcessor {
    fn display_name(&self) -> &str {
        "environment"
    }

    fn process(&self, _loader: &ConfigLoader, expr: ConfigStringExpr) -> Result<ConfigExprResult> {
        match expr {
            ConfigStringExpr::Constant(_) => todo!(),
            ConfigStringExpr::Concat(_) => todo!(),
            ConfigStringExpr::Interpolation(_) => todo!(),
        }
    }
}

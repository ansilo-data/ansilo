use ansilo_core::err::Result;

use crate::loader::ConfigLoader;

/// A config processor applies transformations to the yaml config
/// This is used for interpolating config items from various sources
pub(crate) trait ConfigProcessor {
    /// Gets the human readable display name for the processor
    fn display_name(&self) -> &str;
    /// Applies any transformations to the config
    /// The transformations may be recursively applied using the supplied &ConfigLoader
    fn process(&self, loader: &ConfigLoader, conf: &mut serde_yaml::Value) -> Result<()>;
}

/// AST used to represent configuration expressions
#[derive(Debug, PartialEq, Clone)]
pub(crate) enum ConfigStringExpr {
    Constant(String),
    Concat(Vec<ConfigStringExpr>),
    /// Represents an interpolated value used in the configuration
    /// Format ${[part 1]:[part 2]..:[part n]}
    /// For instance, ${ENV:some_env_var}
    Interpolation(Vec<ConfigStringExpr>),
}

pub(crate) mod env;
pub(self) mod util;

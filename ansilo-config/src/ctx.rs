use std::path::PathBuf;

use crate::loader::ConfigLoader;

/// Context data when parsing configuration files
pub(crate) struct Ctx<'a> {
    /// Current configuration loader
    pub loader: &'a ConfigLoader,
    /// Path of the current config file
    pub path: Option<PathBuf>,
}

impl<'a> Ctx<'a> {
    pub(crate) fn new(loader: &'a ConfigLoader, path: Option<PathBuf>) -> Self {
        Self { loader, path }
    }

    #[cfg(test)]
    pub fn mock() -> Self {
        Self {
            loader: Box::leak(Box::new(ConfigLoader::mock())),
            path: None,
        }
    }
}

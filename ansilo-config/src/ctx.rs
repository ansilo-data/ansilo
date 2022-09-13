use std::{collections::HashMap, path::PathBuf};

use crate::loader::ConfigLoader;

/// Context data when parsing configuration files
pub(crate) struct Ctx<'a> {
    /// Current configuration loader
    pub loader: &'a ConfigLoader,
    /// Path of the current config file
    pub path: Option<PathBuf>,
    /// Configuration arguments
    pub args: HashMap<String, String>,
}

impl<'a> Ctx<'a> {
    pub(crate) fn new(
        loader: &'a ConfigLoader,
        path: Option<PathBuf>,
        args: HashMap<String, String>,
    ) -> Self {
        Self { loader, path, args }
    }

    #[cfg(test)]
    pub fn mock() -> Self {
        Self {
            loader: Box::leak(Box::new(ConfigLoader::mock())),
            path: None,
            args: HashMap::default(),
        }
    }
}

use std::{
    any::{Any, TypeId},
    collections::HashMap,
    path::PathBuf,
};

use crate::loader::ConfigLoader;

/// Context data when parsing configuration files
pub(crate) struct Ctx<'a> {
    /// Current configuration loader
    pub loader: &'a ConfigLoader,
    /// Unprocessed configuration yaml which is being parsed
    pub config: serde_yaml::Value,
    /// Path of the current config file
    pub path: Option<PathBuf>,
    /// Configuration arguments
    pub args: HashMap<String, String>,
    /// State stored by processors when parsing a configuration
    state: HashMap<TypeId, Box<dyn Any>>,
}

impl<'a> Ctx<'a> {
    pub(crate) fn new(
        loader: &'a ConfigLoader,
        config: serde_yaml::Value,
        path: Option<PathBuf>,
        args: HashMap<String, String>,
    ) -> Self {
        Self {
            loader,
            config,
            path,
            args,
            state: HashMap::new(),
        }
    }

    #[cfg(test)]
    pub fn mock() -> Self {
        Self {
            loader: Box::leak(Box::new(ConfigLoader::new())),
            config: serde_yaml::Value::Null,
            path: None,
            args: HashMap::default(),
            state: HashMap::default(),
        }
    }

    /// Returns the supplied state if it exists
    pub fn state<T: 'static>(&self) -> Option<&T> {
        self.state
            .get(&TypeId::of::<T>())
            .and_then(|s| s.downcast_ref::<T>())
    }

    /// Sets the state for the supplied type
    pub fn set_state<T: 'static>(&mut self, state: T) {
        self.state.insert(TypeId::of::<T>(), Box::new(state));
    }
}

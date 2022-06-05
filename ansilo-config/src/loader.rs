use std::{fs, path::Path, any::type_name};

use ansilo_core::{
    config::NodeConfig,
    err::{Context, Result},
};
use ansilo_logging::{debug, info};
use serde::Deserialize;
use serde_yaml::Deserializer;

use crate::processor::{ConfigProcessor, env::EnvConfigProcessor};

/// Parses and loads the configuration
pub struct ConfigLoader<'a> {
    pub file: &'a Path,
    processors: Vec<Box<dyn ConfigProcessor>>,
}

impl<'a> ConfigLoader<'a> {
    /// Initialises the configuration loader
    pub fn new(file: &'a Path) -> Self {
        Self {
            file,
            processors: Self::default_processors(),
        }
    }

    fn default_processors() -> Vec<Box<dyn ConfigProcessor>> {
        vec![
            Box::new(EnvConfigProcessor::default())
        ]
    }

    /// Loads the node configuration from disk
    pub fn load(&self) -> Result<NodeConfig> {
        info!("Loading config from path {}", self.file.display());

        let processed = self.load_yaml()?;
        debug!("Parsing into {}", type_name::<NodeConfig>());
        let config: NodeConfig = serde_yaml::from_value(processed)
            .context("Failed to parse yaml into NodeConfig")?;

        Ok(config)
    }

    /// Loads processed yaml from disk
    pub(crate) fn load_yaml(&self) -> Result<serde_yaml::Value> {
        debug!("Loading yaml from file {}", self.file.display());

        let file_data = fs::read(self.file).context(format!(
            "Failed to read config from file {}",
            self.file.display()
        ))?;

        let mut parsed_value =
            serde_yaml::Value::deserialize(Deserializer::from_slice(file_data.as_slice()))
                .context(format!(
                    "Failed to parse yaml from file {}",
                    self.file.display()
                ))?;

        for processor in self.processors.iter() {
            debug!(
                "Processing yaml using {} processor",
                processor.display_name()
            );
            processor.process(self, &mut parsed_value).context(format!(
                "Failed to process config using the {} processor",
                processor.display_name()
            ))?;
        }

        debug!("Finished processing yaml from file");
        Ok(parsed_value)
    }
}

use std::{any::type_name, fs, path::Path};

use ansilo_core::{
    config::NodeConfig,
    err::{Context, Result},
};
use ansilo_logging::{debug, info};
use serde::Deserialize;
use serde_yaml::Deserializer;

use crate::processor::{
    env::EnvConfigProcessor,
    util::{parse_expression, process_expression, process_strings, expression_to_string},
    ConfigExprProcessor, ConfigExprResult,
};

/// Parses and loads the configuration
pub struct ConfigLoader<'a> {
    pub file: &'a Path,
    processors: Vec<Box<dyn ConfigExprProcessor>>,
}

impl<'a> ConfigLoader<'a> {
    /// Initialises the configuration loader
    pub fn new(file: &'a Path) -> Self {
        Self {
            file,
            processors: Self::default_processors(),
        }
    }

    fn default_processors() -> Vec<Box<dyn ConfigExprProcessor>> {
        vec![Box::new(EnvConfigProcessor::default())]
    }

    /// Loads the node configuration from disk
    pub fn load(&self) -> Result<NodeConfig> {
        info!("Loading config from path {}", self.file.display());

        let processed = self.load_yaml()?;
        debug!("Parsing into {}", type_name::<NodeConfig>());
        let config: NodeConfig =
            serde_yaml::from_value(processed).context("Failed to parse yaml into NodeConfig")?;

        Ok(config)
    }

    /// Loads processed yaml from disk
    pub(crate) fn load_yaml(&self) -> Result<serde_yaml::Value> {
        debug!("Loading yaml from file {}", self.file.display());

        let file_data = fs::read(self.file).context(format!(
            "Failed to read config from file {}",
            self.file.display()
        ))?;

        let mut config =
            serde_yaml::Value::deserialize(Deserializer::from_slice(file_data.as_slice()))
                .context(format!(
                    "Failed to parse yaml from file {}",
                    self.file.display()
                ))?;

        fn process_config(
            loader: &ConfigLoader,
            node: serde_yaml::Value,
        ) -> Result<serde_yaml::Value> {
            process_strings(node, &|string| {
                let exp = parse_expression(string.as_str())?;

                let res = process_expression(exp, &|mut exp| {
                    for processor in loader.processors.iter() {
                        let res = processor.process(loader, exp).context(format!(
                            "Failed to process config value \"{}\" using the {} processor",
                            string,
                            processor.display_name()
                        ))?;

                        exp = match res {
                            ConfigExprResult::Expr(exp) => exp,
                            ConfigExprResult::Yaml(node) => {
                                return Ok(ConfigExprResult::Yaml(process_config(loader, node)?))
                            }
                        }
                    }

                    Ok(ConfigExprResult::Expr(exp))
                })?;

                Ok(match res {
                    ConfigExprResult::Expr(exp) => serde_yaml::Value::String(expression_to_string(exp)),
                    ConfigExprResult::Yaml(node) => node,
                })
            })
        }

        config = process_config(self, config)?;

        debug!("Finished processing yaml from file");
        Ok(config)
    }
}

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
    util::{expression_to_string, parse_expression, process_expression, process_strings},
    ConfigExprProcessor, ConfigExprResult,
};

/// Parses and loads the configuration
pub struct ConfigLoader {
    processors: Vec<Box<dyn ConfigExprProcessor>>,
}

impl ConfigLoader {
    /// Initialises the configuration loader
    pub fn new() -> Self {
        Self {
            processors: Self::default_processors(),
        }
    }

    #[cfg(test)]
    pub fn mock() -> Self {
        Self { processors: vec![] }
    }

    fn default_processors() -> Vec<Box<dyn ConfigExprProcessor>> {
        vec![Box::new(EnvConfigProcessor::default())]
    }

    /// Loads the node configuration from the supplied file
    pub fn load(&self, path: &Path) -> Result<NodeConfig> {
        info!("Loading config from path {}", path.display());

        let processed = self.load_yaml(path)?;
        debug!("Parsing into {}", type_name::<NodeConfig>());
        let config: NodeConfig =
            serde_yaml::from_value(processed).context("Failed to parse yaml into NodeConfig")?;

        Ok(config)
    }

    /// Loads processed yaml from the supplied file
    pub(crate) fn load_yaml(&self, path: &Path) -> Result<serde_yaml::Value> {
        debug!("Loading yaml from file {}", path.display());

        let file_data = fs::read(path).context(format!(
            "Failed to read config from file {}",
            path.display()
        ))?;

        self.load_data(file_data.as_slice())
    }

    /// Parses and processes the supplied yaml
    pub(crate) fn load_data(&self, data: &[u8]) -> Result<serde_yaml::Value> {
        let mut config = serde_yaml::Value::deserialize(Deserializer::from_slice(data))
            .context("Failed to parse yaml")?;

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
                    ConfigExprResult::Expr(exp) => {
                        serde_yaml::Value::String(expression_to_string(exp))
                    }
                    ConfigExprResult::Yaml(node) => node,
                })
            })
        }

        config = process_config(self, config)?;

        debug!("Finished processing yaml from file");
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use super::*;

    fn process_yaml(yaml: &str) -> Result<String> {
        let loader = ConfigLoader::new();

        let processed = loader.load_data(yaml.as_bytes());

        processed
            .and_then(|val| Ok(serde_yaml::to_string(&val)?))
            // remove prefix and trailing new line
            .map(|s| {
                s.replacen("---\n", "", 1)
                    .trim_end_matches('\n')
                    .to_string()
            })
    }

    #[test]
    fn test_config_loader_basic_yaml() {
        let input = "a: b";
        let result = process_yaml(input);

        assert_eq!(result.unwrap(), "a: b");
    }

    #[test]
    fn test_config_loader_unknown_interpolation() {
        let input = r#"a: "${unknown}""#;
        let result = process_yaml(input);

        assert_eq!(result.unwrap(), r#"a: "${unknown}""#);
    }

    #[test]
    fn test_config_loader_env_interpolation() {
        env::set_var("ANSILO_CONFIG_LOADER_TEST1", "FROM_ENV_VAR");
        let input = r#"a: "${env:ANSILO_CONFIG_LOADER_TEST1}""#;
        let result = process_yaml(input);

        assert_eq!(result.unwrap(), r#"a: FROM_ENV_VAR"#);
    }

    #[test]
    fn test_config_loader_nested_env_interpolation() {
        env::set_var("ANSILO_CONFIG_LOADER_TEST2_INNER", "ANSILO_CONFIG_LOADER_TEST2_OUTER");
        env::set_var("ANSILO_CONFIG_LOADER_TEST2_OUTER", "RESOLVED_OUTER_VALUE");
        let input = r#"a: "${env:${env:ANSILO_CONFIG_LOADER_TEST2_INNER}}""#;
        let result = process_yaml(input);

        assert_eq!(result.unwrap(), r#"a: RESOLVED_OUTER_VALUE"#);
    }
}

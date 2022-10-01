use std::{any::type_name, collections::HashMap, fs, path::Path};

use ansilo_core::err::{bail, Context, Result};
use ansilo_logging::{debug, info};
use serde::de::DeserializeOwned;

use crate::{
    ctx::Ctx,
    diagnostic::ConfigParseError,
    processor::{
        arg::ArgConfigProcessor,
        dir::DirConfigProcessor,
        embed::EmbedConfigProcessor,
        env::EnvConfigProcessor,
        fetch::FetchConfigProcessor,
        util::{expression_to_string, parse_expression, process_expression, process_strings},
        vault::VaultConfigProcessor,
        ConfigExprProcessor, ConfigExprResult,
    },
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

    fn default_processors() -> Vec<Box<dyn ConfigExprProcessor>> {
        vec![
            Box::new(DirConfigProcessor::default()),
            Box::new(EmbedConfigProcessor::default()),
            Box::new(FetchConfigProcessor::default()),
            Box::new(EnvConfigProcessor::default()),
            Box::new(ArgConfigProcessor::default()),
            Box::new(VaultConfigProcessor::default()),
        ]
    }

    /// Loads the configuration from the supplied file
    pub fn load<T: DeserializeOwned>(
        &self,
        path: &Path,
        args: HashMap<String, String>,
    ) -> Result<T> {
        let processed = self.load_as_string(path, args)?;
        debug!("Parsing into {}", type_name::<T>());
        let config: T = match serde_yaml::from_str(&processed) {
            Ok(c) => c,
            Err(e) => {
                ConfigParseError::new(processed, e.location().unwrap(), format!("{}", e)).print();
                bail!("Failed to parse configuration");
            }
        };

        Ok(config)
    }

    /// Loads the configuration from the supplied file and returns the processed
    /// yaml as a string. This evaluates any expressions in the config file.
    pub fn load_as_string(&self, path: &Path, args: HashMap<String, String>) -> Result<String> {
        let path = path
            .canonicalize()
            .with_context(|| format!("Failed to get real path of {}", path.display()))?;
        info!("Loading config from path {}", path.display());

        let processed = self.load_yaml(path.as_path(), args)?;
        let processed =
            serde_yaml::to_string(&processed).context("Failed to serialised processed config")?;

        Ok(processed)
    }

    /// Loads processed yaml from the supplied file
    pub(crate) fn load_yaml(
        &self,
        path: &Path,
        args: HashMap<String, String>,
    ) -> Result<serde_yaml::Value> {
        debug!("Loading yaml from file {}", path.display());

        let file_data = fs::read(path).context(format!(
            "Failed to read config from file {}",
            path.display()
        ))?;

        let config: serde_yaml::Value =
            serde_yaml::from_slice(file_data.as_slice()).context("Failed to parse yaml")?;
        let mut ctx = Ctx::new(self, config.clone(), Some(path.to_path_buf()), args);

        self.process_config(&mut ctx, config)
    }

    /// Loads a subsection of config
    pub(crate) fn load_part<T: DeserializeOwned>(
        &self,
        ctx: &mut Ctx,
        part: serde_yaml::Value,
    ) -> Result<T> {
        debug!("Loading config part for {}", type_name::<T>());
        let processed = self.process_config(ctx, part)?;

        debug!("Parsing into {}", type_name::<T>());
        let config: T = serde_yaml::from_value(processed)
            .with_context(|| format!("Failed to parse yaml into {}", type_name::<T>()))?;

        Ok(config)
    }

    /// Parses and processes the supplied yaml
    pub(crate) fn process_config(
        &self,
        ctx: &mut Ctx,
        config: serde_yaml::Value,
    ) -> Result<serde_yaml::Value> {
        fn process_config(ctx: &mut Ctx, node: serde_yaml::Value) -> Result<serde_yaml::Value> {
            process_strings(node, &mut |string| {
                let exp = parse_expression(string.as_str())?;

                let res = process_expression(exp, &mut |mut exp| {
                    for processor in ctx.loader.processors.iter() {
                        let res = processor.process(ctx, exp).context(format!(
                            "Failed to process config value \"{}\" using the {} processor",
                            string,
                            processor.display_name()
                        ))?;

                        exp = match res {
                            ConfigExprResult::Expr(exp) => exp,
                            ConfigExprResult::Yaml(node) => {
                                return Ok(ConfigExprResult::Yaml(process_config(ctx, node)?))
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

        let config = process_config(ctx, config)?;

        debug!("Finished processing yaml from file");
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::path::PathBuf;

    use super::*;

    fn process_yaml(
        yaml: &str,
        path: Option<PathBuf>,
        args: Option<HashMap<String, String>>,
    ) -> Result<String> {
        let loader = ConfigLoader::new();

        let yaml: serde_yaml::Value = serde_yaml::from_str(yaml).unwrap();
        let mut ctx = Ctx::new(&loader, yaml.clone(), path, args.unwrap_or_default());

        let processed = loader.process_config(&mut ctx, yaml);

        processed
            .and_then(|val| Ok(serde_yaml::to_string(&val)?))
            // remove trailing new line
            .map(|s| s.trim_end_matches('\n').to_string())
    }

    #[test]
    fn test_config_loader_basic_yaml() {
        let input = "a: b";
        let result = process_yaml(input, None, None);

        assert_eq!(result.unwrap(), "a: b");
    }

    #[test]
    fn test_config_loader_unknown_interpolation() {
        let input = r#"a: ${unknown}"#;
        let result = process_yaml(input, None, None);

        assert_eq!(result.unwrap(), r#"a: ${unknown}"#);
    }

    #[test]
    fn test_config_loader_env_interpolation() {
        env::set_var("ANSILO_CONFIG_LOADER_TEST1", "FROM_ENV_VAR");
        let input = r#"a: "${env:ANSILO_CONFIG_LOADER_TEST1}""#;
        let result = process_yaml(input, None, None);

        assert_eq!(result.unwrap(), r#"a: FROM_ENV_VAR"#);
    }

    #[test]
    fn test_config_loader_nested_env_interpolation() {
        env::set_var(
            "ANSILO_CONFIG_LOADER_TEST2_INNER",
            "ANSILO_CONFIG_LOADER_TEST2_OUTER",
        );
        env::set_var("ANSILO_CONFIG_LOADER_TEST2_OUTER", "RESOLVED_OUTER_VALUE");
        let input = r#"a: "${env:${env:ANSILO_CONFIG_LOADER_TEST2_INNER}}""#;
        let result = process_yaml(input, None, None);

        assert_eq!(result.unwrap(), r#"a: RESOLVED_OUTER_VALUE"#);
    }

    #[test]
    fn test_config_loader_dir_interpolation() {
        let input = r#"a: "${dir}/bar/baz""#;
        let result = process_yaml(input, Some("/foo/config.yml".into()), None);

        assert_eq!(result.unwrap(), r#"a: /foo/bar/baz"#);
    }

    #[test]
    fn test_config_loader_arg_interpolation() {
        let input = r#"a: "${arg:TEST_ARG} bar""#;
        let result = process_yaml(
            input,
            Some("/foo/config.yml".into()),
            Some(
                [("TEST_ARG".to_string(), "foo".to_string())]
                    .into_iter()
                    .collect(),
            ),
        );

        assert_eq!(result.unwrap(), r#"a: foo bar"#);
    }
}

use anyhow::{Result, Context};
use serde::Deserialize;
use serde_yaml::{Deserializer, Value};

/// Parses the supplied string as a config value
pub fn parse_config<'a>(conf_str: impl Into<&'a str>) -> Result<Value> {
    Value::deserialize(Deserializer::from_str(conf_str.into()))
        .context("Failed to parse configuration yaml")
}

#[cfg(test)]
mod tests {
    use serde_yaml::Mapping;

    use super::*;

    #[test]
    fn test_parse_config() {
        let parsed = parse_config("a: test").unwrap();

        assert_eq!(parsed, Value::Mapping({
            let mut map = Mapping::new();
            map.insert(Value::String("a".to_string()), Value::String("test".to_string()));
            map
        }));
    }

    #[test]
    fn test_parse_config_invalid() {
        assert!(parse_config("@@@").is_err());
    }
}

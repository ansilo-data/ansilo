use std::{
    io::Write,
    process::{self, Stdio},
};

use ansilo_core::{
    auth::CustomAuthContext,
    config::{CustomAuthProviderConfig, CustomUserConfig},
    err::{bail, ensure, Context, Result},
};
use serde::{Deserialize, Serialize};

pub struct CustomAuthProvider {
    conf: &'static CustomAuthProviderConfig,
}

impl CustomAuthProvider {
    pub fn new(conf: &'static CustomAuthProviderConfig) -> Result<Self> {
        Ok(Self { conf })
    }

    /// Authenticates the supplied password/secret
    pub fn authenticate(
        &self,
        user: &CustomUserConfig,
        username: &str,
        password: &str,
    ) -> Result<CustomAuthContext> {
        let user_config: serde_json::Value = match user.custom.as_ref() {
            Some(conf) => serde_yaml::from_value(conf.clone()).with_context(|| {
                format!("Failed to convert custom auth config for user '{username}' to json")
            })?,
            None => serde_json::Value::Null,
        };

        let input = CustomAuthInput {
            username: username.into(),
            password: password.into(),
            user_config,
        };

        let input = serde_json::to_vec(&input).context("Failed to serialise input to json")?;

        // Start the child proc
        let mut proc = process::Command::new("/bin/sh")
            .args(["-c", &self.conf.shell])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .with_context(|| {
                format!(
                    "Failed to run custom auth program '/bin/sh -c \"{}\"'",
                    self.conf.shell
                )
            })?;

        // Write input json to stdin
        let mut stdin = proc.stdin.take().unwrap();
        stdin
            .write_all(input.as_slice())
            .context("Failed to write to stdin")?;
        // Ensure we drop the stdin to avoid the child blocking
        drop(stdin);

        // Wait for the child to complete
        let output = proc
            .wait_with_output()
            .context("Failed to get output from child")?;

        ensure!(
            output.status.success(),
            "Custom auth process exited with non-zero code: {:?}, {}",
            output.status,
            String::from_utf8_lossy(output.stdout.as_slice())
        );

        // Read the result from stdout
        let output: CustomAuthResult = serde_json::from_slice(output.stdout.as_slice())
            .context("Failed to parse output from custom auth program as JSON")?;

        match output {
            CustomAuthResult::Success(res) => Ok(CustomAuthContext {
                data: res.context.unwrap_or(serde_json::Value::Null),
            }),
            CustomAuthResult::Failure(res) => bail!(res.message.unwrap_or("unknown error".into())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct CustomAuthInput {
    username: String,
    password: String,
    user_config: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "result")]
enum CustomAuthResult {
    #[serde(rename = "success")]
    Success(CustomAuthSuccess),
    #[serde(rename = "failure")]
    Failure(CustomAuthFailure),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct CustomAuthSuccess {
    context: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct CustomAuthFailure {
    message: Option<String>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use serial_test::serial;

    use super::*;

    fn mock_provider(script: &str) -> CustomAuthProvider {
        let conf = Box::leak(Box::new(CustomAuthProviderConfig {
            shell: script.into(),
        }));
        CustomAuthProvider::new(conf).unwrap()
    }

    fn mock_user_conf(conf: Option<&str>) -> CustomUserConfig {
        let conf = conf.map(|yaml| serde_yaml::from_str(yaml).unwrap());
        CustomUserConfig { custom: conf }
    }

    #[test]
    #[serial]
    fn test_custom_auth_success() {
        let provider = mock_provider(r#"echo '{"result": "success", "context": {"foo": "bar"}}'"#);

        let user_conf = mock_user_conf(None);

        let res = provider.authenticate(&user_conf, "user", "pass").unwrap();

        assert_eq!(
            res,
            CustomAuthContext {
                data: json!({"foo": "bar"})
            }
        )
    }

    #[test]
    #[serial]
    fn test_custom_auth_failure() {
        let provider =
            mock_provider(r#"echo '{"result": "failure", "message": "failed to auth"}'"#);

        let user_conf = mock_user_conf(None);

        let res = provider
            .authenticate(&user_conf, "user", "pass")
            .unwrap_err();

        assert_eq!(res.to_string(), "failed to auth")
    }

    #[test]
    #[serial]
    fn test_custom_auth_stdin_json() {
        let provider = mock_provider(
            r#"
                INPUT=$(cat /dev/stdin)
                USER=$(echo $INPUT | jq -r ".username");
                PASS=$(echo $INPUT | jq -r ".password");
                CONF=$(echo $INPUT | jq -r ".user_config");
                if [ "$USER" != "app" ];
                then
                    echo '{"result": "failure", "message": "incorrect username"}'
                    exit 0
                fi
                if [ "$PASS" != "password1" ];
                then
                    echo '{"result": "failure", "message": "incorrect password"}'
                    exit 0
                fi

                echo "{\"result\": \"success\", \"context\": $CONF}"
            "#,
        );

        let user_conf = mock_user_conf(Some("abc: def"));

        let res = provider
            .authenticate(&user_conf, "invalid", "pass")
            .unwrap_err();

        assert_eq!(res.to_string(), "incorrect username");

        let res = provider
            .authenticate(&user_conf, "app", "invalid")
            .unwrap_err();

        assert_eq!(res.to_string(), "incorrect password");

        let res = provider
            .authenticate(&user_conf, "app", "password1")
            .unwrap();

        assert_eq!(
            res,
            CustomAuthContext {
                data: json!({"abc": "def"})
            }
        );
    }

    #[test]
    #[serial]
    fn test_custom_auth_non_zero_exit_code() {
        let provider = mock_provider(r#"exit 1"#);

        let user_conf = mock_user_conf(None);

        let res = provider
            .authenticate(&user_conf, "user", "pass")
            .unwrap_err();

        dbg!(res.to_string());
        assert!(res.to_string().contains("non-zero code"))
    }

    #[test]
    #[serial]
    fn test_custom_auth_invalid_json_output() {
        let provider = mock_provider(r#"echo 'invalid json'"#);

        let user_conf = mock_user_conf(None);

        let res = provider
            .authenticate(&user_conf, "user", "pass")
            .unwrap_err();

        dbg!(res.to_string());
        assert!(res.to_string().contains("Failed to parse output"))
    }
}

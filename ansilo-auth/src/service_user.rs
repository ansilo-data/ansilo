use std::process::{self, Stdio};

use ansilo_core::err::{ensure, Context, Result};
use ansilo_logging::debug;
use serde::{Deserialize, Serialize};

use crate::Authenticator;

/// Service user credentials
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(Debug))]
pub struct ServiceUserCredentials {
    #[serde(default)]
    pub username: String,
    pub password: String,
}

impl Authenticator {
    /// Retrieves credentials for the supplied service user
    pub fn get_service_user_creds(&self, service_user_id: &str) -> Result<ServiceUserCredentials> {
        let conf = self
            .conf
            .service_users
            .iter()
            .find(|i| i.id() == service_user_id)
            .with_context(|| format!("No service user with id '{service_user_id}'"))?;

        debug!("Authenticating as service user {service_user_id}");

        // Start the child proc
        let proc = process::Command::new("/bin/sh")
            .args(["-c", &conf.shell])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .with_context(|| {
                format!(
                    "Failed to run service user program '/bin/sh -c \"{}\"'",
                    conf.shell
                )
            })?;

        // Wait for the child to complete
        let output = proc
            .wait_with_output()
            .context("Failed to get output from child")?;

        ensure!(
            output.status.success(),
            "Service user process exited with non-zero code: {:?}, {}",
            output.status,
            String::from_utf8_lossy(output.stdout.as_slice())
        );

        // Read the result from stdout
        let mut output: ServiceUserCredentials =
            serde_json::from_slice(output.stdout.as_slice())
                .context("Failed to parse output from service user program as JSON")?;

        if output.username.is_empty() {
            output.username = conf.username.clone();
        }

        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use ansilo_core::config::{AuthConfig, ServiceUserConfig};

    use super::*;

    #[test]
    fn test_authenticate_as_service_user_success() {
        let conf = Box::leak(Box::new(AuthConfig {
            providers: vec![],
            users: vec![],
            service_users: vec![ServiceUserConfig::new(
                "svc_user".into(),
                "svc_user".into(),
                None,
                r#"echo '{"password": "some_secret_pass"}'"#.into(),
            )],
        }));
        let authenticator = Authenticator::init(conf).unwrap();

        let res = authenticator.get_service_user_creds("svc_user").unwrap();

        assert_eq!(
            res,
            ServiceUserCredentials {
                username: "svc_user".into(),
                password: "some_secret_pass".into()
            }
        )
    }

    #[test]
    fn test_authenticate_as_service_user_with_error_proc() {
        let conf = Box::leak(Box::new(AuthConfig {
            providers: vec![],
            users: vec![],
            service_users: vec![ServiceUserConfig::new(
                "svc_user".into(),
                "svc_user".into(),
                None,
                r#"exit 1"#.into(),
            )],
        }));
        let authenticator = Authenticator::init(conf).unwrap();

        let res = authenticator.get_service_user_creds("svc_user");

        assert!(res.is_err());
    }

    #[test]
    fn test_authenticate_as_service_user_invalid_id() {
        let conf = Box::leak(Box::new(AuthConfig {
            providers: vec![],
            users: vec![],
            service_users: vec![],
        }));
        let authenticator = Authenticator::init(conf).unwrap();

        let res = authenticator.get_service_user_creds("invalid");

        assert!(res.is_err());
    }
}

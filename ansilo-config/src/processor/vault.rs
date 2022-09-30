use std::{collections::HashMap, time::Duration};

use ansilo_core::{
    config::{VaultAuthMethod, VaultConfig},
    err::{bail, ensure, Context, Result},
};
use ansilo_logging::{debug, info, trace};
use vaultrs::client::{Client, VaultClient, VaultClientSettingsBuilder};

use crate::{ctx::Ctx, processor::util::expression_to_string};

use super::{
    util::match_interpolation, ConfigExprProcessor, ConfigExprResult, ConfigStringExpr as X,
};

/// Interpolates configuration using secrets retrieved from HashiCorp Vault
#[derive(Default)]
pub struct VaultConfigProcessor {}

struct VaultProcessorState {
    client: VaultClient,
    rt: tokio::runtime::Runtime,
}

impl ConfigExprProcessor for VaultConfigProcessor {
    fn display_name(&self) -> &str {
        "vault"
    }

    fn process(&self, ctx: &mut Ctx, expr: X) -> Result<ConfigExprResult> {
        Ok(match match_interpolation(&expr, &["vault"]) {
            Some(p) => {
                ensure!(
                    p.len() == 4,
                    "${{vault:...}} must have three arguments: mount, path and key"
                );

                let state = self
                    .authenticate(ctx)
                    .context("Failed to authenticate with Vault")?;

                let mount = &p[1];
                let path = &p[2].trim_start_matches('/');
                let key = &p[3];

                trace!("Retrieving secret from vault {path} (key '{key}') (mount '{mount}')");
                let secret = state
                    .rt
                    .block_on(vaultrs::kv2::read::<HashMap<String, String>>(
                        &state.client,
                        mount,
                        path,
                    ))
                    .context("Failed to retrieve vault secret")?;
                trace!("Retrieved secret succesfully");

                let output = match secret.get(key) {
                    Some(s) => s.clone(),
                    None => bail!("Vault secret '{path}' does not contain key '{key}'"),
                };

                trace!(
                    "Replaced configuration expression '{}' with '{}'",
                    expression_to_string(&expr),
                    output
                );

                ConfigExprResult::Expr(X::Constant(output))
            }
            _ => ConfigExprResult::Expr(expr),
        })
    }
}

impl VaultConfigProcessor {
    fn authenticate<'a>(&self, ctx: &'a mut Ctx) -> Result<&'a VaultProcessorState> {
        if ctx.state::<VaultProcessorState>().is_none() {
            // First load the vault configuration
            info!("Initialising vault client");

            let config = ctx
                .config
                .as_mapping()
                .and_then(|m| m.get("vault"))
                .context("Found ${vault:...} expression but 'vault:' key is not defined")?;

            let config: VaultConfig = ctx
                .loader
                .load_part(ctx, config.clone())
                .context("Failed to load vault config")?;

            trace!("Vault configuration: {:?}", config);

            let settings = VaultClientSettingsBuilder::default()
                .address(config.address)
                .namespace(config.namespace)
                .timeout(config.timeout_secs.map(Duration::from_secs))
                .verify(config.verify.unwrap_or(true))
                .version(config.version.unwrap_or(1))
                .build()
                .context("Failed to configure vault client")?;

            // Initialise the vault client
            let mut client =
                VaultClient::new(settings).context("Failed to initialise vault client")?;

            // Initialise a tokio runtime
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .thread_name("ansilo-tokio-config")
                .build()
                .context("Failed to initialise tokio runtime")?;

            // Authenticate with vault
            let token = match config.auth {
                VaultAuthMethod::Token(t) => t.token,
                VaultAuthMethod::AppRole(a) => {
                    debug!("Authenticating with Vault using approle method");
                    rt.block_on(vaultrs::auth::approle::login(
                        &client,
                        &a.mount,
                        &a.role_id,
                        &a.secret_id,
                    ))
                    .context("Failed to authenticate with Vault")?
                    .client_token
                }
                VaultAuthMethod::Kubernetes(a) => {
                    debug!("Authenticating with Vault using kubernetes method");
                    rt.block_on(vaultrs::auth::kubernetes::login(
                        &client, &a.mount, &a.role, &a.jwt,
                    ))
                    .context("Failed to authenticate with Vault")?
                    .client_token
                }
                VaultAuthMethod::UsernamePassword(a) => {
                    debug!("Authenticating with Vault using userpass method");
                    rt.block_on(vaultrs::auth::userpass::login(
                        &client,
                        &a.mount,
                        &a.username,
                        &a.password,
                    ))
                    .context("Failed to authenticate with Vault")?
                    .client_token
                }
            };

            debug!("Vault token retrieved");
            client.set_token(&token);

            ctx.set_state(VaultProcessorState { rt, client });
        }

        Ok(ctx.state::<VaultProcessorState>().unwrap())
    }
}

#[cfg(test)]
mod tests {
    use ansilo_core::config::{
        VaultAppRoleAuth, VaultKubernetesAuth, VaultTokenAuth, VaultUserPasswordAuth,
    };
    use httpmock::prelude::*;

    use crate::processor::util::parse_expression;

    use super::*;

    fn mock_ctx(vault_conf: VaultConfig) -> Ctx<'static> {
        let mut ctx = Ctx::mock();

        let vault_conf = serde_yaml::to_value(vault_conf).unwrap();
        ctx.config = serde_yaml::Value::Mapping(serde_yaml::Mapping::from_iter([(
            "vault".into(),
            vault_conf,
        )]));

        ctx
    }

    fn mock_auth_response(then: httpmock::Then, token: &str) {
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!(
                {
                    "request_id": "99e030dd-723c-ef19-641a-da90a272e4e7",
                    "lease_id": "",
                    "renewable": false,
                    "lease_duration": 0,
                    "auth": {
                        "client_token": token,
                        "accessor": "",
                        "policies": [],
                        "token_policies": [],
                        "lease_duration": 123,
                        "renewable": false,
                        "entity_id": "",
                        "token_type": "",
                        "orphan": false,
                    }
                }
            ));
    }

    fn mock_secret_response(then: httpmock::Then, secret_json: serde_json::Value) {
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({
                    "request_id": "99e030dd-723c-ef19-641a-da90a272e4e7",
                    "lease_id": "",
                    "renewable": false,
                    "lease_duration": 0,
                    "data": {
                        "data": secret_json,
                        "metadata": {
                            "created_time": "2018-06-25T15:41:06.145555048Z",
                            "deletion_time": "",
                            "destroyed": false,
                            "version": 3
                        }
                    }
                }
            ));
    }

    #[test]
    fn test_vault_config_processor_error_on_no_config() {
        ansilo_logging::init_for_tests();

        let mut ctx = Ctx::mock();
        let processor = VaultConfigProcessor::default();

        let input = parse_expression("${vault:mount:path:key}").unwrap();
        let result = processor.process(&mut ctx, input.clone()).unwrap_err();

        dbg!(format!("{:?}", result));
        assert!(format!("{:?}", result).contains("'vault:' key is not defined"));
    }

    #[test]
    fn test_vault_config_processor_auth_token() {
        ansilo_logging::init_for_tests();

        let server = MockServer::start();
        let processor = VaultConfigProcessor::default();

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/mnt/data/secret/path")
                .header("x-vault-token", "tok");
            mock_secret_response(then, serde_json::json!({"key": "mysupersecret"}));
        });

        let mut ctx = mock_ctx(VaultConfig {
            address: format!("http://{}", server.address()),
            version: None,
            namespace: None,
            verify: None,
            timeout_secs: None,
            auth: VaultAuthMethod::Token(VaultTokenAuth {
                token: "tok".into(),
            }),
        });

        let input = parse_expression("${vault:mnt:/secret/path:key}").unwrap();
        let result = processor.process(&mut ctx, input.clone()).unwrap();

        mock.assert();

        assert_eq!(
            result,
            ConfigExprResult::Expr(X::Constant("mysupersecret".into()))
        );
    }

    #[test]
    fn test_vault_config_processor_key_does_not_exist() {
        ansilo_logging::init_for_tests();

        let server = MockServer::start();
        let processor = VaultConfigProcessor::default();

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/mnt/data/secret/path")
                .header("x-vault-token", "tok");
            mock_secret_response(then, serde_json::json!({}));
        });

        let mut ctx = mock_ctx(VaultConfig {
            address: format!("http://{}", server.address()),
            version: None,
            namespace: None,
            verify: None,
            timeout_secs: None,
            auth: VaultAuthMethod::Token(VaultTokenAuth {
                token: "tok".into(),
            }),
        });

        let input = parse_expression("${vault:mnt:/secret/path:key}").unwrap();
        let result = processor.process(&mut ctx, input.clone()).unwrap_err();

        mock.assert();

        dbg!(format!("{:?}", result));
        assert!(format!("{:?}", result).contains("does not contain key 'key'"));
    }

    #[test]
    fn test_vault_config_processor_auth_app_role() {
        ansilo_logging::init_for_tests();

        let server = MockServer::start();
        let processor = VaultConfigProcessor::default();

        let auth_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/auth/mnt/login")
                .json_body(serde_json::json!({
                    "role_id": "role-id",
                    "secret_id": "secret-id",
                }));
            mock_auth_response(then, "authtok");
        });

        let secret_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/mnt/data/secret/path")
                .header("x-vault-token", "authtok");
            mock_secret_response(then, serde_json::json!({"key": "mysupersecret"}));
        });

        let mut ctx = mock_ctx(VaultConfig {
            address: format!("http://{}", server.address()),
            version: None,
            namespace: None,
            verify: None,
            timeout_secs: None,
            auth: VaultAuthMethod::AppRole(VaultAppRoleAuth {
                mount: "mnt".into(),
                role_id: "role-id".into(),
                secret_id: "secret-id".into(),
            }),
        });

        let input = parse_expression("${vault:mnt:/secret/path:key}").unwrap();
        let result = processor.process(&mut ctx, input.clone()).unwrap();

        auth_mock.assert();
        secret_mock.assert();

        assert_eq!(
            result,
            ConfigExprResult::Expr(X::Constant("mysupersecret".into()))
        );
    }

    #[test]
    fn test_vault_config_processor_auth_userpass() {
        ansilo_logging::init_for_tests();

        let server = MockServer::start();
        let processor = VaultConfigProcessor::default();

        let auth_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/auth/mnt/login/user")
                .json_body(serde_json::json!({
                    "password": "pass",
                }));
            mock_auth_response(then, "authtok");
        });

        let secret_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/mnt/data/secret/path")
                .header("x-vault-token", "authtok");
            mock_secret_response(then, serde_json::json!({"key": "mysupersecret"}));
        });

        let mut ctx = mock_ctx(VaultConfig {
            address: format!("http://{}", server.address()),
            version: None,
            namespace: None,
            verify: None,
            timeout_secs: None,
            auth: VaultAuthMethod::UsernamePassword(VaultUserPasswordAuth {
                mount: "mnt".into(),
                username: "user".into(),
                password: "pass".into(),
            }),
        });

        let input = parse_expression("${vault:mnt:/secret/path:key}").unwrap();
        let result = processor.process(&mut ctx, input.clone()).unwrap();

        auth_mock.assert();
        secret_mock.assert();

        assert_eq!(
            result,
            ConfigExprResult::Expr(X::Constant("mysupersecret".into()))
        );
    }

    #[test]
    fn test_vault_config_processor_auth_kubernetes() {
        ansilo_logging::init_for_tests();

        let server = MockServer::start();
        let processor = VaultConfigProcessor::default();

        let auth_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/auth/mnt/login")
                .json_body(serde_json::json!({
                    "role": "rol",
                    "jwt": "my.jwt",
                }));
            mock_auth_response(then, "authtok");
        });

        let secret_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/mnt/data/secret/path")
                .header("x-vault-token", "authtok");
            mock_secret_response(then, serde_json::json!({"key": "mysupersecret"}));
        });

        let mut ctx = mock_ctx(VaultConfig {
            address: format!("http://{}", server.address()),
            version: None,
            namespace: None,
            verify: None,
            timeout_secs: None,
            auth: VaultAuthMethod::Kubernetes(VaultKubernetesAuth {
                mount: "mnt".into(),
                role: "rol".into(),
                jwt: "my.jwt".into(),
            }),
        });

        let input = parse_expression("${vault:mnt:/secret/path:key}").unwrap();
        let result = processor.process(&mut ctx, input.clone()).unwrap();

        auth_mock.assert();
        secret_mock.assert();

        assert_eq!(
            result,
            ConfigExprResult::Expr(X::Constant("mysupersecret".into()))
        );
    }

    #[test]
    fn test_vault_config_processor_multiple_secrets_auth_once() {
        ansilo_logging::init_for_tests();

        let server = MockServer::start();
        let processor = VaultConfigProcessor::default();

        let auth_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/auth/mnt/login")
                .json_body(serde_json::json!({
                    "role": "rol",
                    "jwt": "my.jwt",
                }));
            mock_auth_response(then, "authtok");
        });

        let secret_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/mnt/data/secret/path")
                .header("x-vault-token", "authtok");
            mock_secret_response(then, serde_json::json!({"key": "mysupersecret"}));
        });

        let mut ctx = mock_ctx(VaultConfig {
            address: format!("http://{}", server.address()),
            version: None,
            namespace: None,
            verify: None,
            timeout_secs: None,
            auth: VaultAuthMethod::Kubernetes(VaultKubernetesAuth {
                mount: "mnt".into(),
                role: "rol".into(),
                jwt: "my.jwt".into(),
            }),
        });

        for _ in 1..=3 {
            let input = parse_expression("${vault:mnt:/secret/path:key}").unwrap();
            let _ = processor.process(&mut ctx, input.clone()).unwrap();
        }

        auth_mock.assert_hits(1);
        secret_mock.assert_hits(3);
    }
}

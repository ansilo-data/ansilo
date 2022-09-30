use ansilo_core::{
    config::{AuthProviderConfig, AuthProviderTypeConfig, NodeConfig},
    err::Result,
    web::auth::{AuthMethod, AuthMethodType, AuthMethods},
};
use ansilo_logging::error;
use axum::{extract::State, Json};
use hyper::StatusCode;

use crate::HttpApiState;

/// Retrieves authentication methods for this node.
/// This is a public endpoint.
pub(super) async fn handler(
    State(state): State<HttpApiState>,
) -> Result<Json<AuthMethods>, (StatusCode, &'static str)> {
    let mut methods = vec![];

    // Map all token-based auth providers from config to login methods
    methods.append(
        &mut state
            .conf()
            .auth
            .providers
            .iter()
            .filter_map(|p| to_auth_method(state.conf(), p).transpose())
            .collect::<Result<Vec<_>>>()
            .map_err(|e| {
                error!("{:?}", e);
                (StatusCode::INTERNAL_SERVER_ERROR, "Internal error")
            })?,
    );

    // If there is a password user, add that as a login option
    if state
        .conf()
        .auth
        .users
        .iter()
        .any(|u| u.r#type.as_password().is_some())
    {
        methods.push(AuthMethod {
            id: "password".into(),
            name: None,
            usernames: None,
            r#type: AuthMethodType::UsernamePassword,
        })
    }

    Ok(Json(AuthMethods { methods }))
}

fn to_auth_method(conf: &NodeConfig, provider: &AuthProviderConfig) -> Result<Option<AuthMethod>> {
    // For token based methods we are happy to expose the usernames
    // as the authentication as when using SSO, we wouldn't expect
    // users to also provide a username.
    let usernames = conf
        .auth
        .users
        .iter()
        .filter(|i| i.provider.as_ref() == Some(&provider.id))
        .map(|i| i.username.clone())
        .collect();

    Ok(match &provider.r#type {
        AuthProviderTypeConfig::Jwt(c) => Some(AuthMethod {
            id: provider.id.clone(),
            name: None,
            usernames: Some(usernames),
            r#type: AuthMethodType::Jwt(c.login.clone()),
        }),
        AuthProviderTypeConfig::Saml(c) => Some(AuthMethod {
            id: provider.id.clone(),
            name: None,
            usernames: Some(usernames),
            r#type: AuthMethodType::Saml(c.login.clone()),
        }),
        _ => return Ok(None),
    })
}

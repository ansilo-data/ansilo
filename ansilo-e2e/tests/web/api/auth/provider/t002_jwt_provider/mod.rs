use ansilo_core::{
    config::{JwtLoginConfig, JwtOauth2Config},
    web::auth::{AuthMethod, AuthMethodType, AuthMethods},
};
use ansilo_e2e::{current_dir, web::url};
use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
fn test() {
    ansilo_logging::init_for_tests();
    let (instance, _port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    let res = reqwest::blocking::get(url(&instance, "/api/v1/auth/provider"))
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<AuthMethods>()
        .unwrap();

    assert_eq!(
        res.methods,
        vec![
            AuthMethod {
                id: "jwt".into(),
                name: None,
                usernames: Some(vec!["token".into()]),
                r#type: AuthMethodType::Jwt(Some(JwtLoginConfig::Oauth2(JwtOauth2Config {
                    authorize_endpoint: "https://authorize.endpoint".into(),
                    params: [("client_id".into(), "abc123".into())]
                        .into_iter()
                        .collect()
                })))
            },
            AuthMethod {
                id: "password".into(),
                name: None,
                usernames: None,
                r#type: AuthMethodType::UsernamePassword
            }
        ]
    );
}

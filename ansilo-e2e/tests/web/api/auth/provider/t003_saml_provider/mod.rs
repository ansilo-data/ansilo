use ansilo_core::{
    config::SamlLoginConfig,
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
                id: "saml".into(),
                name: None,
                usernames: Some(vec!["token".into()]),
                r#type: AuthMethodType::Saml(Some(SamlLoginConfig {
                    authorize_endpoint: "https://authorize.endpoint".into(),
                    entity_id: "test".into()
                }))
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

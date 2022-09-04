use std::{
    env,
    time::{SystemTime, UNIX_EPOCH},
};

use ansilo_core::auth::{AuthContext, JwtAuthContext, ProviderAuthContext};
use ansilo_e2e::current_dir;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use pretty_assertions::assert_eq;
use serde_json::{json, Value};
use serial_test::serial;

fn valid_exp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 1800
}

fn make_valid_token(claims: Value) -> String {
    encode(
        &Header::new(Algorithm::RS512),
        &claims,
        &EncodingKey::from_rsa_pem(include_bytes!("keys/private.key")).unwrap(),
    )
    .unwrap()
}

#[test]
#[serial]
fn test_invalid_jwt() {
    ansilo_logging::init_for_tests();
    let (_instance, port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    assert_eq!(
        ansilo_e2e::util::main::connect_opts("token_read", "not a token", port, |_| ())
            .err()
            .unwrap()
            .to_string(),
        "db error: ERROR: Failed to decode JWT header"
    );
}

#[test]
#[serial]
fn test_invalid_jwt_invalid_signature() {
    ansilo_logging::init_for_tests();
    let (_instance, port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    let token = encode(
        &Header::new(Algorithm::RS512),
        &json!({"scope": "read", "exp": valid_exp()}),
        &EncodingKey::from_rsa_pem(include_bytes!("keys/foreign-private.key")).unwrap(),
    )
    .unwrap();

    assert_eq!(
        ansilo_e2e::util::main::connect_opts("token_read", &token, port, |_| ())
            .err()
            .unwrap()
            .to_string(),
        "db error: ERROR: Failed to authenticate JWT"
    );
}

#[test]
#[serial]
fn test_expired_token() {
    ansilo_logging::init_for_tests();
    let (_instance, port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    let token = make_valid_token(json!({"scope": "read", "exp": 123456}));

    assert_eq!(
        ansilo_e2e::util::main::connect_opts("token_read", &token, port, |_| ())
            .err()
            .unwrap()
            .to_string(),
        "db error: ERROR: Failed to authenticate JWT"
    );
}

#[test]
#[serial]
fn test_token_missing_claim() {
    ansilo_logging::init_for_tests();
    let (_instance, port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    let token = make_valid_token(json!({"scope": "invalid", "exp": valid_exp()}));

    assert_eq!(
        ansilo_e2e::util::main::connect_opts("token_read", &token, port, |_| ())
            .err()
            .unwrap()
            .to_string(),
        "db error: ERROR: Expected claim 'scope' to have at least one of 'read'"
    );
}

#[test]
#[serial]
fn test_valid_token() {
    ansilo_logging::init_for_tests();
    let (_instance, port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    let token = make_valid_token(json!({"scope": "read", "exp": valid_exp()}));
    ansilo_e2e::util::main::connect_opts("token_read", &token, port, |_| ()).unwrap();

    let token = make_valid_token(json!({"scope": "maintain", "exp": valid_exp()}));
    ansilo_e2e::util::main::connect_opts("token_maintain", &token, port, |_| ()).unwrap();
}

#[test]
#[serial]
fn test_read_scope_grants_access() {
    ansilo_logging::init_for_tests();
    let (_instance, port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    let token = make_valid_token(json!({"scope": "read", "exp": valid_exp()}));
    let mut client =
        ansilo_e2e::util::main::connect_opts("token_read", &token, port, |_| ()).unwrap();

    let rows = client.query("SELECT * FROM storage", &[]).unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "secret".to_string());
}

#[test]
#[serial]
fn test_read_scope_denied_write_access() {
    ansilo_logging::init_for_tests();
    let (_instance, port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    let token = make_valid_token(json!({"scope": "read", "exp": valid_exp()}));
    let mut client =
        ansilo_e2e::util::main::connect_opts("token_read", &token, port, |_| ()).unwrap();

    let err = client
        .query("INSERT INTO storage VALUES ('new')", &[])
        .err();

    assert_eq!(
        err.unwrap().to_string(),
        "db error: ERROR: permission denied for table storage"
    );
}

#[test]
#[serial]
fn test_maintain_scope_grants_full_access() {
    ansilo_logging::init_for_tests();
    let (_instance, port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    let token = make_valid_token(json!({"scope": "maintain", "exp": valid_exp()}));
    let mut client =
        ansilo_e2e::util::main::connect_opts("token_maintain", &token, port, |_| ()).unwrap();

    client
        .query("INSERT INTO storage VALUES ('new')", &[])
        .unwrap();
}

#[test]
#[serial]
fn test_auth_context_read_scope() {
    ansilo_logging::init_for_tests();
    let (_instance, port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    let exp = valid_exp();
    let token = make_valid_token(json!({"scope": "read", "exp": exp}));
    let mut client =
        ansilo_e2e::util::main::connect_opts("token_read", &token, port, |_| ()).unwrap();

    let row = client.query_one("SELECT auth_context()", &[]).unwrap();
    let ctx = row.get::<_, serde_json::Value>(0);
    let ctx: AuthContext = serde_json::from_value(ctx).unwrap();

    assert_eq!(ctx.username, "token_read");
    assert_eq!(ctx.provider, "jwt");
    assert_eq!(
        ctx.more,
        ProviderAuthContext::Jwt(JwtAuthContext {
            raw_token: token,
            header: json!({
                "alg": "RS512",
                "typ": "JWT"
            }),
            claims: [("scope".into(), json!("read")), ("exp".into(), json!(exp))]
                .into_iter()
                .collect()
        })
    );
}

#[test]
#[serial]
fn test_auth_context_maintain_scope() {
    ansilo_logging::init_for_tests();
    let (_instance, port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    let exp = valid_exp();
    let token = make_valid_token(json!({"scope": "maintain", "exp": exp}));
    let mut client =
        ansilo_e2e::util::main::connect_opts("token_maintain", &token, port, |_| ()).unwrap();

    let row = client.query_one("SELECT auth_context()", &[]).unwrap();
    let ctx = row.get::<_, serde_json::Value>(0);
    let ctx: AuthContext = serde_json::from_value(ctx).unwrap();

    assert_eq!(ctx.username, "token_maintain");
    assert_eq!(ctx.provider, "jwt");
    assert_eq!(
        ctx.more,
        ProviderAuthContext::Jwt(JwtAuthContext {
            raw_token: token,
            header: json!({
                "alg": "RS512",
                "typ": "JWT"
            }),
            claims: [
                ("scope".into(), json!("maintain")),
                ("exp".into(), json!(exp))
            ]
            .into_iter()
            .collect()
        })
    );
}

use ansilo_core::web::query::{QueryRequest, QueryResponse, QueryResults};
use ansilo_e2e::{current_dir, web::url};
use pretty_assertions::assert_eq;
use reqwest::StatusCode;
use serial_test::serial;

#[test]
#[serial]
fn test() {
    ansilo_logging::init_for_tests();
    let (instance, _port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    let client = reqwest::blocking::Client::new();
    let res = client
        .post(url(&instance, "/api/v1/query"))
        .json(&QueryRequest {
            sql: "SELECT 1 as col".into(),
            params: vec![],
        })
        .basic_auth("app", Some("pass"))
        .send()
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<QueryResponse>()
        .unwrap();

    assert_eq!(
        res,
        QueryResponse::Success(QueryResults {
            columns: vec![("col".to_string(), "Int32".to_string())],
            data: vec![vec!["1".to_string()]]
        })
    );
}

#[test]
#[serial]
fn test_with_query_param() {
    ansilo_logging::init_for_tests();
    let (instance, _port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    let client = reqwest::blocking::Client::new();
    let res = client
        .post(url(&instance, "/api/v1/query"))
        .json(&QueryRequest {
            sql: "SELECT $1 as col".into(),
            params: vec!["abc".into()],
        })
        .basic_auth("app", Some("pass"))
        .send()
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<QueryResponse>()
        .unwrap();

    assert_eq!(
        res,
        QueryResponse::Success(QueryResults {
            columns: vec![(
                "col".to_string(),
                "Utf8String(StringOptions { length: None })".to_string()
            )],
            data: vec![vec!["abc".to_string()]]
        })
    );
}

#[test]
#[serial]
fn test_invalid_user() {
    ansilo_logging::init_for_tests();
    let (instance, _port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    let client = reqwest::blocking::Client::new();
    let res = client
        .post(url(&instance, "/api/v1/query"))
        .json(&QueryRequest {
            sql: "SELECT 1 as col".into(),
            params: vec![],
        })
        .basic_auth("invalid", Some("pass"))
        .send()
        .unwrap();

    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[test]
#[serial]
fn test_invalid_password() {
    ansilo_logging::init_for_tests();
    let (instance, _port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    let client = reqwest::blocking::Client::new();
    let res = client
        .post(url(&instance, "/api/v1/query"))
        .json(&QueryRequest {
            sql: "SELECT 1 as col".into(),
            params: vec![],
        })
        .basic_auth("app", Some("invalid"))
        .send()
        .unwrap();

    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

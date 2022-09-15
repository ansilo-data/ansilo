use ansilo_core::web::query::{QueryRequest, QueryResponse};
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
            sql: r#"
            INSERT INTO test (data) VALUES ('abc');

            SELECT * FROM test 
            "#
            .into(),
            params: vec![],
        })
        .basic_auth("app", Some("pass"))
        .send()
        .unwrap();

    // Unfotunately not supported yet but should be!
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        res.json::<QueryResponse>().unwrap(),
        QueryResponse::Error(
            "db error: ERROR: cannot insert multiple commands into a prepared statement"
                .to_string()
                .into()
        )
    );
}

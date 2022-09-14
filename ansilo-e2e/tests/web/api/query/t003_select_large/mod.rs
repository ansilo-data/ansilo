use ansilo_core::web::query::{QueryRequest, QueryResponse, QueryResults};
use ansilo_e2e::{current_dir, web::url};
use pretty_assertions::assert_eq;
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
            sql: "SELECT x FROM generate_series(1, 1000000) AS x".into(),
        })
        .basic_auth("app", Some("pass"))
        .send()
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<QueryResponse>()
        .unwrap();

    // Should limit to 1000 rows
    assert_eq!(
        res,
        QueryResponse::Success(QueryResults {
            columns: vec![("x".to_string(), "Int32".to_string())],
            data: (1..=1000)
                .into_iter()
                .map(|x| vec![x.to_string()])
                .collect()
        })
    );
}

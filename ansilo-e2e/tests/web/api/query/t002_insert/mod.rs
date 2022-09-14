use ansilo_core::web::query::{QueryRequest, QueryResponse, QueryResults};
use ansilo_e2e::{current_dir, web::url};
use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
fn test() {
    ansilo_logging::init_for_tests();
    let (instance, port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    let client = reqwest::blocking::Client::new();
    let res = client
        .post(url(&instance, "/api/v1/query"))
        .json(&QueryRequest {
            sql: "INSERT INTO test (data) VALUES ('abc')".into(),
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
            columns: vec![],
            data: vec![]
        })
    );

    let mut client = ansilo_e2e::util::main::connect(port);

    let rows = client.query("SELECT * FROM test", &[]).unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>("data"), "abc");
}

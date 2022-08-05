use assert_json_diff::assert_json_include;

use super::query::explain_query_verbose;

#[macro_export]
macro_rules! assert_query_plan_expected {
    ($path:expr) => {
        setup_test(format!("query_plan/{}", $path));
        crate::fdw::test::query_plan::assert_query_plan_expected_fn(include_str!($path));
    };
}

pub fn assert_query_plan_expected_fn(spec_json: &str) {
    let json = serde_json::from_str::<serde_json::Value>(spec_json).unwrap();

    let query = json["SQL"].as_str().unwrap().to_string();
    let expected_plan = json["Expected"].clone();

    let actual_plan = explain_query_verbose(query);

    assert_json_include!(actual: actual_plan, expected: expected_plan)
}

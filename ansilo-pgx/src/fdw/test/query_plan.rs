use assert_json_diff::assert_json_include;
use pretty_assertions::assert_eq;

use super::query::explain_query_verbose;

#[macro_export]
macro_rules! assert_query_plan_expected {
    ($path:expr) => {
        setup_test(format!("query_plan/{}", $path));
        crate::fdw::test::query_plan::assert_query_plan_expected_fn(include_str!($path));
    };
}

#[track_caller]
pub fn assert_query_plan_expected_fn(spec_json: &str) {
    let json = serde_json::from_str::<serde_json::Value>(spec_json).unwrap();

    let query = json["SQL"].as_str().unwrap().to_string();
    let expected_plan = json["Expected"].clone();

    let actual_plan = explain_query_verbose(query);

    assert_json_include!(actual: actual_plan, expected: expected_plan);

    assert_query_ops_equal(&actual_plan, &expected_plan, "{root}".into());
}

/// For a subset of the json we want to be stricter in our assertions
#[track_caller]
fn assert_query_ops_equal(left: &serde_json::Value, right: &serde_json::Value, path: String) {
    let exact_match_fields = ["Remote Ops", "Remote Conds", "Local Conds"];

    match (left, right) {
        (serde_json::Value::Array(l), serde_json::Value::Array(r)) => {
            for key in 0..l.len() {
                if key < r.len() {
                    assert_query_ops_equal(&l[key], &r[key], format!("{path}[{key}]"));
                }
            }
        }
        (serde_json::Value::Object(l), serde_json::Value::Object(r)) => {
            for field in l.keys() {
                if r.contains_key(field) {
                    if exact_match_fields.contains(&field.as_str()) {
                        assert_eq!(l[field], r[field]);
                    }

                    assert_query_ops_equal(
                        &l[field],
                        &r[field],
                        [path.clone(), field.into()].join("."),
                    );
                }
            }
        }
        (_, _) => {}
    }
}

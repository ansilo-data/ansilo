use ansilo_core::{
    config::TokenClaimCheck,
    err::{bail, Result},
};
use serde_json::Value;

/// Validates the supplied claim value passes the check
pub(crate) fn validate_jwt_claim(
    name: &str,
    claim: Option<&Value>,
    check: &TokenClaimCheck,
) -> Result<()> {
    if claim.is_none() {
        bail!("Must provide claim '{name}'");
    }

    fn check_array(name: &str, check: &TokenClaimCheck, actual: &Vec<String>) -> Result<()> {
        match check {
            TokenClaimCheck::Any(expected) => {
                if !expected.iter().any(|c| actual.contains(c)) {
                    bail!(
                        "Expected claim '{name}' to have at least one of {}",
                        expected
                            .iter()
                            .map(|s| format!("'{s}'"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                }
            }
            TokenClaimCheck::All(expected) => {
                if !expected.iter().all(|c| actual.contains(c)) {
                    bail!(
                        "Expected claim '{name}' to have all {}",
                        expected
                            .iter()
                            .map(|s| format!("'{s}'"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                }
            }
            _ => unreachable!(),
        }

        Ok(())
    }

    fn get_strings(arr: &Vec<Value>) -> Vec<String> {
        arr.iter()
            .filter_map(|v| {
                if let Value::String(s) = v {
                    Some(s.into())
                } else {
                    None
                }
            })
            .collect()
    }

    fn get_space_delimited_strings(val: &str) -> Vec<String> {
        val.split(' ').map(|s| s.to_string()).collect()
    }

    let claim = claim.unwrap();

    match (check, claim) {
        (TokenClaimCheck::Eq(expected), Value::String(actual)) => {
            if expected != actual {
                bail!("Expected claim '{name}' to be '{expected}' but found '{actual}'")
            }
        }
        (TokenClaimCheck::Any(_), Value::Array(actual)) => {
            check_array(name, check, &get_strings(actual))?
        }
        (TokenClaimCheck::All(_), Value::Array(actual)) => {
            check_array(name, check, &get_strings(actual))?
        }
        (TokenClaimCheck::Any(_), Value::String(actual)) => {
            check_array(name, check, &get_space_delimited_strings(actual))?
        }
        (TokenClaimCheck::All(_), Value::String(actual)) => {
            check_array(name, check, &get_space_delimited_strings(actual))?
        }
        (TokenClaimCheck::All(_) | TokenClaimCheck::Any(_), _) => {
            bail!("Invalid type for claim '{name}' when expecting array")
        }
        (TokenClaimCheck::Eq(_), _) => {
            bail!("Invalid type for claim '{name}' when expecting string")
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_check_equal_invalid() {
        validate_jwt_claim("c", None, &TokenClaimCheck::Eq("abc".into())).unwrap_err();

        validate_jwt_claim("c", Some(&Value::Null), &TokenClaimCheck::Eq("abc".into()))
            .unwrap_err();

        validate_jwt_claim(
            "c",
            Some(&Value::String("invalid".into())),
            &TokenClaimCheck::Eq("abc".into()),
        )
        .unwrap_err();

        validate_jwt_claim(
            "c",
            Some(&Value::Array(vec![])),
            &TokenClaimCheck::Eq("abc".into()),
        )
        .unwrap_err();

        validate_jwt_claim(
            "c",
            Some(&Value::Array(vec![Value::String("abc".into())])),
            &TokenClaimCheck::Eq("abc".into()),
        )
        .unwrap_err();
    }

    #[test]
    fn test_validate_check_equal_valid() {
        validate_jwt_claim(
            "c",
            Some(&Value::String("abc".into())),
            &TokenClaimCheck::Eq("abc".into()),
        )
        .unwrap();

        validate_jwt_claim(
            "c",
            Some(&Value::String("abc123".into())),
            &TokenClaimCheck::Eq("abc123".into()),
        )
        .unwrap();
    }

    #[test]
    fn test_validate_check_all_invalid() {
        validate_jwt_claim("c", None, &TokenClaimCheck::All(vec!["abc".into()])).unwrap_err();

        validate_jwt_claim(
            "c",
            Some(&Value::Null),
            &TokenClaimCheck::All(vec!["abc".into()]),
        )
        .unwrap_err();

        validate_jwt_claim(
            "c",
            Some(&Value::String("invalid".into())),
            &TokenClaimCheck::All(vec!["abc".into()]),
        )
        .unwrap_err();

        validate_jwt_claim(
            "c",
            Some(&Value::Array(vec![])),
            &TokenClaimCheck::All(vec!["abc".into()]),
        )
        .unwrap_err();

        validate_jwt_claim(
            "c",
            Some(&Value::Array(vec![Value::String("invalid".into())])),
            &TokenClaimCheck::All(vec!["abc".into()]),
        )
        .unwrap_err();

        validate_jwt_claim(
            "c",
            Some(&Value::Array(vec![Value::String("abc".into())])),
            &TokenClaimCheck::All(vec!["abc".into(), "123".into()]),
        )
        .unwrap_err();
    }

    #[test]
    fn test_validate_check_all_valid() {
        validate_jwt_claim(
            "c",
            Some(&Value::Array(vec![Value::String("abc".into())])),
            &TokenClaimCheck::All(vec!["abc".into()]),
        )
        .unwrap();

        validate_jwt_claim(
            "c",
            Some(&Value::Array(vec![
                Value::String("123".into()),
                Value::String("abc".into()),
            ])),
            &TokenClaimCheck::All(vec!["abc".into()]),
        )
        .unwrap();

        validate_jwt_claim(
            "c",
            Some(&Value::Array(vec![
                Value::String("123".into()),
                Value::String("abc".into()),
            ])),
            &TokenClaimCheck::All(vec!["abc".into(), "123".into()]),
        )
        .unwrap();
    }

    #[test]
    fn test_validate_check_any_invalid() {
        validate_jwt_claim("c", None, &TokenClaimCheck::Any(vec!["abc".into()])).unwrap_err();

        validate_jwt_claim(
            "c",
            Some(&Value::Null),
            &TokenClaimCheck::Any(vec!["abc".into()]),
        )
        .unwrap_err();

        validate_jwt_claim(
            "c",
            Some(&Value::String("invalid".into())),
            &TokenClaimCheck::Any(vec!["abc".into()]),
        )
        .unwrap_err();

        validate_jwt_claim(
            "c",
            Some(&Value::Array(vec![])),
            &TokenClaimCheck::Any(vec!["abc".into()]),
        )
        .unwrap_err();

        validate_jwt_claim(
            "c",
            Some(&Value::Array(vec![Value::String("invalid".into())])),
            &TokenClaimCheck::Any(vec!["abc".into()]),
        )
        .unwrap_err();
    }

    #[test]
    fn test_validate_check_any_valid() {
        validate_jwt_claim(
            "c",
            Some(&Value::Array(vec![Value::String("abc".into())])),
            &TokenClaimCheck::Any(vec!["abc".into()]),
        )
        .unwrap();

        validate_jwt_claim(
            "c",
            Some(&Value::Array(vec![
                Value::String("123".into()),
                Value::String("abc".into()),
            ])),
            &TokenClaimCheck::Any(vec!["abc".into()]),
        )
        .unwrap();

        validate_jwt_claim(
            "c",
            Some(&Value::Array(vec![
                Value::String("123".into()),
                Value::String("abc".into()),
            ])),
            &TokenClaimCheck::Any(vec!["abc".into(), "123".into()]),
        )
        .unwrap();

        validate_jwt_claim(
            "c",
            Some(&Value::Array(vec![
                Value::String("123".into()),
                Value::String("abc".into()),
            ])),
            &TokenClaimCheck::Any(vec!["123".into()]),
        )
        .unwrap();
    }

    #[test]
    fn test_validate_check_all_space_delimited_string() {
        validate_jwt_claim(
            "c",
            Some(&Value::String("1234".into())),
            &TokenClaimCheck::All(vec!["abc".into()]),
        )
        .unwrap_err();

        validate_jwt_claim(
            "c",
            Some(&Value::String("abcd".into())),
            &TokenClaimCheck::All(vec!["abc".into()]),
        )
        .unwrap_err();

        validate_jwt_claim(
            "c",
            Some(&Value::String("abc".into())),
            &TokenClaimCheck::All(vec!["abc".into(), "123".into()]),
        )
        .unwrap_err();

        validate_jwt_claim(
            "c",
            Some(&Value::String("abc 123".into())),
            &TokenClaimCheck::All(vec!["abc".into(), "123".into()]),
        )
        .unwrap();

        validate_jwt_claim(
            "c",
            Some(&Value::String("123 abc 456".into())),
            &TokenClaimCheck::All(vec!["abc".into(), "123".into()]),
        )
        .unwrap();

        validate_jwt_claim(
            "c",
            Some(&Value::String("123 abc".into())),
            &TokenClaimCheck::All(vec!["123".into(), "abc".into()]),
        )
        .unwrap();

        validate_jwt_claim(
            "c",
            Some(&Value::String("123abc".into())),
            &TokenClaimCheck::All(vec!["123".into(), "abc".into()]),
        )
        .unwrap_err();

        validate_jwt_claim(
            "c",
            Some(&Value::String("123 abc".into())),
            &TokenClaimCheck::All(vec!["123".into(), "abc".into(), "456".into()]),
        )
        .unwrap_err();

        validate_jwt_claim(
            "c",
            Some(&Value::String("123 abc".into())),
            &TokenClaimCheck::All(vec!["123".into()]),
        )
        .unwrap();
    }

    #[test]
    fn test_validate_check_any_space_delimited_string() {
        validate_jwt_claim(
            "c",
            Some(&Value::String("1234".into())),
            &TokenClaimCheck::Any(vec!["abc".into()]),
        )
        .unwrap_err();

        validate_jwt_claim(
            "c",
            Some(&Value::String("abcd".into())),
            &TokenClaimCheck::Any(vec!["abc".into()]),
        )
        .unwrap_err();

        validate_jwt_claim(
            "c",
            Some(&Value::String("abc".into())),
            &TokenClaimCheck::Any(vec!["abc".into()]),
        )
        .unwrap();

        validate_jwt_claim(
            "c",
            Some(&Value::String("abc 123".into())),
            &TokenClaimCheck::Any(vec!["abc".into()]),
        )
        .unwrap();

        validate_jwt_claim(
            "c",
            Some(&Value::String("123 abc".into())),
            &TokenClaimCheck::Any(vec!["abc".into()]),
        )
        .unwrap();

        validate_jwt_claim(
            "c",
            Some(&Value::String("123 abc".into())),
            &TokenClaimCheck::Any(vec!["123".into(), "abc".into()]),
        )
        .unwrap();

        validate_jwt_claim(
            "c",
            Some(&Value::String("123abc".into())),
            &TokenClaimCheck::Any(vec!["123".into(), "abc".into()]),
        )
        .unwrap_err();

        validate_jwt_claim(
            "c",
            Some(&Value::String("123 abc".into())),
            &TokenClaimCheck::Any(vec!["123".into()]),
        )
        .unwrap();
    }
}

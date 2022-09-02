use ansilo_core::{
    config::TokenClaimCheck,
    err::{bail, Result},
};
use serde_json::Value;

/// Validates the supplied claim value passes the check
pub(crate) fn validate_check(
    claim_type: &str,
    name: &str,
    claim: Option<&Value>,
    check: &TokenClaimCheck,
) -> Result<()> {
    if claim.is_none() {
        bail!("Must provide {claim_type} '{name}'");
    }

    let claim = claim.unwrap();

    match (check, claim) {
        (TokenClaimCheck::Eq(expected), Value::String(actual)) => {
            if expected != actual {
                bail!("Expected {claim_type} '{claim}' to be '{expected}' but found '{actual}'")
            }
        }
        (TokenClaimCheck::Any(expected), Value::Array(actual)) => {
            if !expected
                .iter()
                .any(|c| actual.contains(&Value::String(c.into())))
            {
                bail!(
                    "Expected {claim_type} '{claim}' to have at least one of {}",
                    expected
                        .iter()
                        .map(|s| format!("'{s}'"))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
        }
        (TokenClaimCheck::All(expected), Value::Array(actual)) => {
            if !expected
                .iter()
                .all(|c| actual.contains(&Value::String(c.into())))
            {
                bail!(
                    "Expected {claim_type} '{claim}' to have all {}",
                    expected
                        .iter()
                        .map(|s| format!("'{s}'"))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
        }
        (TokenClaimCheck::All(_) | TokenClaimCheck::Any(_), _) => {
            bail!("Invalid type for {claim_type} '{name}' when expecting array")
        }
        (TokenClaimCheck::Eq(_), _) => {
            bail!("Invalid type for {claim_type} '{name}' when expecting string")
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_check_equal_invalid() {
        validate_check("t", "c", None, &TokenClaimCheck::Eq("abc".into())).unwrap_err();

        validate_check(
            "t",
            "c",
            Some(&Value::Null),
            &TokenClaimCheck::Eq("abc".into()),
        )
        .unwrap_err();

        validate_check(
            "t",
            "c",
            Some(&Value::String("invalid".into())),
            &TokenClaimCheck::Eq("abc".into()),
        )
        .unwrap_err();

        validate_check(
            "t",
            "c",
            Some(&Value::Array(vec![])),
            &TokenClaimCheck::Eq("abc".into()),
        )
        .unwrap_err();

        validate_check(
            "t",
            "c",
            Some(&Value::Array(vec![Value::String("abc".into())])),
            &TokenClaimCheck::Eq("abc".into()),
        )
        .unwrap_err();
    }

    #[test]
    fn test_validate_check_equal_valid() {
        validate_check(
            "t",
            "c",
            Some(&Value::String("abc".into())),
            &TokenClaimCheck::Eq("abc".into()),
        )
        .unwrap();

        validate_check(
            "t",
            "c",
            Some(&Value::String("abc123".into())),
            &TokenClaimCheck::Eq("abc123".into()),
        )
        .unwrap();
    }

    #[test]
    fn test_validate_check_all_invalid() {
        validate_check("t", "c", None, &TokenClaimCheck::All(vec!["abc".into()])).unwrap_err();

        validate_check(
            "t",
            "c",
            Some(&Value::Null),
            &TokenClaimCheck::All(vec!["abc".into()]),
        )
        .unwrap_err();

        validate_check(
            "t",
            "c",
            Some(&Value::String("invalid".into())),
            &TokenClaimCheck::All(vec!["abc".into()]),
        )
        .unwrap_err();

        validate_check(
            "t",
            "c",
            Some(&Value::Array(vec![])),
            &TokenClaimCheck::All(vec!["abc".into()]),
        )
        .unwrap_err();

        validate_check(
            "t",
            "c",
            Some(&Value::Array(vec![Value::String("invalid".into())])),
            &TokenClaimCheck::All(vec!["abc".into()]),
        )
        .unwrap_err();

        validate_check(
            "t",
            "c",
            Some(&Value::Array(vec![Value::String("abc".into())])),
            &TokenClaimCheck::All(vec!["abc".into(), "123".into()]),
        )
        .unwrap_err();
    }

    #[test]
    fn test_validate_check_all_valid() {
        validate_check(
            "t",
            "c",
            Some(&Value::Array(vec![Value::String("abc".into())])),
            &TokenClaimCheck::All(vec!["abc".into()]),
        )
        .unwrap();

        validate_check(
            "t",
            "c",
            Some(&Value::Array(vec![
                Value::String("123".into()),
                Value::String("abc".into()),
            ])),
            &TokenClaimCheck::All(vec!["abc".into()]),
        )
        .unwrap();

        validate_check(
            "t",
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
        validate_check("t", "c", None, &TokenClaimCheck::Any(vec!["abc".into()])).unwrap_err();

        validate_check(
            "t",
            "c",
            Some(&Value::Null),
            &TokenClaimCheck::Any(vec!["abc".into()]),
        )
        .unwrap_err();

        validate_check(
            "t",
            "c",
            Some(&Value::String("invalid".into())),
            &TokenClaimCheck::Any(vec!["abc".into()]),
        )
        .unwrap_err();

        validate_check(
            "t",
            "c",
            Some(&Value::Array(vec![])),
            &TokenClaimCheck::Any(vec!["abc".into()]),
        )
        .unwrap_err();

        validate_check(
            "t",
            "c",
            Some(&Value::Array(vec![Value::String("invalid".into())])),
            &TokenClaimCheck::Any(vec!["abc".into()]),
        )
        .unwrap_err();
    }

    #[test]
    fn test_validate_check_any_valid() {
        validate_check(
            "t",
            "c",
            Some(&Value::Array(vec![Value::String("abc".into())])),
            &TokenClaimCheck::Any(vec!["abc".into()]),
        )
        .unwrap();

        validate_check(
            "t",
            "c",
            Some(&Value::Array(vec![
                Value::String("123".into()),
                Value::String("abc".into()),
            ])),
            &TokenClaimCheck::Any(vec!["abc".into()]),
        )
        .unwrap();

        validate_check(
            "t",
            "c",
            Some(&Value::Array(vec![
                Value::String("123".into()),
                Value::String("abc".into()),
            ])),
            &TokenClaimCheck::Any(vec!["abc".into(), "123".into()]),
        )
        .unwrap();

        validate_check(
            "t",
            "c",
            Some(&Value::Array(vec![
                Value::String("123".into()),
                Value::String("abc".into()),
            ])),
            &TokenClaimCheck::Any(vec!["123".into()]),
        )
        .unwrap();
    }
}

use ansilo_core::{auth::AuthContext, err::Context};
use pgx::*;

use crate::auth::ctx::CurrentAuthContext;

use super::ctx::AuthContextState;

extension_sql!(
    r#"
    CREATE FUNCTION __ansilo_auth."ansilo_set_auth_context"(
        "context" text,
        "reset_nonce" text
    ) RETURNS text
    VOLATILE PARALLEL UNSAFE STRICT
    LANGUAGE c /* Rust */
    AS 'MODULE_PATHNAME', 'ansilo_set_auth_context_wrapper';
"#,
    name = "ansilo_set_auth_context",
    requires = ["ansilo_auth_schema"]
);

#[pg_extern(sql = "")]
fn ansilo_set_auth_context(context: String, reset_nonce: String) -> String {
    debug1!("Setting auth context");
    debug5!("Requested set auth context to '{}'", context.clone());

    assert!(AuthContextState::get().is_none(), "Already in auth context");

    if unsafe { pg_sys::IsTransactionBlock() } {
        panic!("Cannot change auth context in transaction");
    }

    let parsed: serde_json::Value = serde_json::from_str(&context)
        .context("Failed to parse auth context as json")
        .unwrap();
    let context: AuthContext = serde_json::from_value(parsed.clone())
        .context("Failed to parse auth context json structure")
        .unwrap();

    assert!(
        reset_nonce.len() >= 16,
        "Nonce must be at least 16 bytes long"
    );

    AuthContextState::update(AuthContextState::Set(CurrentAuthContext {
        context,
        parsed,
        reset_nonce,
    }));

    debug1!("Auth context updated");

    "OK".into()
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use std::panic::catch_unwind;

    use super::*;

    #[pg_test]
    fn test_set_auth_context_invalid_nonce() {
        catch_unwind(|| ansilo_set_auth_context("test".into(), "test".into())).unwrap_err();
        catch_unwind(|| ansilo_set_auth_context("test".into(), "123456789012345".into()))
            .unwrap_err();
    }

    #[pg_test]
    fn test_set_auth_context_invalid_json() {
        catch_unwind(|| ansilo_set_auth_context("not valid json".into(), "123456789012345".into()))
            .unwrap_err();
    }

    #[pg_test]
    fn test_set_auth_context_valid() {
        let (mut client, _) = pgx_tests::client();

        client
            .batch_execute(
                r#"
            DO $$BEGIN
               ASSERT __ansilo_auth.ansilo_set_auth_context('{"username": "foo", "provider": "bar", "authenticated_at": 123, "type": "password"}', '1234567890123456') = 'OK';
            END$$
        "#,
            )
            .unwrap();
    }

    #[pg_test]
    fn test_set_auth_context_fails_when_already_set() {
        let (mut client, _) = pgx_tests::client();

        client
            .batch_execute(
                r#"
            DO $$BEGIN
                ASSERT __ansilo_auth.ansilo_set_auth_context('{"username": "foo", "provider": "bar", "authenticated_at": 123, "type": "password"}', '1234567890123456') = 'OK';
            END$$
            "#,
            )
            .unwrap();

        client
            .batch_execute(r#"SELECT ansilo_set_auth_context('{"username": "foo", "provider": "bar", "authenticated_at": 123, "type": "password"}', '1234567890123456');"#)
            .unwrap_err();
    }

    #[pg_test]
    fn test_set_auth_context_fails_when_in_transaction() {
        let (mut client, _) = pgx_tests::client();
        let mut t = client.transaction().unwrap();

        t.batch_execute(r#"SELECT ansilo_set_auth_context('{"username": "foo", "provider": "bar", "authenticated_at": 123, "type": "password"}', '1234567890123456');"#)
            .unwrap_err();
    }
}

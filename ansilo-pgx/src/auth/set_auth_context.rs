use pgx::*;

use crate::auth::ctx::AuthContextState;

use super::ctx::AuthContext;

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
    info!("Requested set auth context to '{}'", context.clone());

    assert!(AuthContext::get().is_none(), "Already in auth context");

    if unsafe { pg_sys::IsTransactionBlock() } {
        panic!("Cannot change auth context in transaction");
    }

    assert!(
        reset_nonce.len() >= 16,
        "Nonce must be at least 16 bytes long"
    );

    AuthContext::update(AuthContext::Set(AuthContextState {
        context,
        reset_nonce,
    }));

    info!("Auth context updated");

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
    fn test_set_auth_context_valid() {
        let (mut client, _) = pgx_tests::client();

        client
            .batch_execute(
                r#"
            DO $$BEGIN
               ASSERT __ansilo_auth.ansilo_set_auth_context('test', '1234567890123456') = 'OK';
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
                ASSERT __ansilo_auth.ansilo_set_auth_context('test', '1234567890123456') = 'OK';
            END$$
            "#,
            )
            .unwrap();

        client
            .batch_execute(r#"SELECT ansilo_set_auth_context('test', '1234567890123456');"#)
            .unwrap_err();
    }

    #[pg_test]
    fn test_set_auth_context_fails_when_in_transaction() {
        let (mut client, _) = pgx_tests::client();
        let mut t = client.transaction().unwrap();

        t.batch_execute(r#"SELECT ansilo_set_auth_context('test', '1234567890123456');"#)
            .unwrap_err();
    }
}

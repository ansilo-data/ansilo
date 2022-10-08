use pgx::*;
use subtle::ConstantTimeEq;

use crate::{fdw::common::clear_fdw_ipc_connections, rq::clear_rq_prepared_queries};

use super::ctx::AuthContextState;

extension_sql!(
    r#"
    CREATE FUNCTION __ansilo_auth."ansilo_reset_auth_context"(
        "reset_nonce" text
    ) RETURNS text
    VOLATILE PARALLEL UNSAFE STRICT
    LANGUAGE c /* Rust */
    AS 'MODULE_PATHNAME', 'ansilo_reset_auth_context_wrapper';
    
"#,
    name = "ansilo_reset_auth_context",
    requires = ["ansilo_auth_schema"]
);

#[pg_extern(sql = "")]
fn ansilo_reset_auth_context(reset_nonce: String) -> String {
    debug1!("Requested to reset auth context");

    assert!(AuthContextState::get().is_some(), "Not in auth context");

    if unsafe { pg_sys::IsTransactionBlock() } {
        panic!("Cannot change auth context in transaction");
    }

    let context = AuthContextState::get().unwrap();

    if context
        .reset_nonce
        .as_bytes()
        .ct_eq(reset_nonce.as_bytes())
        .unwrap_u8()
        != 1
    {
        FATAL!("Invalid reset nonce when attempting to reset auth context, aborting process to prevent tampering");
    }

    AuthContextState::update(AuthContextState::None);
    clear_fdw_ipc_connections();
    clear_rq_prepared_queries();

    debug1!("Auth context reset");

    "OK".into()
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use std::panic::catch_unwind;

    use super::*;

    #[pg_test]
    fn test_reset_auth_context_fails_when_not_in_context() {
        catch_unwind(|| ansilo_reset_auth_context("test".into())).unwrap_err();
    }

    #[pg_test]
    fn test_reset_auth_context_fails_with_invalid_nonce() {
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
            .batch_execute(r#"SELECT __ansilo_auth.ansilo_reset_auth_context('invalid');"#)
            .unwrap_err();

        // Invalid nonce should close the connection
        client.batch_execute(r#"SELECT 1"#).unwrap_err();
    }

    #[pg_test]
    fn test_reset_auth_context_valid() {
        let (mut client, _) = pgx_tests::client();

        // should be able to set context again after resetting it
        for _ in 1..5 {
            client
                .batch_execute(
                    r#"
                    DO $$BEGIN
                    ASSERT __ansilo_auth.ansilo_set_auth_context('{"username": "foo", "provider": "bar", "authenticated_at": 123, "type": "password"}', '1234567890123456') = 'OK';
                    ASSERT __ansilo_auth.ansilo_reset_auth_context('1234567890123456') = 'OK';
                    END$$
                "#,
                )
                .unwrap();
        }
    }

    #[pg_test]
    fn test_reset_auth_context_fails_when_in_transaction() {
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

        let mut t = client.transaction().unwrap();

        t.batch_execute(r#"SELECT ansilo_reset_auth_context('1234567890123456');"#)
            .unwrap_err();
    }
}

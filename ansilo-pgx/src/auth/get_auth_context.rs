use pgx::*;

use super::ctx::AuthContext;

#[pg_extern(stable)]
fn auth_context() -> String {
    assert!(AuthContext::get().is_some(), "Not in auth context");

    let context = AuthContext::get().unwrap();

    context.context
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use std::panic::catch_unwind;

    use super::*;

    #[pg_test]
    fn test_get_auth_context_fails_when_not_in_context() {
        catch_unwind(|| auth_context()).unwrap_err();
    }

    #[pg_test]
    fn test_get_auth_context_valid() {
        let (mut client, _) = pgx_tests::client();

        client
            .batch_execute(
                r#"
                    DO $$BEGIN
                    ASSERT ansilo_set_auth_context('test123', '1234567890123456') = 'OK';
                    ASSERT auth_context() = 'test123';
                    END$$
                "#,
            )
            .unwrap();
    }

    #[pg_test]
    fn test_get_auth_context_fails_after_reset() {
        let (mut client, _) = pgx_tests::client();

        client
            .batch_execute(
                r#"
                    DO $$BEGIN
                    ASSERT ansilo_set_auth_context('test123', '1234567890123456') = 'OK';
                    ASSERT auth_context() = 'test123';
                    ASSERT ansilo_reset_auth_context('1234567890123456') = 'OK';
                    END$$
                "#,
            )
            .unwrap();

        client
            .batch_execute(r#"SELECT auth_context();"#)
            .unwrap_err();
    }
}

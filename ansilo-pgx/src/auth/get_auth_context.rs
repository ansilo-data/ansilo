use pgx::*;

use super::ctx::AuthContextState;

#[pg_extern(stable)]
fn auth_context() -> Option<JsonB> {
    match AuthContextState::get() {
        Some(context) => Some(JsonB(context.parsed)),
        None => None,
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

    #[pg_test]
    fn test_get_auth_context_returns_null_when_not_in_context() {
        assert!(auth_context().is_none());
    }

    #[pg_test]
    fn test_get_auth_context_valid() {
        let (mut client, _) = pgx_tests::client().unwrap();

        client
            .batch_execute(
                r#"
                    DO $$BEGIN
                    ASSERT __ansilo_auth.ansilo_set_auth_context('{"username": "foo", "provider": "bar", "authenticated_at": 123, "type": "password"}', '1234567890123456') = 'OK';
                    ASSERT auth_context() = '{"username": "foo", "provider": "bar", "authenticated_at": 123, "type": "password"}';
                    END$$
                "#,
            )
            .unwrap();
    }

    #[pg_test]
    fn test_get_auth_context_fails_after_reset() {
        let (mut client, _) = pgx_tests::client().unwrap();

        client
            .batch_execute(
                r#"
                    DO $$BEGIN
                    ASSERT __ansilo_auth.ansilo_set_auth_context('{"username": "foo", "provider": "bar", "authenticated_at": 123, "type": "password"}', '1234567890123456') = 'OK';
                    ASSERT auth_context() = '{"username": "foo", "provider": "bar", "authenticated_at": 123, "type": "password"}';
                    ASSERT __ansilo_auth.ansilo_reset_auth_context('1234567890123456') = 'OK';
                    ASSERT auth_context() IS NULL;
                    END$$
                "#,
            )
            .unwrap();
    }
}

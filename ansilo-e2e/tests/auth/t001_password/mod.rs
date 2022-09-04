use std::env;

use ansilo_core::auth::{AuthContext, PasswordAuthContext, ProviderAuthContext};
use ansilo_e2e::current_dir;
use pretty_assertions::assert_eq;

#[test]
fn test() {
    ansilo_logging::init_for_tests();
    let (_instance, port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    assert!(ansilo_e2e::util::main::connect_opts("invalid", "pass", port).is_err());
    assert!(ansilo_e2e::util::main::connect_opts("test_user", "wrong_pass", port).is_err());

    let mut client =
        ansilo_e2e::util::main::connect_opts("test_user", "password123", port).unwrap();

    let rows = client.query("SELECT * FROM dummy", &[]).unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "data".to_string());

    // Test auth context
    let row = client.query_one("SELECT auth_context()", &[]).unwrap();
    let ctx = row.get::<_, serde_json::Value>(0);
    let ctx: AuthContext = serde_json::from_value(ctx).unwrap();

    assert_eq!(ctx.username, "test_user");
    assert_eq!(ctx.provider, "password");
    assert_eq!(
        ctx.more,
        ProviderAuthContext::Password(PasswordAuthContext::default())
    );
}

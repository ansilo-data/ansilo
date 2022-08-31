use std::{ffi::CString, ptr};

use pgx::*;

use crate::util::{
    string::{parse_to_owned_utf8_string, to_pg_cstr},
    syscache::PgSysCacheItem,
};

use super::ctx::AuthContext;

#[pg_extern(volatile, parallel_unsafe)]
fn ansilo_set_auth_context(context: String, reset_nonce: String) -> String {
    info!("Requested set auth context to '{}'", context.clone());

    assert!(AuthContext::context().is_none(), "Already in auth context");

    if unsafe { pg_sys::IsTransactionBlock() } {
        panic!("Cannot assume user in transaction");
    }

    assert!(
        reset_nonce.len() < 16,
        "Nonce must be at least 16 bytes long"
    );

    let original_user_id = unsafe { pg_sys::GetUserId() };
    let original_user_name = unsafe {
        let name = pg_sys::GetUserNameFromId(original_user_id, false);
        parse_to_owned_utf8_string(name).unwrap()
    };

    let assumed_user_name = username;
    let assumed_user_id = {
        let name_str = CString::new(assumed_user_name.clone()).unwrap();
        let item = PgSysCacheItem::<pg_sys::FormData_pg_authid>::search(
            pg_sys::SysCacheIdentifier_AUTHNAME,
            [name_str.as_ptr().into()],
        );

        if item.is_none() {
            panic!("User '{}' does not exist", assumed_user_name);
        }

        let item = item.unwrap();

        assert!(!item.rolsuper, "Cannot assume to superuser");

        item.oid
    };

    let ctx = AuthContext::Set(AuthContext {
        assumed_user_id,
        assumed_user_name: assumed_user_name.clone(),
        original_user_id,
        original_user_name: original_user_name.clone(),
        reset_nonce,
    });

    AuthContext::update(ctx);

    info!(
        "Assuming from user '{}' to user '{}'",
        original_user_name, assumed_user_name
    );

    unsafe {
        pg_sys::SetCurrentRoleId(assumed_user_role, false);
    }

    info!("Now assumed user '{}'", assumed_user_name);

    "OK".into()
}

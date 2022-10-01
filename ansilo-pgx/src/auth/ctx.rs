use std::sync::{Mutex, MutexGuard};

use ansilo_core::auth::AuthContext;
use lazy_static::lazy_static;

/// Global state of additional authentication context
#[derive(Clone, PartialEq)]
pub enum AuthContextState {
    None,
    Set(CurrentAuthContext),
}

#[derive(Clone, PartialEq)]
pub struct CurrentAuthContext {
    pub context: AuthContext,
    pub parsed: serde_json::Value,
    pub reset_nonce: String,
}

lazy_static! {
    static ref AUTH_CONTEXT: Mutex<AuthContextState> = Mutex::new(AuthContextState::None);
}

impl AuthContextState {
    fn lock<'a>() -> MutexGuard<'a, Self> {
        AUTH_CONTEXT.lock().expect("Failed to lock auth context")
    }

    pub fn get() -> Option<CurrentAuthContext> {
        pgx::debug1!("Retreiving auth context");
        let ctx = Self::lock();

        match &*ctx {
            Self::None => None,
            Self::Set(a) => Some(a.clone()),
        }
    }

    pub fn update(new: Self) {
        pgx::debug1!("Updating auth context");
        let mut ctx = Self::lock();

        *ctx = new;
    }
}

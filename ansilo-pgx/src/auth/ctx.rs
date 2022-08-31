use std::sync::{Mutex, MutexGuard};

use lazy_static::lazy_static;

/// Global state of additional authentication context
#[derive(Clone, PartialEq)]
pub enum AuthContext {
    None,
    Set(AuthContextState),
}

#[derive(Clone, PartialEq)]
pub struct AuthContextState {
    pub context: String,
    pub reset_nonce: String,
}

lazy_static! {
    static ref AUTH_CONTEXT: Mutex<AuthContext> = Mutex::new(AuthContext::None);
}

impl AuthContext {
    fn lock<'a>() -> MutexGuard<'a, Self> {
        AUTH_CONTEXT.lock().expect("Failed to lock auth context")
    }

    pub fn get() -> Option<AuthContextState> {
        let ctx = Self::lock();

        match &*ctx {
            Self::None => None,
            Self::Set(a) => Some(a.clone()),
        }
    }

    pub fn update(new: Self) {
        let mut ctx = Self::lock();

        *ctx = new;
    }
}

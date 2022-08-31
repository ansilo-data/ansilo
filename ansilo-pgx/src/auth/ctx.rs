use std::sync::{Mutex, MutexGuard};

use lazy_static::lazy_static;

/// Global state of additional authentication context
#[derive(Clone, PartialEq)]
pub enum AuthContext {
    None,
    Set(String),
}

lazy_static! {
    static ref AUTH_CONTEXT: Mutex<AuthContext> = Mutex::new(AuthContext::None);
}

impl AuthContext {
    pub fn get<'a>() -> MutexGuard<'a, Self> {
        AUTH_CONTEXT.lock().expect("Failed to lock auth context")
    }

    pub fn context() -> Option<String> {
        let ctx = AUTH_CONTEXT.lock().expect("Failed to lock auth context");

        match &*ctx {
            Self::None => None,
            Self::Set(a) => Some(a.clone()),
        }
    }

    pub fn update(new: Self) {
        let mut ctx = AUTH_CONTEXT.lock().expect("Failed to lock auth context");

        *ctx = new;
    }
}

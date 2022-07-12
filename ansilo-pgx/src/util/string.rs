use ansilo_core::err::{Result, bail, Context};
use std::ffi::CStr;

/// Converts a null-terminated C string to a rust UTF-8 string
pub unsafe fn parse_to_owned_utf8_string(ptr: *const i8) -> Result<String> {
    if ptr.is_null() {
        bail!("Failed pointer is null")
    }

    CStr::from_ptr(ptr)
        .to_str()
        .map(|s| s.to_string())
        .context("Failed to parse option as valid UTF-8 string")
}

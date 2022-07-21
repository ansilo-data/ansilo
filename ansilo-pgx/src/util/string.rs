use ansilo_core::err::{bail, Context, Result};
use std::ffi::{CStr, CString};

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

/// Converts a rust string to a pg allocated cstr pointer
pub fn to_cstr(string: &str) -> Result<CString> {
    CString::new(string)
        .context("Failed to convert rust string to C string")
}

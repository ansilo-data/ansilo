use ansilo_core::err::{bail, Context, Result};
use pgx::pg_sys;
use std::{
    ffi::{CStr, CString},
    ptr,
};

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

/// Converts a rust string to a cstring (still rust allocated)
pub fn to_cstr(string: &str) -> Result<CString> {
    CString::new(string).context("Failed to convert rust string to C string")
}

/// Converts a rust string to a pg allocated cstr pointer
pub unsafe fn to_pg_cstr(string: &str) -> Result<*mut i8> {
    let cstr = to_cstr(string)?;
    let bytes = cstr.as_bytes_with_nul();

    let pg_str = pg_sys::palloc(bytes.len()) as *mut u8;
    ptr::copy_nonoverlapping(bytes.as_ptr(), pg_str, bytes.len());

    Ok(pg_str as *mut i8)
}

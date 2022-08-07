use ansilo_core::err::{bail, Result};
use pgx::pg_sys::{defGetString, DefElem};

use super::string::parse_to_owned_utf8_string;

pub unsafe fn def_get_owned_utf8_string(opt: *mut DefElem) -> Result<String> {
    if opt.is_null() {
        bail!("Failed to parse option as string");
    }

    let ptr = defGetString(opt);

    parse_to_owned_utf8_string(ptr)
}

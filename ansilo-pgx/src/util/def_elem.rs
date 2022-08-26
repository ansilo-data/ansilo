use std::collections::HashMap;

use ansilo_core::err::{bail, Result};
use pgx::{
    pg_sys::{defGetString, DefElem},
    PgList,
};

use super::string::parse_to_owned_utf8_string;

pub unsafe fn parse_def_elems_to_hash_map(
    opts: PgList<DefElem>,
) -> Result<HashMap<String, String>> {
    let mut map = HashMap::new();

    for opt in opts.iter_ptr() {
        map.insert(
            parse_to_owned_utf8_string((*opt).defname)?,
            def_get_owned_utf8_string(opt)?,
        );
    }

    Ok(map)
}

pub unsafe fn def_get_owned_utf8_string(opt: *mut DefElem) -> Result<String> {
    if opt.is_null() {
        bail!("Failed to parse option as string");
    }

    let ptr = defGetString(opt);

    parse_to_owned_utf8_string(ptr)
}

#[cfg(any(test, feature = "pg_test"))]
#[pgx::pg_schema]
mod tests {
    use cstr::cstr;
    use pgx::*;
    use std::ptr;

    use super::*;

    #[pg_test]
    fn test_def_elems_to_hash_map_null() {
        unsafe {
            assert_eq!(
                parse_def_elems_to_hash_map(PgList::from_pg(ptr::null_mut())).unwrap(),
                HashMap::new()
            );
        }
    }

    #[pg_test]
    fn test_def_elems_to_hash_map_with_options() {
        unsafe {
            let mut opts = PgList::<DefElem>::new();

            let mut elem = DefElem::default();

            elem.defname = cstr!("foo").as_ptr() as *mut _;
            elem.arg = pg_sys::makeString(cstr!("bar").as_ptr() as *mut _) as *mut pg_sys::Node;

            opts.push(&mut elem as *mut _);

            assert_eq!(
                parse_def_elems_to_hash_map(opts).unwrap(),
                [("foo".to_string(), "bar".to_string())]
                    .into_iter()
                    .collect::<HashMap<_, _>>()
            );
        }
    }
}

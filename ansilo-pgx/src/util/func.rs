use std::{ffi::CString, ptr};

use pgx::*;

/// Calls the supplied user defined function by its name
/// We assume this function takes no arguments
pub(crate) unsafe fn call_udf(name: &str) {
    debug1!("Invoking user-defined function: {name}");
    let c_func_name = CString::new(name).unwrap();
    let mut func_name = PgList::<pg_sys::Value>::new();
    func_name.push(pg_sys::makeString(c_func_name.as_ptr() as *mut _));

    // Look up the function by name
    let func_oid = pg_sys::LookupFuncName(func_name.as_ptr(), 0, ptr::null(), false);

    // Invoke the function in a dedicated memory context
    // to ensure everything is cleaned up
    // @see https://doxygen.postgresql.org/fmgr_8c.html#aefdeecfeb6f7fbc861bde3cfd3d407c8
    pgx::PgMemoryContexts::new("udf_invoke").switch_to(move |_| {
        // Init function call structs
        let mut flinfo = pg_sys::FmgrInfo::default();

        // Init the function lookup info
        pg_sys::fmgr_info(func_oid as _, &mut flinfo as *mut _);

        // It is too painful to calculate required size based off
        // the SizeForFunctionCallInfo macro in rust.
        // So just allocate far more than is necessary. Famous last words.
        let mut fcinfo: pg_sys::FunctionCallInfo = &mut [0u8; 1024] as *mut u8 as *mut _;

        // Init fcinfo, @see https://doxygen.postgresql.org/fmgr_8h_source.html#l00150
        (*fcinfo).flinfo = &mut flinfo as *mut _;
        (*fcinfo).context = ptr::null_mut();
        (*fcinfo).resultinfo = ptr::null_mut();
        (*fcinfo).fncollation = pg_sys::InvalidOid;
        (*fcinfo).nargs = 0;

        // Finally invoke the function
        // We dont care about the result
        let _ = (*(*fcinfo).flinfo).fn_addr.unwrap()(fcinfo);
    });
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use std::panic;

    use super::*;

    #[pg_test]
    fn test_call_udf_non_existant_func() {
        unsafe {
            let res = panic::catch_unwind(|| {
                call_udf("non_existant_function");
            });

            assert!(res.is_err())
        }
    }

    #[pg_test]
    fn test_call_udf_with_valid_function() {
        unsafe {
            call_udf("hello_ansilo");
        }
    }
}

use pgx::pg_sys::*;

pub unsafe extern "C" fn is_foreign_path_async_capable(path: *mut ForeignPath) -> bool {
    unimplemented!()
}

pub unsafe extern "C" fn foreign_async_request(areq: *mut AsyncRequest) {
    unimplemented!()
}

pub unsafe extern "C" fn foreign_async_configure_wait(areq: *mut AsyncRequest) {
    unimplemented!()
}

pub unsafe extern "C" fn foreign_async_notify(areq: *mut AsyncRequest) {
    unimplemented!()
}
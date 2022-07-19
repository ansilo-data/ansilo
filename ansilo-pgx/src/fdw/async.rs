use pgx::*;
use pgx::pg_sys::{AsyncRequest, ForeignPath};

#[pg_guard]
pub unsafe extern "C" fn is_foreign_path_async_capable(path: *mut ForeignPath) -> bool {
    unimplemented!()
}

#[pg_guard]
pub unsafe extern "C" fn foreign_async_request(areq: *mut AsyncRequest) {
    unimplemented!()
}

#[pg_guard]
pub unsafe extern "C" fn foreign_async_configure_wait(areq: *mut AsyncRequest) {
    unimplemented!()
}

#[pg_guard]
pub unsafe extern "C" fn foreign_async_notify(areq: *mut AsyncRequest) {
    unimplemented!()
}
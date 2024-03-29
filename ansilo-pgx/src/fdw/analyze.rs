use pgx::*;
use pgx::pg_sys::{HeapTuple, Relation, BlockNumber, AcquireSampleRowsFunc};

/// We currenot do not support analyzing foriegn tables
#[pg_guard]
pub unsafe extern "C" fn acquire_sampl(
    relation: Relation,
    elevel: ::std::os::raw::c_int,
    rows: *mut HeapTuple,
    targrows: ::std::os::raw::c_int,
    totalrows: *mut f64,
    totaldeadrows: *mut f64,
) -> ::std::os::raw::c_int {
    unimplemented!()
}

#[pg_guard]
pub unsafe extern "C" fn analyze_foreign_table(
    relation: Relation,
    func: *mut AcquireSampleRowsFunc,
    totalpages: *mut BlockNumber,
) -> bool {
    unimplemented!()
}

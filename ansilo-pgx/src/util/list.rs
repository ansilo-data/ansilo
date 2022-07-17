use pgx::{pg_sys, PgList};

/// Converts a rust allocated vec to a pg-owned List
pub unsafe fn vec_to_pg_list<T>(vec: Vec<*mut T>) -> *mut pg_sys::List {
    let mut list = PgList::<T>::new();

    for i in vec.into_iter() {
        list.push(i);
    }

    list.into_pg()
}

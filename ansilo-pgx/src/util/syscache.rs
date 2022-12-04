use pgx::pg_sys::{self, Datum, HeapTupleData, Oid};
use std::ops::Deref;

/// A reference to an item in the sys cache
pub struct PgSysCacheItem<'a, T> {
    /// The reference to the item
    item: &'a T,
    /// The pointer to the cache heap tuple, used for free'ing when the item is dropped
    htup: *mut HeapTupleData,
}

impl<'a, T> PgSysCacheItem<'a, T> {
    /// Searches the postgres sys cache and returns the matching cached item if found
    pub fn search<const N: usize>(cache_id: Oid, key: [Datum; N]) -> Option<PgSysCacheItem<'a, T>> {
        if N == 0 || N > 4 {
            panic!("SysCache key length invalid: must be between 1 and 4, {N} given");
        }

        unsafe {
            let htup = match N {
                1 => pg_sys::SearchSysCache1(cache_id as _, key[0]),
                2 => pg_sys::SearchSysCache2(cache_id as _, key[0], key[1]),
                3 => pg_sys::SearchSysCache3(cache_id as _, key[0], key[1], key[2]),
                4 => pg_sys::SearchSysCache4(cache_id as _, key[0], key[1], key[2], key[3]),
                _ => unreachable!(),
            };

            if htup.is_null() {
                return None;
            }

            let item = pg_sys::heap_tuple_get_struct::<T>(htup);

            Some(PgSysCacheItem {
                item: item.as_ref::<'a>().unwrap(),
                htup,
            })
        }
    }
}

impl<'a, T> Deref for PgSysCacheItem<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.item
    }
}

impl<'a, T> Drop for PgSysCacheItem<'a, T> {
    fn drop(&mut self) {
        unsafe {
            pg_sys::ReleaseSysCache(self.htup);
        }
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgx::pg_schema]
mod tests {
    use super::*;
    use pgx::*;

    #[pg_test]
    fn test_util_sys_cache_search_valid() {
        // @see https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_proc.dat

        let func_name = {
            let cached_func = PgSysCacheItem::<pg_sys::FormData_pg_proc>::search(
                pg_sys::SysCacheIdentifier_PROCOID as _,
                [pg_sys::Datum::from(2108)], // oid for SUM(int)
            )
            .unwrap();

            pg_sys::name_data_to_str(&cached_func.proname).to_string()
        };

        assert_eq!(func_name, "sum")
    }

    #[pg_test]
    fn test_util_sys_cache_search_invalid() {
        let cached_func = PgSysCacheItem::<pg_sys::FormData_pg_proc>::search(
            pg_sys::SysCacheIdentifier_PROCOID as _,
            [Datum::from(999999)],
        );

        assert!(cached_func.is_none());
    }
}

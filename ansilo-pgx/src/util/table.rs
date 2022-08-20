use pgx::{
    pg_sys::{self, FormData_pg_attribute, Oid},
    *,
};
use std::ops::Deref;

/// An open table description
pub struct PgTable {
    tab: *mut pg_sys::RelationData,
    lock_mode: pg_sys::LOCKMODE,
}

impl PgTable {
    /// Opens a table description
    pub fn open(relid: Oid, lock_mode: pg_sys::LOCKMODE) -> Option<Self> {
        let tab = unsafe { pg_sys::try_table_open(relid, lock_mode as _) };

        if tab.is_null() {
            return None;
        }

        Some(Self { tab, lock_mode })
    }

    pub fn relid(&self) -> Oid {
        self.rd_id as _
    }

    pub unsafe fn name(&self) -> &str {
        name_data_to_str(&(*self.rd_rel).relname)
    }

    pub fn attrs(&self) -> impl Iterator<Item = &FormData_pg_attribute> {
        let tupdesc = self.rd_att;

        unsafe {
            (*tupdesc)
                .attrs
                .as_slice((*tupdesc).natts as _)
                .iter()
                .filter(|a| !a.is_dropped())
        }
    }
}

impl Deref for PgTable {
    type Target = pg_sys::RelationData;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.tab }
    }
}

impl Drop for PgTable {
    fn drop(&mut self) {
        unsafe {
            pg_sys::table_close(self.tab, self.lock_mode as _);
        }
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

    #[pg_test]
    fn test_util_open_get_table() {
        let oid = Spi::connect(|mut client| {
            client.update(
                r#"CREATE TABLE IF NOT EXISTS "dummy_table" (col INTEGER)"#,
                None,
                None,
            );

            let oid = client
                .select(r#"SELECT '"dummy_table"'::regclass::oid"#, None, None)
                .next()
                .unwrap()
                .by_ordinal(1)
                .unwrap()
                .value::<Oid>()
                .unwrap();

            Ok(Some(oid))
        })
        .unwrap();

        let table = PgTable::open(oid, pg_sys::NoLock as _).unwrap();
        let ref_count = &table.rd_refcnt as *const _;

        assert_eq!(table.relid(), oid);
        assert_eq!(unsafe { *ref_count }, 1);
        assert_eq!(
            table.attrs().map(|a| a.name()).collect::<Vec<_>>(),
            vec!["col"]
        );

        drop(table);

        assert_eq!(unsafe { *ref_count }, 0);
    }

    #[pg_test]
    fn test_util_open_table_invalid() {
        let table = PgTable::open(0, pg_sys::NoLock as _);

        assert!(table.is_none());
    }
}

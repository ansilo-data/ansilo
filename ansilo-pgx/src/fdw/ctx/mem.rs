use pgx::{pg_sys::{PlannerInfo, ScanState}, *};

/// Stores the supplied value in memory managed by postgres.
/// This value is dropped at the end of the top level transaction.
pub(crate) unsafe fn pg_transaction_scoped<T>(v: T) -> PgBox<T, AllocatedByPostgres> {
    pg_scoped(PgMemoryContexts::TopTransactionContext, v)
}

/// Stores the supplied value in memory managed by postgres.
/// This value is dropped at the end of the current query.
pub(crate) unsafe fn pg_query_scoped<T>(
    root: *mut PlannerInfo,
    v: T,
) -> PgBox<T, AllocatedByPostgres> {
    pg_scoped(PgMemoryContexts::For((*root).planner_cxt), v)
}

/// Stores the supplied value in memory managed by postgres.
/// This value is dropped at the end of the current scan query.
pub(crate) unsafe fn pg_scan_scoped<T>(
    scan: *mut ScanState,
    v: T,
) -> PgBox<T, AllocatedByPostgres> {
    pg_scoped(PgMemoryContexts::For((*(*scan).ps.ps_ExprContext).ecxt_per_query_memory), v)
}

/// Stores the supplied value in memory managed by postgres.
/// This value is dropped at the end of the current memory context.
#[allow(unused)]
pub(crate) unsafe fn pg_current_scoped<T>(v: T) -> PgBox<T, AllocatedByPostgres> {
    pg_scoped(PgMemoryContexts::CurrentMemoryContext, v)
}

/// Transfer ownership of the supplied value to postgres.
/// Registers a callback so the value is dropped then the supplied
/// memory context is reset.
pub(crate) unsafe fn pg_scoped<T>(mut memctx: PgMemoryContexts, v: T) -> PgBox<T, AllocatedByPostgres> {
    let ptr = memctx.leak_and_drop_on_delete(v);

    PgBox::from_pg(ptr)
}

use pgx::{pg_sys, *};

/// Retrieves a datum from a tuple
/// Reimplementation of hidden pg symbol:
/// @see https://doxygen.postgresql.org/tuptable_8h.html#a8ee682195a373b7093c67920c3616217
pub unsafe fn slot_get_attr(
    slot: *mut pg_sys::TupleTableSlot,
    att_idx: usize,
) -> (bool, pg_sys::Datum) {
    // Postgres attnum is 1-based
    let attnum = att_idx + 1;

    if attnum > (*slot).tts_nvalid as _ {
        pg_sys::slot_getsomeattrs_int(slot, attnum as _);
    }

    (
        *(*slot).tts_isnull.add(att_idx),
        *(*slot).tts_values.add(att_idx),
    )
}

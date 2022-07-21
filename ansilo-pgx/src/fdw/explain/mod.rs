use std::ffi::CString;
use std::ptr;

use cstr::cstr;
use pgx::pg_sys::{
    ExplainState, ForeignScan, ForeignScanState, List, ModifyTableState, ResultRelInfo,
};
use pgx::*;

use crate::fdw::ctx::*;
use crate::util::string::to_cstr;

#[pg_guard]
pub unsafe extern "C" fn explain_foreign_scan(node: *mut ForeignScanState, es: *mut ExplainState) {
    let plan = (*node).ss.ps.plan as *mut ForeignScan;
    let (ctx, query) = from_fdw_private_rel((*plan).fdw_private);

    let select = query.as_select().unwrap();

    if (*es).verbose {
        explain_json(es, "Query Context", serde_json::to_value(select).unwrap())
    }
}

#[pg_guard]
pub unsafe extern "C" fn explain_foreign_modify(
    mtstate: *mut ModifyTableState,
    rinfo: *mut ResultRelInfo,
    fdw_private: *mut List,
    subplan_index: ::std::os::raw::c_int,
    es: *mut ExplainState,
) {
    unimplemented!()
}

#[pg_guard]
pub unsafe extern "C" fn explain_direct_modify(node: *mut ForeignScanState, es: *mut ExplainState) {
    unimplemented!()
}

/// Output the supplied JSON object to the current ExplainState
unsafe fn explain_json(es: *mut ExplainState, label: &str, json: serde_json::Value) {
    let label = to_cstr(label).unwrap();

    // Helpers
    unsafe fn indent(es: *mut ExplainState) {
        (*es).indent += 1;
    }

    unsafe fn unindent(es: *mut ExplainState) {
        (*es).indent -= 1;
    }

    unsafe fn output_label(es: *mut ExplainState, label: &CString) {
        if label.as_bytes().is_empty() {
            return;
        }

        let empty = cstr!("");
        pg_sys::ExplainPropertyText(label.as_ptr(), empty.as_ptr(), es);
    }

    unsafe fn label_non_empty(label: &CString) -> *const i8 {
        if label.as_bytes().is_empty() {
            ptr::null() as _
        } else {
            label.as_ptr()
        }
    }

    match json {
        serde_json::Value::Null => {
            let null = cstr!("null");
            pg_sys::ExplainPropertyText(label.as_ptr(), null.as_ptr(), es);
        }
        serde_json::Value::Bool(val) => {
            pg_sys::ExplainPropertyBool(label.as_ptr(), val, es);
        }
        serde_json::Value::Number(num) if num.is_u64() => {
            pg_sys::ExplainPropertyUInteger(
                label.as_ptr(),
                ptr::null() as _,
                num.as_u64().unwrap(),
                es,
            );
        }
        serde_json::Value::Number(num) if num.is_i64() => {
            pg_sys::ExplainPropertyInteger(
                label.as_ptr(),
                ptr::null() as _,
                num.as_i64().unwrap(),
                es,
            );
        }
        serde_json::Value::Number(num) => {
            pg_sys::ExplainPropertyFloat(
                label.as_ptr(),
                ptr::null() as _,
                num.as_f64().unwrap(),
                5,
                es,
            );
        }
        serde_json::Value::String(val) => {
            let val = to_cstr(&val).unwrap();
            pg_sys::ExplainPropertyText(label.as_ptr(), val.as_ptr(), es);
        }
        serde_json::Value::Array(arr) => {
            let r#type = cstr!("Array");

            if arr.iter().any(|v| {
                !matches!(
                    v,
                    serde_json::Value::Array(_) | serde_json::Value::Object(_)
                )
            }) {
                pg_sys::ExplainOpenGroup(r#type.as_ptr(), label_non_empty(&label), true, es);
                if (*es).format == pg_sys::ExplainFormat_EXPLAIN_FORMAT_TEXT {
                    output_label(es, &label);
                    indent(es);
                }
                for (i, val) in arr.into_iter().enumerate() {
                    explain_json(es, &format!("{i}"), val)
                }
                if (*es).format == pg_sys::ExplainFormat_EXPLAIN_FORMAT_TEXT {
                    unindent(es);
                }
                pg_sys::ExplainCloseGroup(r#type.as_ptr(), label_non_empty(&label), true, es);
            } else {
                pg_sys::ExplainOpenGroup(r#type.as_ptr(), label_non_empty(&label), false, es);
                if (*es).format == pg_sys::ExplainFormat_EXPLAIN_FORMAT_TEXT {
                    output_label(es, &label);
                    indent(es);
                }
                for val in arr.into_iter() {
                    explain_json(es, "", val)
                }
                if (*es).format == pg_sys::ExplainFormat_EXPLAIN_FORMAT_TEXT {
                    unindent(es);
                }
                pg_sys::ExplainCloseGroup(r#type.as_ptr(), label_non_empty(&label), false, es);
            }
        }
        serde_json::Value::Object(obj) => {
            let r#type = cstr!("Object");

            pg_sys::ExplainOpenGroup(r#type.as_ptr(), label_non_empty(&label), true, es);
            if (*es).format == pg_sys::ExplainFormat_EXPLAIN_FORMAT_TEXT {
                output_label(es, &label);
                indent(es);
            }
            for (key, val) in obj.into_iter() {
                explain_json(es, &key, val)
            }
            if (*es).format == pg_sys::ExplainFormat_EXPLAIN_FORMAT_TEXT {
                unindent(es);
            }
            pg_sys::ExplainCloseGroup(r#type.as_ptr(), label_non_empty(&label), true, es);
        }
    }
}

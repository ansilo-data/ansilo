use std::ffi::{CStr, CString};
use std::ptr;

use ansilo_core::err::Result;
use cstr::cstr;
use pgx::pg_sys::{
    ExplainState, ForeignScan, ForeignScanState, List, ModifyTableState, Node, Plan, PlanState,
    RestrictInfo, ResultRelInfo,
};
use pgx::*;

use crate::fdw::ctx::*;
use crate::util::list::vec_to_pg_list;
use crate::util::string::{parse_to_owned_utf8_string, to_cstr};

#[pg_guard]
pub unsafe extern "C" fn explain_foreign_scan(node: *mut ForeignScanState, es: *mut ExplainState) {
    let plan = (*node).ss.ps.plan as *mut ForeignScan;
    let (ctx, query, _) = from_fdw_private_rel((*plan).fdw_private);

    let select = query.as_select().unwrap();

    if (*es).verbose {
        explain_conds(
            (*node).ss.ps.plan,
            es,
            "Local Conds",
            query.local_conds.clone(),
        );
        explain_conds(
            (*node).ss.ps.plan,
            es,
            "Remote Conds",
            query.remote_conds.clone(),
        );
        explain_json(
            es,
            "Remote Ops",
            serde_json::to_value(select.remote_ops.clone()).unwrap(),
        );
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

/// Deparses the supplied conditions so that they can be shown in the explain query output
unsafe fn explain_conds(
    plan: *mut Plan,
    es: *mut ExplainState,
    label: &'static str,
    conds: Vec<*mut RestrictInfo>,
) {
    // These bindings are not provided by pgx
    extern "C" {
        /// @see https://doxygen.postgresql.org/ruleutils_8c.html#a92d74c070ec1014f3afc3653136b7c3f
        fn set_deparse_context_plan(
            ctx: *mut List,
            planstate: *mut Plan,
            ancestors: *mut List,
        ) -> *mut List;

        /// @see https://doxygen.postgresql.org/ruleutils_8c.html#a15ae01afb23cc9c716378cae7b1ea411
        fn deparse_expression(
            node: *mut Node,
            ctx: *mut List,
            forceprefix: bool,
            showimplicit: bool,
        ) -> *mut i8;
    }

    let label = to_cstr(label).unwrap();
    let ancestors = PgList::<()>::new();
    let context = set_deparse_context_plan((*es).deparse_cxt, plan, ancestors.as_ptr());

    let deparsed = conds
        .into_iter()
        .map(|i| (*i).clause)
        .map(|i| deparse_expression(i as *mut _, context, true, false))
        .collect::<Vec<_>>();
    let deparsed = vec_to_pg_list(deparsed);

    pg_sys::ExplainPropertyList(label.as_ptr(), deparsed, es);
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

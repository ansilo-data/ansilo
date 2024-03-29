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
    pgx::debug1!("Explaining foriegn scan");
    let plan = (*node).ss.ps.plan as *mut ForeignScan;
    let (mut planned_ctx,) = from_fdw_private_plan((*plan).fdw_private);

    // Since this is an EXPLAIN we should be safe to assume we are in the original
    // planning phase
    let mut query = planned_ctx.unsafe_original_planning_ctx();

    // Retrieve explain state from data source
    let remote_query = query.explain((*es).verbose).unwrap();

    explain_json(es, "Remote Query", remote_query);

    // If verbose mode, show the query plan operations
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
            serde_json::to_value(query.as_select().unwrap().remote_ops.clone()).unwrap(),
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
    let (_, mut query, _) = from_fdw_private_modify((*rinfo).ri_FdwState as *mut _);

    explain_modify((*mtstate).ps.plan, es, &mut query);
}

#[pg_guard]
pub unsafe extern "C" fn explain_direct_modify(node: *mut ForeignScanState, es: *mut ExplainState) {
    let plan = (*node).ss.ps.plan as *mut ForeignScan;

    let (mut planned_ctx,) = from_fdw_private_plan((*plan).fdw_private);
    // Since this is an EXPLAIN we should be safe to assume we are in the original
    // planning phase
    let mut query = planned_ctx.unsafe_original_planning_ctx();

    explain_modify(plan as *mut Plan, es, &mut query);
}

unsafe fn explain_modify(plan: *mut Plan, es: *mut ExplainState, query: &mut FdwQueryContext) {
    // Retrieve explain state from data source
    let remote_query = query.explain((*es).verbose).unwrap();

    explain_json(es, "Remote Query", remote_query);

    // If verbose mode, show the remote query plan operations
    if (*es).verbose {
        let remote_ops = match &query.q {
            FdwQueryType::Insert(q) => serde_json::to_value(q.remote_ops.clone()),
            FdwQueryType::BulkInsert(q) => serde_json::to_value(q.remote_ops.clone()),
            FdwQueryType::Update(q) => serde_json::to_value(q.remote_ops.clone()),
            FdwQueryType::Delete(q) => serde_json::to_value(q.remote_ops.clone()),
            _ => panic!(
                "Unexpected query type in explain foreign modify: {:?}",
                query.q
            ),
        }
        .unwrap();

        explain_conds(plan, es, "Remote Conds", query.remote_conds.clone());
        explain_json(es, "Remote Ops", remote_ops);
    }
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

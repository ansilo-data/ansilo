use ansilo_core::{err::Result, sqlil};
use pgx::pg_sys::{self};

use crate::fdw::ctx::FdwContext;

use super::*;

/// RelabelType represents a "dummy" type coercion between two binary-compatible datatypes
pub unsafe fn convert_relabel_type(
    node: *const pg_sys::RelabelType,
    ctx: &mut ConversionContext,
    planner: &PlannerContext,
    fdw: &FdwContext,
) -> Result<sqlil::Expr> {
    // We simply ignore this node
    convert((*node).arg as *mut _, ctx, planner, fdw)
}

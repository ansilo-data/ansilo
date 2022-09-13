use ansilo_core::{err::Result, sqlil};
use pgx::pg_sys::{self};

use crate::fdw::ctx::{FdwContext, PlannerContext};

use super::*;

/// CoerceViaIO represents a type coercion between two types whose textual
/// representations are compatible, implemented by invoking the source type's
/// typoutput function then the destination type's typinput function.
pub(super) unsafe fn convert_coerce_via_io_type(
    node: *const pg_sys::CoerceViaIO,
    ctx: &mut ConversionContext,
    planner: &PlannerContext,
    fdw: &FdwContext,
) -> Result<sqlil::Expr> {
    let expr = convert((*node).arg as *mut _, ctx, planner, fdw)?;
    let result_type = from_pg_type((*node).resulttype)?;

    // If this is a constant value we simply perform the type coercion now
    Ok(if let sqlil::Expr::Constant(constant) = expr {
        sqlil::Expr::Constant(sqlil::Constant::new(
            constant.value.try_coerce_into(&result_type)?,
        ))
    } else {
        // Otherwise we treat it as a cast
        sqlil::Expr::Cast(sqlil::Cast::new(Box::new(expr), result_type))
    })
}

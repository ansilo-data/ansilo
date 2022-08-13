use ansilo_core::{
    err::{bail, Result},
    sqlil,
};
use pgx::pg_sys;

use crate::fdw::ctx::{FdwContext, PlannerContext};

use super::*;

pub(super) unsafe fn convert_case_expr(
    _node: *const pg_sys::CaseExpr,
    _ctx: &mut ConversionContext,
    _planner: &PlannerContext,
    _fdw: &FdwContext,
) -> Result<sqlil::Expr> {
    bail!("Case expressions are not supported")
}

// #[cfg(any(test, feature = "pg_test"))]
// #[pg_schema]
// mod tests {
//     use super::*;
//     use pgx::*;

//     use crate::sqlil::test;

//     #[pg_test]
//     fn test_sqlil_convert_case() {}
// }

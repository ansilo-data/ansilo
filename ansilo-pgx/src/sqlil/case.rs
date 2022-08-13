use ansilo_core::{err::{Result, bail}, sqlil};
use pgx::{*, pg_sys};

use crate::fdw::ctx::{FdwContext, PlannerContext};

use super::*;

pub unsafe fn convert_case_expr(
    node: *const pg_sys::CaseExpr,
    ctx: &mut ConversionContext,
    planner: &PlannerContext,
    fdw: &FdwContext,
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

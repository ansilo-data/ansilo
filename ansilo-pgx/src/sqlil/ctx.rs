use pgx::pg_sys::{PlannerInfo, RelOptInfo, Relids};

use crate::fdw::ctx::FdwContext;

/// Query planner and optimizer context needed to perform conversion of postgres nodes to sqlil
pub struct SqlilContext {
    /// The root query planner info
    pub root: *const PlannerInfo,
    /// The relation currently being processed
    pub rel: *const RelOptInfo,
    /// The id's of the base relations
    pub relids: Relids,
}

impl SqlilContext {
    pub fn new(root: *const PlannerInfo, rel: *const RelOptInfo, relids: Relids) -> Self {
        Self { root, rel, relids }
    }
}

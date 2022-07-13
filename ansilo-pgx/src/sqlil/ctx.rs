use std::collections::HashMap;

use pgx::pg_sys::{PlannerInfo, RelOptInfo, Relids};

use crate::fdw::ctx::FdwContext;

/// Query planner and optimizer context needed to perform conversion of postgres nodes to sqlil
pub struct PlannerContext {
    /// The root query planner info
    pub root: *const PlannerInfo,
    /// The relation currently being processed
    pub rel: *const RelOptInfo,
    /// The id's of the base relations
    pub relids: Relids,
}

impl PlannerContext {
    pub fn new(root: *const PlannerInfo, rel: *const RelOptInfo, relids: Relids) -> Self {
        Self { root, rel, relids }
    }
}

/// Mapping data that is accrued while converting pg expr's to sqlil
#[derive(Debug, Clone, PartialEq)]
pub struct ConversionContext {
    /// Query parameter mappings
    /// Postgres query params (paramkind, paramid) to SQLIL parameter id's
    params: HashMap<(u32, i32), u32>,
}

impl ConversionContext {
    pub fn new() -> Self {
        Self {
            params: HashMap::new(),
        }
    }

    /// Registers or retrieves the existing parameter ID
    pub(crate) fn register_param(&mut self, paramkind: u32, paramid: i32) -> u32 {
        if let Some(id) = self.params.get(&(paramkind, paramid)) {
            *id
        } else {
            let id = (self.params.len() + 1) as u32;
            self.params.insert((paramkind, paramid), id);
            id
        }
    }
}

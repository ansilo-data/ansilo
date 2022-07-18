use std::ffi::c_void;

use pgx::pg_sys::{
    self, JoinPathExtraData, JoinType, Node, PlannerInfo, RelOptInfo, UpperRelationKind,
};

/// Query planner and optimizer context needed to perform conversion of postgres nodes to sqlil
pub enum PlannerContext {
    BaseRel(BaseRelContext),
    JoinRel(JoinRelContext),
    UpperRel(UpperRelContext),
}

impl PlannerContext {
    pub fn base_rel(root: *const PlannerInfo, base_rel: *const RelOptInfo) -> Self {
        Self::BaseRel(BaseRelContext { root, base_rel })
    }

    pub fn join_rel(
        root: *const PlannerInfo,
        join_rel: *const RelOptInfo,
        outer_rel: *const RelOptInfo,
        inner_rel: *const RelOptInfo,
        join_type: JoinType,
        extra: *const JoinPathExtraData,
    ) -> Self {
        Self::JoinRel(JoinRelContext {
            root,
            join_rel,
            outer_rel,
            inner_rel,
            join_type,
            extra,
        })
    }

    pub fn upper_rel(
        root: *const PlannerInfo,
        kind: UpperRelationKind,
        input_rel: *const RelOptInfo,
        output_rel: *const RelOptInfo,
        extra: *const c_void,
    ) -> Self {
        Self::UpperRel(UpperRelContext {
            root,
            kind,
            input_rel,
            output_rel,
            extra,
        })
    }

    pub fn root(&self) -> *const PlannerInfo {
        match self {
            PlannerContext::BaseRel(i) => (*i).root,
            PlannerContext::JoinRel(i) => (*i).root,
            PlannerContext::UpperRel(i) => (*i).root,
        }
    }

    pub fn current_rel(&self) -> *const RelOptInfo {
        match self {
            PlannerContext::BaseRel(i) => (*i).base_rel,
            PlannerContext::JoinRel(i) => (*i).join_rel,
            PlannerContext::UpperRel(i) => (*i).output_rel,
        }
    }

    pub fn as_base_rel(&self) -> Option<&BaseRelContext> {
        if let Self::BaseRel(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub fn as_join_rel(&self) -> Option<&JoinRelContext> {
        if let Self::JoinRel(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub fn as_upper_rel(&self) -> Option<&UpperRelContext> {
        if let Self::UpperRel(v) = self {
            Some(v)
        } else {
            None
        }
    }
}

pub struct BaseRelContext {
    /// The root query planner info
    pub root: *const PlannerInfo,
    /// The relation currently being processed
    pub base_rel: *const RelOptInfo,
}

pub struct JoinRelContext {
    /// The root query planner info
    pub root: *const PlannerInfo,
    /// The relation currently being processed
    pub join_rel: *const RelOptInfo,
    /// The join inner rel
    pub outer_rel: *const RelOptInfo,
    /// The join inner rel
    pub inner_rel: *const RelOptInfo,
    /// The type of join
    pub join_type: JoinType,
    /// The extra join data
    pub extra: *const JoinPathExtraData,
}

pub struct UpperRelContext {
    /// The root query planner info
    pub root: *const PlannerInfo,
    /// The type of the upper relation
    pub kind: UpperRelationKind,
    /// The input rel
    pub input_rel: *const RelOptInfo,
    /// The output rel
    pub output_rel: *const RelOptInfo,
    /// The extra join data
    pub extra: *const c_void,
}

/// Mapping data that is accrued while converting pg expr's to sqlil
#[derive(Debug, Clone, PartialEq)]
pub struct ConversionContext {
    /// Query parameter mappings
    /// Postgres query params (paramkind, paramid) to SQLIL parameter id's
    params: Vec<(*mut Node, u32)>,
}

impl ConversionContext {
    pub fn new() -> Self {
        Self { params: vec![] }
    }

    /// Registers a new param or retrieves the existing param associated to the supplied node
    pub(crate) unsafe fn register_param(&mut self, node: *mut Node) -> u32 {
        if let Some((_, param_id)) = self
            .params
            .iter()
            .find(|(other, _)| pg_sys::equal(*other as _, node as _))
        {
            *param_id
        } else {
            let param_id = (self.params.len() + 1) as u32;
            self.params.push((node, param_id));
            param_id
        }
    }

    pub fn param_nodes(&self) -> Vec<*mut Node> {
        self.params.iter().map(|(i, _)| *i).collect()
    }

    pub unsafe fn param_ids(&self, node: *mut Node) -> Vec<u32> {
        self.params
            .iter()
            .filter(|(n, _)| pg_sys::equal(*n as *mut _ as *const _, node as _))
            .map(|(_, id)| *id)
            .collect()
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgx::pg_schema]
mod tests {
    use pgx::*;

    use super::*;

    #[pg_test]
    fn test_sqlil_ctx_conversion_register_param() {
        let mut node1 = pg_sys::Param {
            xpr: pg_sys::Expr {
                type_: pg_sys::NodeTag_T_Const,
            },
            paramkind: 1,
            paramid: 1,
            paramtype: 1,
            paramtypmod: 1,
            paramcollid: 1,
            location: 1,
        };

        let mut node1_dup = pg_sys::Param {
            xpr: pg_sys::Expr {
                type_: pg_sys::NodeTag_T_Const,
            },
            paramkind: 1,
            paramid: 1,
            paramtype: 1,
            paramtypmod: 1,
            paramcollid: 1,
            location: 1,
        };

        let mut node2 = pg_sys::Param {
            xpr: pg_sys::Expr {
                type_: pg_sys::NodeTag_T_Const,
            },
            paramkind: 1,
            paramid: 2,
            paramtype: 1,
            paramtypmod: 1,
            paramcollid: 1,
            location: 1,
        };

        let mut ctx = ConversionContext::new();

        unsafe {
            let res = ctx.register_param(&mut node1 as *mut _ as *mut pg_sys::Node);
            assert_eq!(res, 1);

            let res = ctx.register_param(&mut node1_dup as *mut _ as *mut pg_sys::Node);
            assert_eq!(res, 1);

            let res = ctx.register_param(&mut node2 as *mut _ as *mut pg_sys::Node);
            assert_eq!(res, 2);

            let res = ctx.register_param(&mut node2 as *mut _ as *mut pg_sys::Node);
            assert_eq!(res, 2);
        }
    }
}

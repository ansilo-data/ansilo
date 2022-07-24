use std::ffi::c_void;

use pgx::pg_sys::{
    self, JoinPathExtraData, JoinType, Node, PlannerInfo, RelOptInfo, UpperRelationKind,
};

use crate::fdw::ctx::from_fdw_private_rel;


/// Query planner and optimizer context needed to perform conversion of postgres nodes to sqlil
#[derive(Clone)]
pub enum PlannerContext {
    BaseRel(BaseRelContext),
    JoinRel(JoinRelContext),
    UpperRel(UpperRelContext),
}

impl PlannerContext {
    pub fn base_rel(root: *mut PlannerInfo, base_rel: *mut RelOptInfo) -> Self {
        Self::BaseRel(BaseRelContext { root, base_rel })
    }

    pub fn join_rel(
        root: *mut PlannerInfo,
        join_rel: *mut RelOptInfo,
        outer_rel: *mut RelOptInfo,
        inner_rel: *mut RelOptInfo,
        join_type: JoinType,
        extra: *mut JoinPathExtraData,
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
        root: *mut PlannerInfo,
        kind: UpperRelationKind,
        input_rel: *mut RelOptInfo,
        output_rel: *mut RelOptInfo,
        extra: *mut c_void,
    ) -> Self {
        Self::UpperRel(UpperRelContext {
            root,
            kind,
            input_rel,
            output_rel,
            extra,
        })
    }

    pub fn root(&self) -> *mut PlannerInfo {
        match self {
            PlannerContext::BaseRel(i) => (*i).root,
            PlannerContext::JoinRel(i) => (*i).root,
            PlannerContext::UpperRel(i) => (*i).root,
        }
    }

    /// Returns the rel representing either the base scan rel
    /// or the upmost join rel if present
    pub unsafe fn get_scan_rel(&self) -> Option<*mut RelOptInfo> {
        let input_rel = match self {
            PlannerContext::BaseRel(i) => return Some((*i).base_rel),
            PlannerContext::JoinRel(i) => return Some((*i).join_rel),
            PlannerContext::UpperRel(i) => (*i).input_rel,
        };

        if [
            pg_sys::RelOptKind_RELOPT_BASEREL,
            pg_sys::RelOptKind_RELOPT_JOINREL,
            pg_sys::RelOptKind_RELOPT_OTHER_JOINREL,
        ]
        .contains(&(*input_rel).reloptkind as _)
        {
            return Some(input_rel);
        }

        if (*input_rel).fdw_private.is_null() {
            return None;
        }

        let (_, _, planner) = from_fdw_private_rel((*input_rel).fdw_private as *mut _);

        planner.get_scan_rel()
    }
}

#[derive(Clone)]
pub struct BaseRelContext {
    /// The root query planner info
    pub root: *mut PlannerInfo,
    /// The relation currently being processed
    pub base_rel: *mut RelOptInfo,
}

#[derive(Clone)]
pub struct JoinRelContext {
    /// The root query planner info
    pub root: *mut PlannerInfo,
    /// The relation currently being processed
    pub join_rel: *mut RelOptInfo,
    /// The join inner rel
    pub outer_rel: *mut RelOptInfo,
    /// The join inner rel
    pub inner_rel: *mut RelOptInfo,
    /// The type of join
    pub join_type: JoinType,
    /// The extra join data
    pub extra: *mut JoinPathExtraData,
}

#[derive(Clone)]
pub struct UpperRelContext {
    /// The root query planner info
    pub root: *mut PlannerInfo,
    /// The type of the upper relation
    pub kind: UpperRelationKind,
    /// The input rel
    pub input_rel: *mut RelOptInfo,
    /// The output rel
    pub output_rel: *mut RelOptInfo,
    /// The extra join data
    pub extra: *mut c_void,
}
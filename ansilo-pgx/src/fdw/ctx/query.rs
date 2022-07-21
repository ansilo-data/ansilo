use std::{collections::HashMap, iter::Chain, slice::Iter};

use ansilo_core::{
    err::{anyhow, Error},
    sqlil,
};
use ansilo_pg::fdw::proto::{OperationCost, RowStructure, SelectQueryOperation, ServerMessage};
use pgx::pg_sys::{self, RestrictInfo};
use serde::{Deserialize, Serialize};

use crate::sqlil::ConversionContext;

/// Query-specific state for the FDW
#[derive(Clone, PartialEq)]
pub struct FdwQueryContext {
    /// The type-specific query state
    pub q: FdwQueryType,
    /// The query cost calculation
    pub cost: OperationCost,
    /// Conditions required to be evaluated locally
    pub local_conds: Vec<*mut RestrictInfo>,
    /// Conditions required to be evaluated remotely
    pub remote_conds: Vec<*mut RestrictInfo>,
    /// The conversion context used to track query parameters
    pub cvt: ConversionContext,
}

impl FdwQueryContext {
    pub fn select() -> Self {
        Self {
            q: FdwQueryType::Select(FdwSelectQuery::default()),
            cost: OperationCost::default(),
            local_conds: vec![],
            remote_conds: vec![],
            cvt: ConversionContext::new(),
        }
    }

    pub fn pushdown_safe(&self) -> bool {
        self.q.pushdown_safe()
    }

    pub fn as_select(&self) -> Option<&FdwSelectQuery> {
        match &self.q {
            FdwQueryType::Select(q) => Some(q),
        }
    }

    pub fn as_select_mut(&mut self) -> Option<&mut FdwSelectQuery> {
        match &mut self.q {
            FdwQueryType::Select(q) => Some(q),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FdwQueryType {
    Select(FdwSelectQuery),
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct FdwSelectQuery {
    /// The conditions which are performed locally (can't be pushed down)
    pub local_ops: Vec<SelectQueryOperation>,
    /// The conditions which are able to be pushed down to the remote
    pub remote_ops: Vec<SelectQueryOperation>,
    /// The current column alias counter
    col_num: u32,
}

impl FdwQueryType {
    fn pushdown_safe(&self) -> bool {
        match self {
            FdwQueryType::Select(q) => q.local_ops.is_empty(),
        }
    }
}

impl FdwSelectQuery {
    pub(crate) fn all_ops(&self) -> Chain<Iter<SelectQueryOperation>, Iter<SelectQueryOperation>> {
        self.remote_ops.iter().chain(self.local_ops.iter())
    }

    pub(crate) fn new_column_alias(&mut self) -> String {
        let num = self.col_num;
        self.col_num += 1;
        format!("c{num}")
    }

    pub(crate) fn new_column(&mut self, expr: sqlil::Expr) -> SelectQueryOperation {
        SelectQueryOperation::AddColumn((self.new_column_alias(), expr))
    }
}

/// Context storage for the FDW stored in the fdw_private field
#[derive(Clone)]
pub struct FdwScanContext {
    /// The prepared query parameter expr's and their type oid's
    /// Each item is keyed by its parameter id
    pub param_exprs: Option<HashMap<u32, (*mut pg_sys::ExprState, pg_sys::Oid)>>,
    /// The resultant row structure after the query has been executed
    pub row_structure: Option<RowStructure>,
}

impl FdwScanContext {
    pub fn new() -> Self {
        Self {
            param_exprs: None,
            row_structure: None,
        }
    }
}

fn unexpected_response(response: ServerMessage) -> Error {
    if let ServerMessage::GenericError(message) = response {
        anyhow!("Error from server: {message}")
    } else {
        anyhow!("Unexpected response {:?}", response)
    }
}

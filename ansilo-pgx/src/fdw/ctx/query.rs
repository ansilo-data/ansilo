use std::{collections::HashMap, rc::Rc};

use ansilo_core::sqlil;
use ansilo_pg::fdw::proto::{
    DeleteQueryOperation, InsertQueryOperation, OperationCost, RowStructure, SelectQueryOperation,
    UpdateQueryOperation,
};
use pgx::pg_sys::{self, RestrictInfo};
use serde::{Deserialize, Serialize};

use crate::sqlil::ConversionContext;

/// Query-specific state for the FDW
#[derive(Clone)]
pub struct FdwQueryContext {
    /// The type-specific query state
    pub q: FdwQueryType,
    /// The base entity size estimation
    pub base_cost: OperationCost,
    /// The base relation relid
    pub base_relid: pg_sys::Oid,
    /// The estimate of the number of rows returned by the query
    /// before any local conditions are checked
    pub retrieved_rows: Option<u64>,
    /// Conditions required to be evaluated locally
    pub local_conds: Vec<*mut RestrictInfo>,
    /// Conditions required to be evaluated remotely
    pub remote_conds: Vec<*mut RestrictInfo>,
    /// The conversion context used to track query parameters
    pub cvt: ConversionContext,
    /// Callbacks used to calculate query costs based on the current path
    pub cost_fns: Vec<Rc<dyn Fn(&Self, OperationCost) -> OperationCost>>,
}

impl FdwQueryContext {
    pub fn new(base_relid: pg_sys::Oid, query: FdwQueryType, base_cost: OperationCost) -> Self {
        let mut cvt = ConversionContext::new();
        cvt.register_alias(base_relid);

        let retrieved_rows = base_cost.rows;

        Self {
            q: query,
            base_cost,
            base_relid,
            retrieved_rows,
            local_conds: vec![],
            remote_conds: vec![],
            cvt,
            cost_fns: vec![],
        }
    }

    pub fn select(base_relid: pg_sys::Oid, base_cost: OperationCost) -> Self {
        Self::new(base_relid, FdwQueryType::Select(FdwSelectQuery::default()), base_cost)
    }

    pub fn insert(base_relid: pg_sys::Oid) -> Self {
        Self::new(base_relid, FdwQueryType::Insert(FdwInsertQuery::default()), OperationCost::default())
    }

    pub fn base_rel_alias(&self) -> &str {
        self.cvt.get_alias(self.base_relid).unwrap()
    }

    pub fn as_select(&self) -> Option<&FdwSelectQuery> {
        match &self.q {
            FdwQueryType::Select(q) => Some(q),
            _ => None
        }
    }

    pub fn as_select_mut(&mut self) -> Option<&mut FdwSelectQuery> {
        match &mut self.q {
            FdwQueryType::Select(q) => Some(q),
            _ => None
        }
    }

    pub fn as_insert(&self) -> Option<&FdwInsertQuery> {
        match &self.q {
            FdwQueryType::Insert(q) => Some(q),
            _ => None
        }
    }

    pub fn as_insert_mut(&mut self) -> Option<&mut FdwInsertQuery> {
        match &mut self.q {
            FdwQueryType::Insert(q) => Some(q),
            _ => None
        }
    }

    pub fn add_cost(&mut self, cb: impl Fn(&Self, OperationCost) -> OperationCost + 'static) {
        self.cost_fns.push(Rc::new(cb));
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FdwQueryType {
    Select(FdwSelectQuery),
    Insert(FdwInsertQuery),
    Update(FdwUpdateQuery),
    Delete(FdwDeleteQuery),
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct FdwSelectQuery {
    /// The operations which are able to be pushed down to the remote
    pub remote_ops: Vec<SelectQueryOperation>,
    /// The current column alias counter
    col_num: u32,
}

impl FdwSelectQuery {
    pub(crate) fn new_column_alias(&mut self) -> String {
        let num = self.col_num;
        self.col_num += 1;
        format!("c{num}")
    }

    pub(crate) fn new_column(&mut self, expr: sqlil::Expr) -> SelectQueryOperation {
        SelectQueryOperation::AddColumn((self.new_column_alias(), expr))
    }
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct FdwInsertQuery {
    /// The operations applied to the insert query
    pub remote_ops: Vec<InsertQueryOperation>,
    /// The list of query parameters and their respective pg type oid's for insert query
    pub params: Vec<(sqlil::Parameter, pg_sys::Oid)>
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct FdwUpdateQuery {
    /// The operations applied to the update query
    pub remote_ops: Vec<UpdateQueryOperation>,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct FdwDeleteQuery {
    /// The operations applied to the delete query
    pub remote_ops: Vec<DeleteQueryOperation>,
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

/// Context storage for the FDW stored in the fdw_private field
#[derive(Clone)]
pub struct FdwModifyContext {
    
}

impl FdwModifyContext {
    pub fn new() -> Self {
        Self {
        }
    }
}
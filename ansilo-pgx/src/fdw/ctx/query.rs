use std::{collections::HashMap, rc::Rc};

use ansilo_core::{data::DataType, sqlil};
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
        Self::new(
            base_relid,
            FdwQueryType::Select(FdwSelectQuery::default()),
            base_cost,
        )
    }

    pub fn insert(base_relid: pg_sys::Oid) -> Self {
        Self::new(
            base_relid,
            FdwQueryType::Insert(FdwInsertQuery::default()),
            OperationCost::default(),
        )
    }

    pub fn update(base_relid: pg_sys::Oid) -> Self {
        Self::new(
            base_relid,
            FdwQueryType::Update(FdwUpdateQuery::default()),
            OperationCost::default(),
        )
    }

    pub fn delete(base_relid: pg_sys::Oid) -> Self {
        Self::new(
            base_relid,
            FdwQueryType::Delete(FdwDeleteQuery::default()),
            OperationCost::default(),
        )
    }

    pub fn base_rel_alias(&self) -> &str {
        self.cvt.get_alias(self.base_relid).unwrap()
    }

    pub fn as_select(&self) -> Option<&FdwSelectQuery> {
        match &self.q {
            FdwQueryType::Select(q) => Some(q),
            _ => None,
        }
    }

    pub fn as_select_mut(&mut self) -> Option<&mut FdwSelectQuery> {
        match &mut self.q {
            FdwQueryType::Select(q) => Some(q),
            _ => None,
        }
    }

    pub fn as_insert(&self) -> Option<&FdwInsertQuery> {
        match &self.q {
            FdwQueryType::Insert(q) => Some(q),
            _ => None,
        }
    }

    pub fn as_insert_mut(&mut self) -> Option<&mut FdwInsertQuery> {
        match &mut self.q {
            FdwQueryType::Insert(q) => Some(q),
            _ => None,
        }
    }

    pub fn as_update(&self) -> Option<&FdwUpdateQuery> {
        match &self.q {
            FdwQueryType::Update(q) => Some(q),
            _ => None,
        }
    }

    pub fn as_update_mut(&mut self) -> Option<&mut FdwUpdateQuery> {
        match &mut self.q {
            FdwQueryType::Update(q) => Some(q),
            _ => None,
        }
    }

    pub fn as_delete(&self) -> Option<&FdwDeleteQuery> {
        match &self.q {
            FdwQueryType::Delete(q) => Some(q),
            _ => None,
        }
    }

    pub fn as_delete_mut(&mut self) -> Option<&mut FdwDeleteQuery> {
        match &mut self.q {
            FdwQueryType::Delete(q) => Some(q),
            _ => None,
        }
    }

    /// Creates a new parameter (not associated to a node)
    pub(crate) fn create_param(&mut self, r#type: DataType) -> sqlil::Parameter {
        sqlil::Parameter::new(r#type, self.cvt.create_param())
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
    /// Mapping of each row's vars to thier resno's in the output
    /// The structure is HashMap<varno, HashMap<varattnum, resno>>
    res_cols: HashMap<u32, HashMap<u32, u32>>,
    /// Mapping of output resno's which refer to whole-rows to the varno
    /// they refer to. The structure is HashMap<resno, varno>
    res_var_nos: HashMap<u32, u32>
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

    pub(crate) unsafe fn record_result_col(&mut self, res_no: u32, var: *mut pg_sys::Var) {
        if !self.res_cols.contains_key(&(*var).varno) {
            self.res_cols.insert((*var).varno, HashMap::new());
        }

        let cols = self.res_cols.get_mut(&(*var).varno).unwrap();
        cols.insert((*var).varattno as _, res_no);
    }

    pub(crate) fn get_result_cols(&self, var_no: u32) -> Option<&HashMap<u32, u32>> {
        self.res_cols.get(&var_no)
    }

    pub(crate) fn record_result_var_no(&mut self, res_no: u32, var_no: u32) {
        self.res_var_nos.insert(res_no, var_no);
    }

    pub(crate) fn get_result_var_no(&self, res_no: u32) -> Option<u32> {
        self.res_var_nos.get(&res_no).cloned()
    }
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct FdwInsertQuery {
    /// The operations applied to the insert query
    pub remote_ops: Vec<InsertQueryOperation>,
    /// The list of query parameters and their respective pg type oid's
    /// which are used to supply the insert row data for the query
    pub params: Vec<(sqlil::Parameter, pg_sys::Oid)>,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct FdwUpdateQuery {
    /// The operations applied to the update query
    pub remote_ops: Vec<UpdateQueryOperation>,
    /// The list of query parameters and their respective attnum's and type oid's
    /// which are used to supply the updated row data for the query
    pub update_params: Vec<(sqlil::Parameter, u32, pg_sys::Oid)>,
    /// The list of query parametersand their respective attnum's and type oid's
    /// which are used to specify the row to update
    pub rowid_params: Vec<(sqlil::Parameter, u32, pg_sys::Oid)>,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct FdwDeleteQuery {
    /// The operations applied to the delete query
    pub remote_ops: Vec<DeleteQueryOperation>,
    /// The list of query parametersand their respective attnum's and type oid's
    /// which are used to specify the row to delete
    pub rowid_params: Vec<(sqlil::Parameter, u32, pg_sys::Oid)>,
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
pub struct FdwModifyContext {}

impl FdwModifyContext {
    pub fn new() -> Self {
        Self {}
    }
}

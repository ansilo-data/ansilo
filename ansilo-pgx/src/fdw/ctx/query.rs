use std::{cmp, collections::HashMap, rc::Rc, sync::Arc};

use ansilo_core::{
    data::{DataType, DataValue},
    err::{anyhow, Context, Error, Result},
    sqlil,
};
use ansilo_pg::fdw::{
    data::{DataWriter, LoggedQuery, QueryHandle, QueryHandleWriter, ResultSet, ResultSetReader},
    proto::{
        BulkInsertQueryOperation, ClientMessage, ClientQueryMessage, DeleteQueryOperation,
        InsertQueryOperation, OperationCost, QueryId, QueryInputStructure, QueryOperation,
        QueryOperationResult, RowStructure, SelectQueryOperation, ServerMessage,
        ServerQueryMessage, UpdateQueryOperation,
    },
};

use itertools::Itertools;
use pgx::{
    pg_sys::{self, RestrictInfo},
    warning,
};
use serde::{Deserialize, Serialize};

use crate::{fdw::common::FdwIpcConnection, sqlil::ConversionContext};

/// Query-specific state for the FDW
pub struct FdwQueryContext {
    /// The type-specific query state
    pub q: FdwQueryType,
    /// The IPC connection
    connection: QueryScopedConnection,
    /// The current query handle writer
    query_writer: Option<QueryHandleWriter<FdwQueryHandle>>,
    /// The current result set reader
    result_set: Option<ResultSetReader<FdwResultSet>>,
    /// The base entity size estimation
    pub base_cost: OperationCost,
    /// The base relation var number
    pub base_varno: pg_sys::Oid,
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

#[derive(Clone)]
struct QueryScopedConnection {
    /// The query id used by the IPC server to identify the query
    pub query_id: QueryId,
    /// The IPC connection
    pub connection: Arc<FdwIpcConnection>,
}

#[derive(Clone)]
pub struct FdwQueryHandle {
    /// The connection to ansilo
    connection: QueryScopedConnection,
    /// The query input structure
    pub query_input: QueryInputStructure,
}

#[derive(Clone)]
pub struct FdwResultSet {
    /// The connection to ansilo
    connection: QueryScopedConnection,
    /// The result set output structure
    pub row_structure: RowStructure,
}

impl FdwQueryContext {
    pub fn new(
        connection: Arc<FdwIpcConnection>,
        query_id: QueryId,
        base_varno: pg_sys::Oid,
        query: FdwQueryType,
        base_cost: OperationCost,
        cvt: ConversionContext,
    ) -> Self {
        let retrieved_rows = base_cost.rows;

        Self {
            connection: QueryScopedConnection::new(query_id, connection),
            q: query,
            query_writer: None,
            result_set: None,
            base_cost,
            base_varno,
            retrieved_rows,
            local_conds: vec![],
            remote_conds: vec![],
            cvt,
            cost_fns: vec![],
        }
    }

    pub fn base_rel_alias(&self) -> &str {
        self.cvt.get_alias(self.base_varno).unwrap()
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

    pub fn as_bulk_insert(&self) -> Option<&FdwBulkInsertQuery> {
        match &self.q {
            FdwQueryType::BulkInsert(q) => Some(q),
            _ => None,
        }
    }

    pub fn as_bulk_insert_mut(&mut self) -> Option<&mut FdwBulkInsertQuery> {
        match &mut self.q {
            FdwQueryType::BulkInsert(q) => Some(q),
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

    /// Apply's the supplied operation to the current state of the query.
    /// Depending on the support of executing the query operation on the data source
    /// this could be rejected, in which case the query operation must be performed
    /// locally by postgres.
    pub fn apply(&mut self, query_op: QueryOperation) -> Result<QueryOperationResult> {
        let result = self
            .connection
            .send(ClientQueryMessage::Apply(query_op))
            .and_then(|res| match res {
                ServerQueryMessage::OperationResult(result) => Ok(result),
                _ => Err(unexpected_response(res)),
            })
            .context("Applying query op")?;

        Ok(result)
    }

    /// Compiles the current query state into a prepared query.
    /// Any required query parameters will have to written before execution.
    pub fn prepare(&mut self) -> Result<QueryInputStructure> {
        let query_input = self
            .connection
            .send(ClientQueryMessage::Prepare)
            .and_then(|res| match res {
                ServerQueryMessage::Prepared(structure) => Ok(structure),
                _ => Err(unexpected_response(res)),
            })
            .context("Preparing query")?;

        self.query_writer = Some(QueryHandleWriter::new(FdwQueryHandle {
            connection: self.connection.clone(),
            query_input: query_input.clone(),
        })?);

        Ok(query_input)
    }

    /// Gets the query input structure expected by the prepared query
    pub fn get_input_structure(&self) -> Result<&QueryInputStructure> {
        self.query_writer
            .as_ref()
            .map(|i| i.get_structure())
            .context("Query not prepared")
    }

    /// Writes the supplied query params
    /// This function assumes that the values are in the order expected by the query input structure
    pub fn write_params(&mut self, data: Vec<DataValue>) -> Result<()> {
        let writer = self.query_writer.as_mut().context("Query not prepared")?;

        // This wont be too inefficient as it is being buffered
        // by an underlying BufWriter
        for val in data.into_iter() {
            writer.write_data_value(val)?;
        }

        Ok(())
    }

    /// Writes the supplied query params
    /// This will ensure the correct ordering of the query parameters by sorting them
    /// using the parameter id's in the supplied vec.
    #[allow(unused)]
    pub fn write_params_unordered(&mut self, data: Vec<(u32, DataValue)>) -> Result<()> {
        let writer = self.query_writer.as_mut().context("Query not prepared")?;
        let mut ordered_params = vec![];
        let mut data = data.into_iter().into_group_map();

        for (param_id, _) in writer.get_structure().params.iter() {
            ordered_params.push(data.get_mut(param_id).unwrap().remove(0));
        }

        self.write_params(ordered_params)
    }

    /// Executes the current query.
    /// All query parameters are expected to have been written.
    pub fn execute(&mut self) -> Result<RowStructure> {
        let writer = self.query_writer.as_mut().context("Query not prepared")?;

        writer.flush()?;
        let result_set = writer.inner_mut().execute()?;
        let row_structure = result_set.row_structure.clone();

        self.result_set = Some(ResultSetReader::new(result_set)?);

        Ok(row_structure)
    }

    /// Reads the next data value from the result set of this query
    pub fn read_result_data(&mut self) -> Result<Option<DataValue>> {
        let reader = self.result_set.as_mut().context("Query not executed")?;

        reader.read_data_value()
    }

    /// Restart's the current query.
    /// Query parameters will have to be rewritten for the next execution.
    pub fn restart_query(&mut self) -> Result<()> {
        let writer = self.query_writer.as_mut().context("Query not executed")?;
        writer.restart()?;

        Ok(())
    }

    /// Performs multiple executions of the query in a single request.
    /// This will not read any result data from the query.
    pub fn execute_batch(&mut self, data: Vec<Vec<(u32, DataValue)>>) -> Result<()> {
        let mut reqs = vec![];
        let structure = self.get_input_structure()?;
        let mut writer = DataWriter::new(std::io::Cursor::new(vec![]), Some(structure.types()));

        for row in data.into_iter() {
            let mut row = row.into_iter().into_group_map();

            for (param_id, _) in structure.params.iter() {
                writer
                    .write_data_value(row.get_mut(param_id).unwrap().remove(0))
                    .unwrap();
            }

            let data = std::mem::replace(writer.inner_mut(), std::io::Cursor::new(vec![]));
            reqs.push(ClientMessage::Query(
                self.connection.query_id,
                ClientQueryMessage::WriteParams(data.into_inner()),
            ));
            reqs.push(ClientMessage::Query(
                self.connection.query_id,
                ClientQueryMessage::Execute,
            ));
            reqs.push(ClientMessage::Query(
                self.connection.query_id,
                ClientQueryMessage::Restart,
            ));

            writer.restart()?;
        }

        let res = self
            .connection
            .connection
            .send(ClientMessage::Batch(reqs))?;

        let results = match res {
            ServerMessage::Batch(res) => res,
            _ => return Err(unexpected_outer_response(res).context("batch execute")),
        };

        for res in results {
            if let ServerMessage::Error(_) = res {
                return Err(unexpected_outer_response(res).context("batch execute"));
            }
        }

        Ok(())
    }

    /// Retrieves any useful debugging information on the execution plan
    /// of the query.
    pub fn explain(&mut self, verbose: bool) -> Result<serde_json::Value> {
        let json: String = self
            .connection
            .send(ClientQueryMessage::Explain(verbose))
            .and_then(|res| match res {
                ServerQueryMessage::Explained(result) => Ok(result),
                _ => Err(unexpected_response(res)),
            })
            .context("Explain query")?;

        let parsed: serde_json::Value = serde_json::from_str(&json)
            .with_context(|| format!("Failed to parse JSON from explain result: {:?}", json))?;

        Ok(parsed)
    }

    /// Gets the maximum batch size for the current query.
    /// This is only supported for insert queries.
    pub fn get_max_batch_size(&mut self) -> Result<u32> {
        let size: u32 = self
            .connection
            .send(ClientQueryMessage::GetMaxBatchSize)
            .and_then(|res| match res {
                ServerQueryMessage::MaxBatchSize(size) => Ok(size),
                _ => Err(unexpected_response(res)),
            })
            .context("Max batch size")?;

        Ok(size)
    }

    /// Creates a copy of the query that can be modified
    /// independently of the original
    pub(crate) fn duplicate(&self) -> Result<Self> {
        let query_id = self
            .connection
            .send(ClientQueryMessage::Duplicate)
            .and_then(|res| match res {
                ServerQueryMessage::Duplicated(query_id) => Ok(query_id),
                _ => return Err(unexpected_response(res)),
            })
            .context("Duplicating query")?;

        Ok(Self {
            q: self.q.clone(),
            connection: QueryScopedConnection::new(
                query_id,
                Arc::clone(&self.connection.connection),
            ),
            query_writer: None,
            result_set: None,
            base_cost: self.base_cost.clone(),
            base_varno: self.base_varno.clone(),
            retrieved_rows: self.retrieved_rows.clone(),
            local_conds: self.local_conds.clone(),
            remote_conds: self.remote_conds.clone(),
            cvt: self.cvt.clone(),
            cost_fns: self.cost_fns.clone(),
        })
    }

    /// Creates a new parameter (not associated to a node)
    pub(crate) fn create_param(&mut self, r#type: DataType) -> sqlil::Parameter {
        sqlil::Parameter::new(r#type, self.cvt.create_param())
    }

    /// Adds a new query cost callback, used to modify the cost of the query
    /// when planning
    pub fn add_cost(&mut self, cb: impl Fn(&Self, OperationCost) -> OperationCost + 'static) {
        self.cost_fns.push(Rc::new(cb));
    }
}

/// Upon dropping the query context we want to ensure
/// the query is dropped on the server side.
impl Drop for FdwQueryContext {
    fn drop(&mut self) {
        let result = self
            .connection
            .send(ClientQueryMessage::Discard)
            .and_then(|res| match res {
                ServerQueryMessage::Discarded => Ok(()),
                _ => return Err(unexpected_response(res)),
            })
            .context("Discarding query");

        if let Err(err) = result {
            warning!(
                "Failed to discard query {} on connection {}: {:?}",
                self.connection.query_id,
                self.connection.connection.data_source_id.clone(),
                err
            )
        }
    }
}

/// TODO[low]: the query handle and result set are agnostic enough to be migrated to ansilo-pg crate
impl QueryHandle for FdwQueryHandle {
    type TResultSet = FdwResultSet;

    fn get_structure(&self) -> Result<QueryInputStructure> {
        Ok(self.query_input.clone())
    }

    fn write(&mut self, buff: &[u8]) -> Result<usize> {
        self.connection
            .send(ClientQueryMessage::WriteParams(buff.to_vec()))
            .and_then(|res| match res {
                ServerQueryMessage::ParamsWritten => Ok(buff.len()),
                _ => return Err(unexpected_response(res)),
            })
            .context("Failed to write query params")
    }

    fn restart(&mut self) -> Result<()> {
        self.connection
            .send(ClientQueryMessage::Restart)
            .and_then(|res| match res {
                ServerQueryMessage::Restarted => Ok(()),
                _ => return Err(unexpected_response(res)),
            })
            .context("Failed to restart query")
    }

    fn execute(&mut self) -> Result<Self::TResultSet> {
        self.connection
            .send(ClientQueryMessage::Execute)
            .and_then(|res| match res {
                ServerQueryMessage::Executed(row_structure) => Ok(FdwResultSet {
                    connection: self.connection.clone(),
                    row_structure,
                }),
                _ => return Err(unexpected_response(res)),
            })
            .context("Failed to execute query")
    }

    fn logged(&self) -> Result<LoggedQuery> {
        unimplemented!()
    }
}

impl ResultSet for FdwResultSet {
    fn get_structure(&self) -> Result<RowStructure> {
        Ok(self.row_structure.clone())
    }

    fn read(&mut self, buff: &mut [u8]) -> Result<usize> {
        self.connection
            .send(ClientQueryMessage::Read(buff.len() as _))
            .and_then(|res| match res {
                ServerQueryMessage::ResultData(data) => {
                    let read = cmp::min(buff.len(), data.len());
                    buff[..read].copy_from_slice(&data[..read]);
                    Ok(read)
                }
                _ => return Err(unexpected_response(res)),
            })
            .context("Failed to read from result set")
    }
}

impl QueryScopedConnection {
    fn new(query_id: QueryId, connection: Arc<FdwIpcConnection>) -> Self {
        Self {
            query_id,
            connection,
        }
    }

    fn send(&self, message: ClientQueryMessage) -> Result<ServerQueryMessage> {
        let res = self
            .connection
            .send(ClientMessage::Query(self.query_id, message))?;

        let res = match res {
            ServerMessage::Query(res) => res,
            _ => return Err(unexpected_outer_response(res)),
        };

        Ok(res)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FdwQueryType {
    Select(FdwSelectQuery),
    Insert(FdwInsertQuery),
    BulkInsert(FdwBulkInsertQuery),
    Update(FdwUpdateQuery),
    Delete(FdwDeleteQuery),
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct FdwSelectQuery {
    /// The operations which are able to be pushed down to the remote
    pub remote_ops: Vec<SelectQueryOperation>,
    /// The current column alias counter
    pub col_num: u32,
    /// Mapping of each row's vars to thier resno's in the output
    /// The structure is HashMap<varno, HashMap<varattnum, resno>>
    res_cols: HashMap<u32, HashMap<u32, u32>>,
    /// Mapping of output resno's which refer to whole-rows to the varno
    /// they refer to. The structure is HashMap<resno, varno>
    res_var_nos: HashMap<u32, u32>,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct FdwInsertQuery {
    /// The operations applied to the insert query
    pub remote_ops: Vec<InsertQueryOperation>,
    /// The relation id of the table being inserted to
    pub relid: u32,
    /// The columns being inserted
    pub inserted_cols: Vec<u32>,
    /// The list of query parameters and their respective attnum's and type oid's
    /// which are used to supply the insert row data for the query
    pub params: Vec<(sqlil::Parameter, u32, pg_sys::Oid)>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct FdwBulkInsertQuery {
    /// The operations applied to the bulk insert query
    pub remote_ops: Vec<BulkInsertQueryOperation>,
    /// The list of query parameters and their respective attnum's and type oid's
    /// which are used to supply the insert row data for the query
    pub params: Vec<(sqlil::Parameter, u32, pg_sys::Oid)>,
    /// The number of rows inserted in a single query
    pub batch_size: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct FdwBulkInsertQueryExplainSummary {
    pub cols: Vec<(String, sqlil::Expr)>,
    pub batch_size: u32,
}

impl FdwBulkInsertQuery {
    pub fn summary(&self) -> FdwBulkInsertQueryExplainSummary {
        let cols = self
            .remote_ops
            .iter()
            .find_map(|op| op.as_set_bulk_rows().clone())
            .map(|(cols, params)| {
                cols.iter()
                    .cloned()
                    .zip(params.iter().cloned())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        FdwBulkInsertQueryExplainSummary {
            cols,
            batch_size: self.batch_size,
        }
    }
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
pub struct FdwModifyContext {
    /// The context for the inner scan
    pub scan: FdwScanContext,
    /// Base insert query context used for resizing bulk inserts
    pub singular_insert: Option<FdwQueryContext>,
    /// Whether this is an EXPLAIN only query
    pub explain_only: bool,
}

impl FdwModifyContext {
    pub fn new() -> Self {
        Self {
            scan: FdwScanContext::new(),
            singular_insert: None,
            explain_only: false,
        }
    }
}

fn unexpected_outer_response(response: ServerMessage) -> Error {
    if let ServerMessage::Error(message) = response {
        anyhow!("Error from server: {message}")
    } else {
        anyhow!("Unexpected response {:?}", response)
    }
}

fn unexpected_response(response: ServerQueryMessage) -> Error {
    anyhow!("Unexpected response {:?}", response)
}

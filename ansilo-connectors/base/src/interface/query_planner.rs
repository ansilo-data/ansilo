use ansilo_core::{
    err::Result,
    sqlil::{self as sql}, data::DataType,
};
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use crate::common::entity::{ConnectorEntityConfig, EntitySource};

use super::Connection;

/// The query planner determines if SQLIL queries can be executed remotely
pub trait QueryPlanner {
    type TConnection: Connection;
    type TQuery;
    type TEntitySourceConfig: Clone;

    /// Gets an estimate of the number of rows for the entity
    fn estimate_size(
        connection: &mut Self::TConnection,
        entity: &EntitySource<Self::TEntitySourceConfig>,
    ) -> Result<OperationCost>;

    /// Gets expressions for the primary key/row ID of the supplied entity
    /// This is called for performing updates/deletes to existing rows.
    fn get_row_id_exprs(
        connection: &mut Self::TConnection,
        conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        entity: &EntitySource<Self::TEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<Vec<(sql::Expr, DataType)>>;

    /// Creates a query of the specified type
    fn create_base_query(
        connection: &mut Self::TConnection,
        conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        entity: &EntitySource<Self::TEntitySourceConfig>,
        source: &sql::EntitySource,
        r#type: sql::QueryType,
    ) -> Result<(OperationCost, sql::Query)> {
        match r#type {
            sql::QueryType::Select => Self::create_base_select(connection, conf, entity, source)
                .map(|(op, q)| (op, q.into())),
            sql::QueryType::Insert => Self::create_base_insert(connection, conf, entity, source)
                .map(|(op, q)| (op, q.into())),
            sql::QueryType::Update => Self::create_base_update(connection, conf, entity, source)
                .map(|(op, q)| (op, q.into())),
            sql::QueryType::Delete => Self::create_base_delete(connection, conf, entity, source)
                .map(|(op, q)| (op, q.into())),
        }
    }

    /// Creates a base query to select all rows of the entity
    fn create_base_select(
        connection: &mut Self::TConnection,
        conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        entity: &EntitySource<Self::TEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Select)>;

    /// Creates a base update query to update all rows of the entity
    fn create_base_insert(
        connection: &mut Self::TConnection,
        conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        entity: &EntitySource<Self::TEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Insert)>;

    /// Creates a base update query to update all rows of the entity
    fn create_base_update(
        connection: &mut Self::TConnection,
        conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        entity: &EntitySource<Self::TEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Update)>;

    /// Creates a base delete query to delete all rows of the entity
    fn create_base_delete(
        connection: &mut Self::TConnection,
        conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        entity: &EntitySource<Self::TEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Delete)>;

    /// Adds the supplied operation to the select query
    fn apply_select_operation(
        connection: &mut Self::TConnection,
        conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        select: &mut sql::Select,
        op: SelectQueryOperation,
    ) -> Result<QueryOperationResult>;

    /// Adds the supplied operation to the select query
    fn apply_insert_operation(
        connection: &mut Self::TConnection,
        conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        insert: &mut sql::Insert,
        op: InsertQueryOperation,
    ) -> Result<QueryOperationResult>;

    /// Adds the supplied operation to the update query
    fn apply_update_operation(
        connection: &mut Self::TConnection,
        conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        update: &mut sql::Update,
        op: UpdateQueryOperation,
    ) -> Result<QueryOperationResult>;

    /// Adds the supplied operation to the delete query
    fn apply_delete_operation(
        connection: &mut Self::TConnection,
        conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        delete: &mut sql::Delete,
        op: DeleteQueryOperation,
    ) -> Result<QueryOperationResult>;

    /// Returns a JSON representation of the query state used for debugging
    fn explain_query(
        connection: &mut Self::TConnection,
        conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        query: &sql::Query,
        verbose: bool,
    ) -> Result<serde_json::Value>;
}

/// An operation to apply to the current state of a query
#[derive(Debug, PartialEq, Clone, Encode, Decode, Serialize, Deserialize)]
pub enum QueryOperation {
    Select(SelectQueryOperation),
    Insert(InsertQueryOperation),
    Update(UpdateQueryOperation),
    Delete(DeleteQueryOperation),
}

impl From<SelectQueryOperation> for QueryOperation {
    fn from(op: SelectQueryOperation) -> Self {
        Self::Select(op)
    }
}

impl From<InsertQueryOperation> for QueryOperation {
    fn from(op: InsertQueryOperation) -> Self {
        Self::Insert(op)
    }
}

impl From<UpdateQueryOperation> for QueryOperation {
    fn from(op: UpdateQueryOperation) -> Self {
        Self::Update(op)
    }
}

impl From<DeleteQueryOperation> for QueryOperation {
    fn from(op: DeleteQueryOperation) -> Self {
        Self::Delete(op)
    }
}

/// A cost estimate for a query operation
#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub enum QueryOperationResult {
    Ok(OperationCost),
    Unsupported,
}

/// Select planning operations
#[derive(Debug, PartialEq, Clone, Encode, Decode, Serialize, Deserialize)]
pub enum SelectQueryOperation {
    AddColumn((String, sql::Expr)),
    AddWhere(sql::Expr),
    AddJoin(sql::Join),
    AddGroupBy(sql::Expr),
    AddOrderBy(sql::Ordering),
    SetRowLimit(u64),
    SetRowOffset(u64),
    SetRowLockMode(sql::SelectRowLockMode),
}

impl SelectQueryOperation {
    /// Returns `true` if the select query operation is [`AddColumn`].
    ///
    /// [`AddColumn`]: SelectQueryOperation::AddColumn
    #[must_use]
    pub fn is_add_column(&self) -> bool {
        matches!(self, Self::AddColumn(..))
    }

    /// Returns `true` if the select query operation is [`AddWhere`].
    ///
    /// [`AddWhere`]: SelectQueryOperation::AddWhere
    #[must_use]
    pub fn is_add_where(&self) -> bool {
        matches!(self, Self::AddWhere(..))
    }

    /// Returns `true` if the select query operation is [`AddJoin`].
    ///
    /// [`AddJoin`]: SelectQueryOperation::AddJoin
    #[must_use]
    pub fn is_add_join(&self) -> bool {
        matches!(self, Self::AddJoin(..))
    }

    /// Returns `true` if the select query operation is [`AddGroupBy`].
    ///
    /// [`AddGroupBy`]: SelectQueryOperation::AddGroupBy
    #[must_use]
    pub fn is_add_group_by(&self) -> bool {
        matches!(self, Self::AddGroupBy(..))
    }

    /// Returns `true` if the select query operation is [`AddOrderBy`].
    ///
    /// [`AddOrderBy`]: SelectQueryOperation::AddOrderBy
    #[must_use]
    pub fn is_add_order_by(&self) -> bool {
        matches!(self, Self::AddOrderBy(..))
    }

    /// Returns `true` if the select query operation is [`SetRowLimit`].
    ///
    /// [`SetRowLimit`]: SelectQueryOperation::SetRowLimit
    #[must_use]
    pub fn is_set_row_limit(&self) -> bool {
        matches!(self, Self::SetRowLimit(..))
    }

    /// Returns `true` if the select query operation is [`SetRowOffset`].
    ///
    /// [`SetRowOffset`]: SelectQueryOperation::SetRowOffset
    #[must_use]
    pub fn is_set_row_offset(&self) -> bool {
        matches!(self, Self::SetRowOffset(..))
    }

    /// Returns `true` if the select query operation is [`SetRowLockMode`].
    ///
    /// [`SetRowLockMode`]: SelectQueryOperation::SetRowLockMode
    #[must_use]
    pub fn is_set_row_lock_mode(&self) -> bool {
        matches!(self, Self::SetRowLockMode(..))
    }
}

/// Insert planning operations
#[derive(Debug, PartialEq, Clone, Encode, Decode, Serialize, Deserialize)]
pub enum InsertQueryOperation {
    AddColumn((String, sql::Expr)),
}

impl InsertQueryOperation {
    /// Returns `true` if the insert query operation is [`AddColumn`].
    ///
    /// [`AddColumn`]: InsertQueryOperation::AddColumn
    #[must_use]
    pub fn is_add_column(&self) -> bool {
        matches!(self, Self::AddColumn(..))
    }
}

/// Update planning operations
#[derive(Debug, PartialEq, Clone, Encode, Decode, Serialize, Deserialize)]
pub enum UpdateQueryOperation {
    AddSet((String, sql::Expr)),
    AddWhere(sql::Expr),
}

impl UpdateQueryOperation {
    /// Returns `true` if the update query operation is [`AddSet`].
    ///
    /// [`AddSet`]: UpdateQueryOperation::AddSet
    #[must_use]
    pub fn is_add_set(&self) -> bool {
        matches!(self, Self::AddSet(..))
    }

    /// Returns `true` if the update query operation is [`AddWhere`].
    ///
    /// [`AddWhere`]: UpdateQueryOperation::AddWhere
    #[must_use]
    pub fn is_add_where(&self) -> bool {
        matches!(self, Self::AddWhere(..))
    }
}

/// Delete planning operations
#[derive(Debug, PartialEq, Clone, Encode, Decode, Serialize, Deserialize)]
pub enum DeleteQueryOperation {
    AddWhere(sql::Expr),
}

impl DeleteQueryOperation {
    /// Returns `true` if the delete query operation is [`AddWhere`].
    ///
    /// [`AddWhere`]: DeleteQueryOperation::AddWhere
    #[must_use]
    pub fn is_add_where(&self) -> bool {
        matches!(self, Self::AddWhere(..))
    }
}

/// A cost estimate for a query operation
#[derive(Debug, Default, Clone, PartialEq, Encode, Decode)]
pub struct OperationCost {
    /// The estimated number of rows
    pub rows: Option<u64>,
    /// The estimated average width of each row in bytes
    pub row_width: Option<u32>,
    /// The relative cost factor of opening the connection for this operation
    pub startup_cost: Option<f64>,
    /// The relative cost factor of performing the operation
    pub total_cost: Option<f64>,
}

impl OperationCost {
    pub fn new(
        rows: Option<u64>,
        row_width: Option<u32>,
        startup_cost: Option<f64>,
        total_cost: Option<f64>,
    ) -> Self {
        Self {
            rows,
            row_width,
            startup_cost,
            total_cost,
        }
    }

    pub fn default_to(&mut self, default: &Self) {
        self.rows = self.rows.or(default.rows);
        self.row_width = self.row_width.or(default.row_width);
        self.startup_cost = self.startup_cost.or(default.startup_cost);
        self.total_cost = self.total_cost.or(default.total_cost);
    }
}

use ansilo_core::{err::Result, sqlil as sql};
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
        connection: &Self::TConnection,
        entity: &EntitySource<Self::TEntitySourceConfig>,
    ) -> Result<OperationCost>;

    /// Creates a base query to select all rows of the entity
    fn create_base_select(
        connection: &Self::TConnection,
        conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Select)>;

    /// Creates a base update query to update all rows of the entity
    fn create_base_insert(
        connection: &Self::TConnection,
        conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Insert)>;

    /// Creates a base update query to update all rows of the entity
    fn create_base_update(
        connection: &Self::TConnection,
        conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Update)>;

    /// Creates a base delete query to delete all rows of the entity
    fn create_base_delete(
        connection: &Self::TConnection,
        conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Delete)>;

    /// Adds the supplied operation to the select query
    fn apply_select_operation(
        connection: &Self::TConnection,
        conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        select: &mut sql::Select,
        op: SelectQueryOperation,
    ) -> Result<QueryOperationResult>;

    /// Adds the supplied operation to the select query
    fn apply_insert_operation(
        connection: &Self::TConnection,
        conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        insert: &mut sql::Insert,
        op: InsertQueryOperation,
    ) -> Result<QueryOperationResult>;

    /// Adds the supplied operation to the update query
    fn apply_update_operation(
        connection: &Self::TConnection,
        conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        update: &mut sql::Update,
        op: UpdateQueryOperation,
    ) -> Result<QueryOperationResult>;

    /// Adds the supplied operation to the delete query
    fn apply_delete_operation(
        connection: &Self::TConnection,
        conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        delete: &mut sql::Delete,
        op: DeleteQueryOperation,
    ) -> Result<QueryOperationResult>;

    /// Returns a JSON representation of the query state used for debugging
    fn explain_query(
        connection: &Self::TConnection,
        conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        query: &sql::Query,
        verbose: bool,
    ) -> Result<serde_json::Value>;
}

/// A cost estimate for a query operation
#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub enum QueryOperationResult {
    PerformedRemotely(OperationCost),
    PerformedLocally,
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
}

/// Insert planning operations
#[derive(Debug, PartialEq, Clone, Encode, Decode, Serialize, Deserialize)]
pub enum InsertQueryOperation {
    AddColumn((String, sql::Expr)),
}

/// Update planning operations
#[derive(Debug, PartialEq, Clone, Encode, Decode, Serialize, Deserialize)]
pub enum UpdateQueryOperation {
    AddSet((String, sql::Expr)),
    AddWhere(sql::Expr),
    AddOrderBy(sql::Ordering),
    SetRowLimit(u64),
    SetRowOffset(u64),
}

/// Delete planning operations
#[derive(Debug, PartialEq, Clone, Encode, Decode, Serialize, Deserialize)]
pub enum DeleteQueryOperation {
    AddWhere(sql::Expr),
    AddOrderBy(sql::Ordering),
    SetRowLimit(u64),
    SetRowOffset(u64),
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

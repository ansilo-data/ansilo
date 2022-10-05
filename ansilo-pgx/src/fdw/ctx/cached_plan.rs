use std::sync::{Arc, Weak};

use ansilo_core::sqlil::EntityId;
use ansilo_pg::fdw::proto::{OperationCost, QueryId};
use pgx::{pg_sys, AllocatedByPostgres, PgBox, PgList};

use crate::{
    fdw::common::{self, FdwIpcConnection, TableOptions},
    sqlil::{ConversionContext, PlannedConversionContext},
};

use super::{FdwContext, FdwQueryContext, FdwQueryType};

/// Query-specific state for the FDW that we store in our plans
/// for query execution.
pub struct FdwPlanContext {
    /// The type-specific query state
    q: FdwQueryType,
    /// Entity id
    entity: EntityId,
    /// Foreign table oid
    foreign_table_oid: pg_sys::Oid,
    /// Foreign table opt
    foreign_table_opts: TableOptions,
    /// The conversion context used to track query parameters
    cvt: PlannedConversionContext,
    /// A weak reference to the the original IPC connection used to plan this query.
    /// It is common for plan's to be "oneshot" so we optimise for the path
    /// where a query is planned and executed in the same transaction
    orig_con: Weak<FdwIpcConnection>,
    /// The original query id used to plan this query
    orig_query_id: QueryId,
    /// Similarly some functions that get called after original foreign scan
    /// has finished planning (explain, direct modifications etc)
    /// In this case we need the reference to the original query context
    /// since that has all the planning info
    /// We have to be careful when referencing this as it could be dropped
    /// in the case of cached plans, so it must only be used when we know
    /// that we are still in the original planning phase.
    orig_ctx: PgBox<FdwQueryContext, AllocatedByPostgres>,
}

impl FdwPlanContext {
    /// Create a new plan context from the supplied connection and query context
    pub fn create(
        ctx: &FdwContext,
        query: PgBox<FdwQueryContext, AllocatedByPostgres>,
    ) -> (Self, Vec<*mut pg_sys::Node>) {
        let (cvt, fdw_exprs) = unsafe { query.cvt.to_planned() };

        let plan = Self {
            q: query.q.clone(),
            entity: ctx.entity.clone(),
            foreign_table_oid: ctx.foreign_table_oid,
            foreign_table_opts: ctx.foreign_table_opts.clone(),
            cvt,
            orig_con: Arc::downgrade(&ctx.connection),
            orig_query_id: query.query_id(),
            orig_ctx: query,
        };

        (plan, fdw_exprs)
    }

    /// Restores the query context from this planned context
    pub fn restore(
        &self,
        fdw_exprs: Option<PgList<pg_sys::Node>>,
    ) -> (FdwContext, FdwQueryContext) {
        let cvt = if let Some(fdw_exprs) = fdw_exprs {
            ConversionContext::from_planned(&self.cvt, fdw_exprs)
        } else {
            ConversionContext::new()
        };

        // Fast path: if we are in the same transaction that was used for planning
        // the original IPC connection should still be valid.
        if let Some(con) = self.orig_con.upgrade() {
            let ctx = FdwContext::new(
                Arc::clone(&con),
                self.entity.clone(),
                self.foreign_table_oid,
                self.foreign_table_opts.clone(),
            );

            // Some of these values are unused during execution.
            // We should probably split this out into another struct at some point
            pgx::debug1!("Restored query state with IPC connection from planned state");
            let mut query = FdwQueryContext::new(
                con,
                self.orig_query_id,
                0, // unused
                self.q.clone(),
                OperationCost::default(), // unused
                cvt,
            );
            // Since the original query context is still alive we dont want to drop
            // the query prematurely while it can still be reused
            query.should_discard = false;

            return (ctx, query);
        }

        pgx::debug1!("Could not restore query state, building a new query");
        let mut ctx = unsafe { common::connect_table(self.foreign_table_oid) };

        let mut query = ctx
            .create_query(
                0, // unused
                self.q.r#type(),
            )
            .unwrap();

        for op in self.q.get_remote_ops() {
            query.apply(op).unwrap();
        }

        query.cvt = cvt;

        (ctx, query)
    }

    /// Returns the original query context.
    /// SAFETY: This is only valid when we are still in the original planning context.
    pub unsafe fn unsafe_original_planning_ctx(&mut self) -> &mut FdwQueryContext {
        &mut self.orig_ctx
    }
}

use std::{collections::HashMap, ffi::c_void, mem, ops::ControlFlow, ptr};

use ansilo_core::{
    common::data::DataValue,
    err::{bail, Result},
    sqlil::{self, JoinType, Ordering, OrderingType},
};
use ansilo_pg::fdw::proto::{
    ClientMessage, ClientSelectMessage, OperationCost, QueryOperationResult, SelectQueryOperation,
    ServerMessage, ServerSelectMessage,
};
use pgx::{
    pg_sys::{
        add_path, shm_toc, EquivalenceClass, EquivalenceMember, ForeignPath, ForeignScan,
        ForeignScanState, JoinPathExtraData, List, Node, Oid, ParallelContext, Path, PathKey, Plan,
        PlannerInfo, RangeTblEntry, RelOptInfo, RestrictInfo, Size, TupleTableSlot,
        UpperRelationKind,
    },
    *,
};

use crate::sqlil::{
    convert, convert_list, from_datum, parse_entity_version_id_from_foreign_table,
    parse_entity_version_id_from_rel, ConversionContext, PlannerContext,
};

use super::{
    common,
    ctx::{FdwContext, FdwQueryContext, FdwSelectQuery},
};

macro_rules! unexpected_response {
    ($res:expr) => {
        error!("Unexpected response from server: {:?}", $res)
    };
}

/// Default cost values in case they cant be estimated
/// Values borroed from
/// @see https://doxygen.postgresql.org/postgres__fdw_8c_source.html#l03570
const DEFAULT_FDW_STARTUP_COST: u64 = 100;
const DEFAULT_FDW_TUPLE_COST: f64 = 0.01;

/// We want to be pessimistict about the number of rows in tables
/// to avoid overly selective query plans
const DEFAULT_ROW_VOLUME: u64 = 100_000;

/// Estimate # of rows and width of the result of the scan
///
/// We should consider the effect of all baserestrictinfo clauses here, but
/// not any join clauses.
pub unsafe extern "C" fn get_foreign_rel_size(
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    foreigntableid: Oid,
) {
    let mut ctx = common::connect(foreigntableid);
    let planner = PlannerContext::base_rel(root, baserel);

    let baserel_conds = PgList::<RestrictInfo>::from_pg((*baserel).baserestrictinfo);

    let entity = parse_entity_version_id_from_foreign_table(foreigntableid).unwrap();

    // If no conditions we can use the cheap path
    let entity = ctx.entity.clone();
    let res = ctx.send(ClientMessage::EstimateSize(entity)).unwrap();

    let mut base_cost = match res {
        ServerMessage::EstimatedSizeResult(e) => e,
        _ => unexpected_response!(res),
    };

    // We have to evaluate the possibility and costs of pushing down the restriction clauses
    let mut query = FdwQueryContext::select();
    let conds = baserel_conds
        .iter_ptr()
        .filter_map(|i| {
            let expr = convert((*i).clause as *const _, &mut query.cvt, &planner, &*ctx).ok();

            // Store conditions requiring local evaluation for later
            if expr.is_none() {
                query.local_conds.push(i);
            }

            expr
        })
        .map(|i| SelectQueryOperation::AddWhere(i))
        .collect::<Vec<_>>();

    let mut base_query = estimate_path_cost(&mut ctx, &planner, query, conds);

    // Default to base cost
    {
        let cost = &mut base_query.cost;
        cost.rows = cost.rows.or(base_cost.rows).or(Some(DEFAULT_ROW_VOLUME));
        cost.row_width = cost.row_width.or(base_cost.row_width);
        cost.connection_cost = cost
            .connection_cost
            .or(base_cost.connection_cost)
            .or(Some(DEFAULT_FDW_STARTUP_COST));
        cost.total_cost = cost.total_cost.or(base_cost.total_cost);
    }

    if let Some(rows) = base_query.cost.rows {
        (*baserel).rows = rows as _;
    }

    if let Some(row_width) = base_query.cost.row_width {
        (*(*baserel).reltarget).width = row_width as _;
    }

    (*baserel).fdw_private = into_fdw_private(ctx, base_query) as *mut _;
}

/// Create possible scan paths for a scan on the foreign table
///
/// @see https://doxygen.postgresql.org/postgres__fdw_8c.html#a5e0a23f5638e9b82a7e8c6c5be3389a2
pub unsafe extern "C" fn get_foreign_paths(
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    foreigntableid: Oid,
) {
    let (mut ctx, base_query) = from_fdw_private((*baserel).fdw_private as *mut _);
    let base_cost = base_query.cost.clone();
    let planner = PlannerContext::base_rel(root, baserel);

    // Create a default full-scan path for the rel
    let path = pg_sys::create_foreignscan_path(
        root,
        baserel,
        ptr::null_mut(),
        base_cost.rows.unwrap() as f64,
        base_cost.connection_cost.unwrap() as f64,
        base_cost.total_cost.unwrap() as f64,
        ptr::null_mut(),
        (*baserel).lateral_relids,
        ptr::null_mut(),
        into_fdw_private_path(PgBox::new(base_query.clone()).into_pg_boxed()),
    );
    add_path(baserel, path as *mut pg_sys::Path);

    // Generate parameterised paths for nested-loop joins
    // with few rows on the outer rel
    let mut join_restrictions = PgList::<RestrictInfo>::from_pg((*baserel).joininfo)
        .iter_ptr()
        .collect::<Vec<_>>();

    if (*baserel).has_eclass_joins {
        /* Callback argument for ec_member_matches_foreign */
        #[repr(C)]
        struct EcMemberCallback {
            current: *mut pg_sys::Expr, /* current expr, or NULL if not yet found */
            already_used: *mut pg_sys::List, /* expressions already dealt with */
        }

        unsafe extern "C" fn member_matches_foreign_cb(
            root: *mut PlannerInfo,
            rel: *mut RelOptInfo,
            ec: *mut EquivalenceClass,
            em: *mut EquivalenceMember,
            arg: *mut ::std::os::raw::c_void,
        ) -> bool {
            let arg: *mut EcMemberCallback = arg as *mut _;
            let expr = (*em).em_expr;

            if !(*arg).current.is_null() {
                return pg_sys::equal(expr as _, (*arg).current as _);
            }

            if pg_sys::list_member((*arg).already_used, expr as _) {
                return false;
            }

            (*arg).current = expr;
            return true;
        }

        let mut arg: EcMemberCallback = mem::zeroed();

        loop {
            arg.current = ptr::null_mut();

            let clauses = pg_sys::generate_implied_equalities_for_column(
                root,
                baserel,
                Some(member_matches_foreign_cb),
                &mut arg as *mut EcMemberCallback as *mut _,
                (*baserel).lateral_referencers,
            );

            if arg.current.is_null() {
                break;
            }

            let clauses = PgList::<RestrictInfo>::from_pg(clauses);
            join_restrictions.append(&mut clauses.iter_ptr().collect());

            arg.already_used = pg_sys::lappend(arg.already_used, arg.current as _);
        }
    }

    let mut cvt = ConversionContext::new();
    let param_paths = join_restrictions
        .into_iter()
        .filter(|i| pg_sys::join_clause_is_movable_to(*i, baserel))
        .filter(|i| convert((**i).clause as *mut Node, &mut cvt, &planner, &ctx).is_ok())
        .filter_map(|i| {
            let required_outer = pg_sys::bms_union((*i).clause_relids, (*baserel).lateral_relids);
            let required_outer = pg_sys::bms_del_member(required_outer, (*baserel).relid as _);

            if pg_sys::bms_is_empty(required_outer) {
                None
            } else {
                Some(required_outer)
            }
        })
        .map(|i| pg_sys::get_baserel_parampathinfo(root, baserel, i))
        .collect::<Vec<_>>();

    // Create a path for each parameterised path option
    for ppi in param_paths.into_iter() {
        let mut query = base_query.clone();

        let ops = convert_list((*ppi).ppi_clauses, &mut query.cvt, &planner, &ctx)
            .unwrap()
            .into_iter()
            .map(|i| SelectQueryOperation::AddWhere(i))
            .collect::<Vec<_>>();

        let query = estimate_path_cost(&mut ctx, &planner, query, ops);

        let path = pg_sys::create_foreignscan_path(
            root,
            baserel,
            ptr::null_mut(),
            query.cost.rows.unwrap() as f64,
            query.cost.connection_cost.unwrap() as f64,
            query.cost.total_cost.unwrap() as f64,
            ptr::null_mut(),
            (*ppi).ppi_req_outer,
            ptr::null_mut(),
            into_fdw_private_path(query),
        );
        add_path(baserel, path as *mut pg_sys::Path);
    }

    // TODO: explore value of exploiting query_pathkeys
}

/// Add possible ForeignPath to joinrel, if join is safe to push down.
///
/// @see https://doxygen.postgresql.org/postgres__fdw_8c.html#a37cae9c397f76945ef22779c7c566002
pub unsafe extern "C" fn get_foreign_join_paths(
    root: *mut PlannerInfo,
    joinrel: *mut RelOptInfo,
    outerrel: *mut RelOptInfo,
    innerrel: *mut RelOptInfo,
    jointype: pg_sys::JoinType,
    extra: *mut JoinPathExtraData,
) {
    // This joinrel has already been processed
    if !(*joinrel).fdw_private.is_null() {
        return;
    }

    // We do not support lateral references
    if !pg_sys::bms_is_empty((*joinrel).lateral_relids) {
        return;
    }

    // If there is a possibility that EvalPlanQual will be executed, we need
    // to be able to reconstruct the row using scans of the base relations.
    // GetExistingLocalJoinPath will find a suitable path for this purpose in
    // the path list of the joinrel, if one exists.  We must be careful to
    // call it before adding any ForeignPath, since the ForeignPath might
    // dominate the only suitable local path available.  We also do it before
    // calling foreign_join_ok(), since that function updates fpinfo and marks
    // it as pushable if the join is found to be pushable.
    let epq_path = ptr::null_mut::<Path>();
    if (*(*root).parse).commandType == pg_sys::CmdType_CMD_DELETE
        || (*(*root).parse).commandType == pg_sys::CmdType_CMD_UPDATE
        || !(*root).rowMarks.is_null()
    {
        let epq_path = pg_sys::GetExistingLocalJoinPath(joinrel);
        if epq_path.is_null() {
            elog(PgLogLevel::DEBUG3, "could not push down foreign join because a local path suitable for EPQ checks was not found");
            return;
        }
    }

    let (mut outer_ctx, outer_query) = from_fdw_private((*outerrel).fdw_private as *mut _);
    let (inner_ctx, inner_query) = from_fdw_private((*innerrel).fdw_private as *mut _);

    // We only support pushing down joins to the same data source
    if outer_ctx.data_source_id != inner_ctx.data_source_id {
        return;
    }

    let join_type = match jointype {
        pg_sys::JoinType_JOIN_INNER => JoinType::Inner,
        pg_sys::JoinType_JOIN_LEFT => JoinType::Left,
        pg_sys::JoinType_JOIN_RIGHT => JoinType::Right,
        pg_sys::JoinType_JOIN_FULL => JoinType::Full,
        // We dont support all join types
        _ => return,
    };

    // Skip where not everything can be pushed down
    if !outer_query.pushdown_safe() || !inner_query.pushdown_safe() {
        return;
    }

    // Skip where local conditions need to be evaluated before the join
    if !outer_query.local_conds.is_empty() || !inner_query.local_conds.is_empty() {
        return;
    }

    // We only support joining to a base table with conditions (no grouping, windowing etc)
    let mut inner_ops = inner_query
        .as_select()
        .unwrap()
        .all_ops()
        .cloned()
        .collect::<Vec<_>>();
    if inner_ops.iter().any(|i| !i.is_add_where()) {
        return;
    }

    let mut join_query = outer_query.clone();
    let planner = PlannerContext::join_rel(root, joinrel, outerrel, innerrel, jointype, extra);
    let mut join_clauses = vec![];

    for restriction in PgList::<RestrictInfo>::from_pg((*extra).restrictlist).iter_ptr() {
        let join_clause = convert(
            (*restriction).clause as *mut Node as *const _,
            &mut join_query.cvt,
            &planner,
            &outer_ctx,
        );

        /// For an full join we are required to push down all clauses
        if join_type == JoinType::Full && join_clause.is_err() {
            return;
        }

        if let Ok(clause) = join_clause {
            join_clauses.push(clause);
        } else {
            join_query.local_conds.push(restriction);
        }
    }

    // We dont want to push down a cross join
    if join_clauses.is_empty() {
        return;
    }

    let join_op = SelectQueryOperation::AddJoin(sqlil::Join::new(
        join_type,
        inner_ctx.entity.clone(),
        join_clauses,
    ));

    // Apply the join then the conditions to the inner rel
    inner_ops.insert(0, join_op.clone());

    let mut join_query = estimate_path_cost(&mut outer_ctx, &planner, join_query, inner_ops);

    /// If we failed to push down the join then dont generate the path
    if !join_query
        .as_select()
        .unwrap()
        .remote_ops
        .contains(&join_op)
    {
        return;
    }

    // Calculate default costs (we are pessimistic)
    {
        let cost = &mut join_query.cost;
        cost.rows = cost
            .rows
            .or(Some(((*innerrel).rows * (*outerrel).rows) as u64));
        cost.connection_cost = cost.connection_cost.or(Some(DEFAULT_FDW_STARTUP_COST));
        cost.total_cost = cost.total_cost.or(Some(
            (cost.rows.unwrap() as f64 * DEFAULT_FDW_TUPLE_COST) as u64,
        ));
    }

    // Finally create the new path
    let join_path = pg_sys::create_foreign_join_path(
        root,
        joinrel,
        ptr::null_mut(), /* default pathtarget */
        join_query.cost.rows.unwrap() as f64,
        join_query.cost.connection_cost.unwrap() as f64,
        join_query.cost.total_cost.unwrap() as f64,
        ptr::null_mut(), /* no pathkeys */
        (*joinrel).lateral_relids,
        epq_path,
        ptr::null_mut(),
    );
    add_path(joinrel, join_path as *mut _);

    (*joinrel).fdw_private = into_fdw_private(outer_ctx, join_query) as *mut _;
}

/// Add paths for post-join operations like aggregation, grouping etc. if
/// corresponding operations are safe to push down.
///
/// @see https://doxygen.postgresql.org/postgres__fdw_8c.html#a80eb48019ea69aaf90a87a6027d3bdba
pub unsafe extern "C" fn get_foreign_upper_paths(
    root: *mut PlannerInfo,
    stage: UpperRelationKind,
    inputrel: *mut RelOptInfo,
    outputrel: *mut RelOptInfo,
    extra: *mut ::std::os::raw::c_void,
) {
    // If input rel could not be pushed down then skip
    if (*inputrel).fdw_private.is_null() {
        return;
    }

    // If output rel has already been processed then skip
    if !(*outputrel).fdw_private.is_null() {
        return;
    }

    let planner = PlannerContext::upper_rel(root, stage, inputrel, outputrel, extra);

    match stage {
        pg_sys::UpperRelationKind_UPPERREL_GROUP_AGG => get_foreign_grouping_paths(
            root,
            inputrel,
            outputrel,
            extra as *mut pg_sys::GroupPathExtraData,
            &planner,
        ),
        pg_sys::UpperRelationKind_UPPERREL_ORDERED => {
            get_foreign_ordered_paths(root, inputrel, outputrel, &planner)
        }
        pg_sys::UpperRelationKind_UPPERREL_FINAL => get_foreign_final_paths(
            root,
            inputrel,
            outputrel,
            extra as *mut pg_sys::FinalPathExtraData,
            &planner,
        ),
        _ => return,
    }
}

pub unsafe extern "C" fn get_foreign_grouping_paths(
    root: *mut PlannerInfo,
    inputrel: *mut RelOptInfo,
    outputrel: *mut RelOptInfo,
    extra: *mut pg_sys::GroupPathExtraData,
    planner: &PlannerContext,
) {
    let (mut ctx, input_query) = from_fdw_private((*inputrel).fdw_private as *mut _);

    // If we have local conditions on the input we cannot push down the group by
    if !input_query.local_conds.is_empty() {
        return;
    }

    // Dont support grouping sets
    if !(*(*root).parse).groupingSets.is_null() {
        return;
    }

    // Currently, we do no support HAVING clauses
    if !(*extra).havingQual.is_null() {
        return;
    }

    let mut group_query = input_query.clone();
    let groupedrel = (*outputrel).reltarget;
    let mut query_ops = vec![];

    // Iterate each target expr
    for (i, expr) in PgList::<Node>::from_pg((*groupedrel).exprs)
        .iter_ptr()
        .enumerate()
    {
        let expr = convert(expr, &mut group_query.cvt, &planner, &ctx);

        let sort_group_ref = if (*groupedrel).sortgrouprefs.is_null() {
            0
        } else {
            *((*groupedrel).sortgrouprefs.add(i))
        };
        let sort_group = pg_sys::get_sortgroupref_clause_noerr(sort_group_ref, (*groupedrel).exprs);

        // Is this expr a GROUP BY expression?
        if sort_group_ref != 0 && !sort_group.is_null() {
            // If we cannot push the grouping expression down then abort
            if expr.is_err() {
                return;
            }

            let expr = expr.unwrap();

            // We cannot push down parameters within group by clauses
            if expr.walk_any(|e| matches!(e, sqlil::Expr::Parameter(_))) {
                return;
            }

            query_ops.push(SelectQueryOperation::AddGroupBy(expr));
        } else {
            // This is an expression in the output list, append to the query output
            // TODO: Support pushing down bare aggregates and transform locally into target output
            // if the entire expr is unable to be pushed down as per:
            // @see https://doxygen.postgresql.org/postgres__fdw_8c_source.html#l06192
            if expr.is_ok()
                && !expr
                    .as_ref()
                    .unwrap()
                    .walk_any(|e| matches!(e, sqlil::Expr::Parameter(_)))
            {
                query_ops.push(
                    group_query
                        .as_select_mut()
                        .unwrap()
                        .new_column(expr.unwrap()),
                );
            } else {
                return;
            }
        }
    }

    // TODO: add in checks for aggregates in local conditions?

    if query_ops.is_empty() {
        return;
    }

    let mut group_query = estimate_path_cost(&mut ctx, planner, group_query, query_ops.clone());

    // If failed to push down then abort
    if query_ops
        .iter()
        .any(|i| !group_query.as_select().unwrap().remote_ops.contains(i))
    {
        return;
    }

    // Calculate default costs (we are pessimistic)
    {
        let cost = &mut group_query.cost;
        cost.rows = cost.rows.or(Some((*inputrel).rows as u64));
        cost.connection_cost = cost.connection_cost.or(Some(DEFAULT_FDW_STARTUP_COST));
        cost.total_cost = cost.total_cost.or(Some(
            (cost.rows.unwrap() as f64 * DEFAULT_FDW_TUPLE_COST) as u64,
        ));
    }

    let path = pg_sys::create_foreign_upper_path(
        root,
        outputrel,
        groupedrel,
        group_query.cost.rows.unwrap() as f64,
        group_query.cost.connection_cost.unwrap() as f64,
        group_query.cost.total_cost.unwrap() as f64,
        ptr::null_mut(),
        ptr::null_mut(),
        ptr::null_mut(),
    );
    pg_sys::add_path(outputrel, path as *mut _);

    (*outputrel).fdw_private = into_fdw_private(ctx, group_query) as *mut _;
}

pub unsafe extern "C" fn get_foreign_ordered_paths(
    root: *mut PlannerInfo,
    inputrel: *mut RelOptInfo,
    outputrel: *mut RelOptInfo,
    planner: &PlannerContext,
) {
    let (mut ctx, input_query) = from_fdw_private((*inputrel).fdw_private as *mut _);
    let mut order_query = input_query.clone();
    let mut query_ops = vec![];

    for path_key in PgList::<PathKey>::from_pg((*root).sort_pathkeys).iter_ptr() {
        let ec = (*path_key).pk_eclass;

        // We cant push down non-deterministic sorts
        if (*ec).ec_has_volatile {
            return;
        }

        // We dont support NULLS FIRST
        if (*path_key).pk_nulls_first {
            return;
        }

        let em = find_em_for_rel_target(root, ec, inputrel, planner, &mut order_query, &mut ctx);

        if em.is_none() {
            return;
        }

        let (expr, node) = em.unwrap();

        let expr_type = pg_sys::exprType(node);
        let opr_item = pg_sys::lookup_type_cache(
            expr_type,
            (pg_sys::TYPECACHE_LT_OPR | pg_sys::TYPECACHE_GT_OPR) as _,
        );

        let sort_type = if (*opr_item).lt_opr != 0 {
            OrderingType::Asc
        } else if (*opr_item).gt_opr != 0 {
            OrderingType::Desc
        } else {
            // Custom sort operators are not supported
            return;
        };

        query_ops.push(SelectQueryOperation::AddOrderBy(Ordering::new(
            sort_type, expr,
        )));
    }

    let mut order_query = estimate_path_cost(&mut ctx, planner, order_query, query_ops.clone());

    // If failed to push down then abort
    if query_ops
        .iter()
        .any(|i| !order_query.as_select().unwrap().remote_ops.contains(i))
    {
        return;
    }

    // Calculate default costs (we are pessimistic)
    {
        let cost = &mut order_query.cost;
        cost.rows = cost.rows.or(Some((*inputrel).rows as u64));
        cost.connection_cost = cost.connection_cost.or(Some(DEFAULT_FDW_STARTUP_COST));
        cost.total_cost = cost.total_cost.or(Some(
            (cost.rows.unwrap() as f64 * DEFAULT_FDW_TUPLE_COST) as u64,
        ));
    }

    let path = pg_sys::create_foreign_upper_path(
        root,
        inputrel,
        (*root).upper_targets[pg_sys::UpperRelationKind_UPPERREL_ORDERED as usize],
        order_query.cost.rows.unwrap() as f64,
        order_query.cost.connection_cost.unwrap() as f64,
        order_query.cost.total_cost.unwrap() as f64,
        ptr::null_mut(),
        ptr::null_mut(),
        ptr::null_mut(),
    );
    pg_sys::add_path(outputrel, path as *mut _);

    (*outputrel).fdw_private = into_fdw_private(ctx, order_query) as *mut _;
}

pub unsafe extern "C" fn get_foreign_final_paths(
    root: *mut PlannerInfo,
    inputrel: *mut RelOptInfo,
    outputrel: *mut RelOptInfo,
    extra: *mut pg_sys::FinalPathExtraData,
    planner: &PlannerContext,
) {
    let parse = (*root).parse;
    let (mut ctx, input_query) = from_fdw_private((*inputrel).fdw_private as *mut _);

    // Only supported for select
    if (*parse).commandType != pg_sys::CmdType_CMD_SELECT {
        return;
    }

    // No work needed
    if (*extra).limit_needed {
        return;
    }

    // Dont support SRF's
    if (*parse).hasTargetSRFs {
        return;
    }

    // Only support const limits and offsets
    unsafe fn as_const_u64(node: *mut Node) -> Result<Option<u64>> {
        if !node.is_null() {
            return Ok(None);
        }

        let node = if pgx::is_a(node, pg_sys::NodeTag_T_Const) {
            node as *mut pg_sys::Const
        } else {
            bail!("Must be const");
        };

        let val = from_datum((*node).consttype, (*node).constvalue)?;

        Ok(Some(match val {
            DataValue::Int32(i) => i as u64,
            DataValue::Int64(i) => i as u64,
            _ => bail!("Invalid const data type"),
        }))
    }

    let offset = match as_const_u64((*parse).limitOffset) {
        Ok(i) => i,
        Err(_) => return,
    };
    let limit = match as_const_u64((*parse).limitCount) {
        Ok(i) => i,
        Err(_) => return,
    };

    // No work to do
    if offset.is_none() && limit.is_none() {
        return;
    }

    let mut limit_query = input_query.clone();
    let mut query_ops = vec![];

    if let Some(offset) = offset {
        query_ops.push(SelectQueryOperation::SetRowOffset(offset));
    }

    if let Some(limit) = limit {
        query_ops.push(SelectQueryOperation::SetRowLimit(limit));
    }

    let mut limit_query = estimate_path_cost(&mut ctx, planner, limit_query, query_ops.clone());

    // If failed to push down then abort
    if query_ops
        .iter()
        .any(|i| !limit_query.as_select().unwrap().remote_ops.contains(i))
    {
        return;
    }

    // Calculate default costs (we are pessimistic)
    {
        let cost = &mut limit_query.cost;
        cost.rows = cost.rows.or(limit).or(Some((*inputrel).rows as u64));
        cost.connection_cost = cost.connection_cost.or(Some(DEFAULT_FDW_STARTUP_COST));
        cost.total_cost = cost.total_cost.or(Some(
            (cost.rows.unwrap() as f64 * DEFAULT_FDW_TUPLE_COST) as u64,
        ));
    }

    let path = pg_sys::create_foreign_upper_path(
        root,
        inputrel,
        (*root).upper_targets[pg_sys::UpperRelationKind_UPPERREL_FINAL as usize],
        limit_query.cost.rows.unwrap() as f64,
        limit_query.cost.connection_cost.unwrap() as f64,
        limit_query.cost.total_cost.unwrap() as f64,
        ptr::null_mut(),
        ptr::null_mut(),
        ptr::null_mut(),
    );
    pg_sys::add_path(outputrel, path as *mut _);

    (*outputrel).fdw_private = into_fdw_private(ctx, limit_query) as *mut _;
}

pub unsafe extern "C" fn get_foreign_plan(
    root: *mut PlannerInfo,
    foreignrel: *mut RelOptInfo,
    foreigntableid: Oid,
    best_path: *mut ForeignPath,
    tlist: *mut List,
    scan_clauses: *mut List,
    outer_plan: *mut Plan,
) -> *mut ForeignScan {
    let (mut ctx, mut query) = from_fdw_private((*foreignrel).fdw_private as *mut _);
    let select = query.as_select_mut().unwrap();
    let planner = PlannerContext::base_rel(root, foreignrel);

    let mut target_map = HashMap::new();
    let mut unpushed_exprs = vec![];

    // First attempt to map all target cols to expr's
    for node in PgList::<Node>::from_pg((*(*foreignrel).reltarget).exprs).iter_ptr() {
        if let Ok(expr) = convert(node, &mut query.cvt, &planner, &ctx) {
            let col_alias = select.new_column_alias();
            let query_op = SelectQueryOperation::AddColumn((col_alias.clone(), expr));

            if apply_query_operation(&mut ctx, select, query_op).is_none() {
                target_map.insert(node, col_alias);
                continue;
            }
        }

        unpushed_exprs.push(node);
    }

    // Second pull out all cols required for local_conds
}

pub unsafe extern "C" fn begin_foreign_scan(
    node: *mut ForeignScanState,
    eflags: ::std::os::raw::c_int,
) {
    unimplemented!()
}

pub unsafe extern "C" fn iterate_foreign_scan(node: *mut ForeignScanState) -> *mut TupleTableSlot {
    unimplemented!()
}

pub unsafe extern "C" fn recheck_foreign_scan(
    node: *mut ForeignScanState,
    slot: *mut TupleTableSlot,
) -> bool {
    unimplemented!()
}
pub unsafe extern "C" fn re_scan_foreign_scan(node: *mut ForeignScanState) {
    unimplemented!()
}

pub unsafe extern "C" fn end_foreign_scan(node: *mut ForeignScanState) {
    unimplemented!()
}

pub unsafe extern "C" fn shutdown_foreign_scan(node: *mut ForeignScanState) {
    unimplemented!()
}

pub unsafe extern "C" fn estimate_dsm_foreign_scan(
    node: *mut ForeignScanState,
    pcxt: *mut ParallelContext,
) -> Size {
    unimplemented!()
}

pub unsafe extern "C" fn initialize_dsm_foreign_scan(
    node: *mut ForeignScanState,
    pcxt: *mut ParallelContext,
    coordinate: *mut ::std::os::raw::c_void,
) {
    unimplemented!()
}

pub unsafe extern "C" fn re_initialize_dsm_foreign_scan(
    node: *mut ForeignScanState,
    pcxt: *mut ParallelContext,
    coordinate: *mut ::std::os::raw::c_void,
) {
    unimplemented!()
}

pub unsafe extern "C" fn initialize_worker_foreign_scan(
    node: *mut ForeignScanState,
    toc: *mut shm_toc,
    coordinate: *mut ::std::os::raw::c_void,
) {
    unimplemented!()
}

pub unsafe extern "C" fn is_foreign_scan_parallel_safe(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    rte: *mut RangeTblEntry,
) -> bool {
    unimplemented!()
}

pub unsafe extern "C" fn reparameterize_foreign_path_by_child(
    root: *mut PlannerInfo,
    fdw_private: *mut List,
    child_rel: *mut RelOptInfo,
) -> *mut List {
    unimplemented!()
}

// Generate a path cost estimation based on the supplied conditions
unsafe fn estimate_path_cost(
    ctx: &mut FdwContext,
    planner: &PlannerContext,
    mut query: FdwQueryContext,
    new_query_ops: Vec<SelectQueryOperation>,
) -> PgBox<FdwQueryContext, AllocatedByPostgres> {
    let select = query.as_select_mut().unwrap();

    // Initialise a new select query
    let res = ctx
        .send(ClientMessage::Select(ClientSelectMessage::Create(
            ctx.entity.clone(),
        )))
        .unwrap();

    let mut cost = match res {
        ServerMessage::Select(ServerSelectMessage::Result(
            QueryOperationResult::PerformedRemotely(cost),
        )) => cost,
        _ => unexpected_response!(res),
    };

    let mut cost = OperationCost::default();

    // We have already applied these ops to the query before but not on the
    // remote side
    // TODO: optimise so we dont perform duplicate work
    for query_op in select.all_ops() {
        let _ = ctx
            .send(ClientMessage::Select(ClientSelectMessage::Apply(
                query_op.clone(),
            )))
            .unwrap();
    }

    // Apply each of the query operations and evaluate the cost
    for query_op in new_query_ops {
        if !can_push_down(select, &query_op) {
            select.local_ops.push(query_op);
            continue;
        }

        if let Some(new_cost) = apply_query_operation(ctx, select, query_op) {
            cost = new_cost;
        }
    }

    query.cost = cost;

    PgBox::new(query).into_pg_boxed()
}

fn can_push_down(select: &mut FdwSelectQuery, query_op: &SelectQueryOperation) -> bool {
    if select.local_ops.is_empty() {
        return true;
    }

    // Push downs are not affected by output cols
    let mut ops = select.local_ops.iter().filter(|i| !i.is_add_column());

    match query_op {
        SelectQueryOperation::AddColumn(_) => true,
        SelectQueryOperation::AddWhere(_) => ops.any(|i| !i.is_add_where()),
        SelectQueryOperation::AddJoin(_) => ops.any(|i| !i.is_add_where()),
        SelectQueryOperation::AddGroupBy(_) => ops.count() > 0,
        SelectQueryOperation::AddOrderBy(_) => ops.count() > 0,
        SelectQueryOperation::SetRowLimit(_) => ops.count() > 0,
        SelectQueryOperation::SetRowOffset(_) => ops.count() > 0,
    }
}

fn apply_query_operation(
    ctx: &mut FdwContext,
    select: &mut FdwSelectQuery,
    query_op: SelectQueryOperation,
) -> Option<OperationCost> {
    let response = ctx
        .send(ClientMessage::Select(ClientSelectMessage::Apply(
            query_op.clone(),
        )))
        .unwrap();

    let result = match response {
        ServerMessage::Select(ServerSelectMessage::Result(result)) => result,
        _ => unexpected_response!(response),
    };

    match result {
        QueryOperationResult::PerformedRemotely(cost) => {
            select.remote_ops.push(query_op);
            Some(cost)
        }
        QueryOperationResult::PerformedLocally => {
            select.local_ops.push(query_op);
            None
        }
    }
}

/// Converts the supplied context data to a pointer suitable
/// to be stored in fdw_private fields
unsafe fn into_fdw_private(ctx: PgBox<FdwContext>, query: PgBox<FdwQueryContext>) -> *mut List {
    let mut list = PgList::<c_void>::new();

    list.push(ctx.into_pg() as *mut _);
    list.push(query.into_pg() as *mut _);

    list.into_pg()
}

unsafe fn from_fdw_private(
    list: *mut List,
) -> (
    PgBox<FdwContext, AllocatedByPostgres>,
    PgBox<FdwQueryContext, AllocatedByPostgres>,
) {
    let list = PgList::<c_void>::from_pg(list);
    assert!(list.len() == 2);

    let ctx = PgBox::<FdwContext>::from_pg(list.get_ptr(0).unwrap() as *mut _);
    let query = PgBox::<FdwQueryContext>::from_pg(list.get_ptr(1).unwrap() as *mut _);

    (ctx, query)
}

unsafe fn into_fdw_private_path(query: PgBox<FdwQueryContext>) -> *mut List {
    let mut list = PgList::<c_void>::new();

    list.push(query.into_pg() as *mut _);

    list.into_pg()
}

unsafe fn from_fdw_private_path(list: *mut List) -> PgBox<FdwQueryContext, AllocatedByPostgres> {
    let list = PgList::<c_void>::from_pg(list);
    assert!(list.len() == 1);

    let query = PgBox::<FdwQueryContext>::from_pg(list.get_ptr(0).unwrap() as *mut _);

    query
}

unsafe fn find_em_for_rel_target(
    root: *mut PlannerInfo,
    ec: *mut EquivalenceClass,
    rel: *mut RelOptInfo,
    planner: &PlannerContext,
    query: &mut FdwQueryContext,
    ctx: &mut FdwContext,
) -> Option<(sqlil::Expr, *mut Node)> {
    let target = (*rel).reltarget;

    for (i, mut expr) in PgList::<pg_sys::Expr>::from_pg((*target).exprs)
        .iter_ptr()
        .enumerate()
    {
        let sgref = if (*target).sortgrouprefs.is_null() {
            0
        } else {
            *((*target).sortgrouprefs.add(i))
        };

        /* Ignore non-sort expressions */
        if sgref == 0
            || pg_sys::get_sortgroupref_clause_noerr(sgref, (*(*root).parse).sortClause).is_null()
        {
            continue;
        }

        /* We ignore binary-compatible relabeling on both ends */
        while !expr.is_null() && pgx::is_a(expr as *mut _, pg_sys::NodeTag_T_RelabelType) {
            expr = (*(expr as *mut pg_sys::RelabelType)).arg;
        }

        /* Locate an EquivalenceClass member matching this expr, if any */
        for em in PgList::<EquivalenceMember>::from_pg((*ec).ec_members).iter_ptr() {
            {
                /* Don't match constants */
                if (*em).em_is_const {
                    continue;
                }

                /* Ignore child members */
                if (*em).em_is_child {
                    continue;
                }

                /* Match if same expression (after stripping relabel) */
                let mut em_expr = (*em).em_expr;
                while !em_expr.is_null()
                    && pgx::is_a(em_expr as *mut _, pg_sys::NodeTag_T_RelabelType)
                {
                    em_expr = (*(em_expr as *mut pg_sys::RelabelType)).arg;
                }

                if !pg_sys::equal(em_expr as *mut _, expr as *mut _) {
                    continue;
                }

                let expr = convert((*em).em_expr as *mut _, &mut query.cvt, planner, ctx);
                if let Ok(expr) = expr {
                    return Some((expr, (*em).em_expr as *mut Node));
                }
            }
        }
    }

    None
}

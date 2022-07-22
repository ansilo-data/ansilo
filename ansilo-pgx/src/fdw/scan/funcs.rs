use std::{collections::HashMap, ffi::c_void, mem, ops::ControlFlow, ptr};

use ansilo_core::{
    data::DataValue,
    err::{bail, Context, Result},
    sqlil::{self, JoinType, Ordering, OrderingType},
};
use ansilo_pg::fdw::{
    data::DataWriter,
    proto::{
        ClientMessage, ClientSelectMessage, OperationCost, QueryInputStructure,
        QueryOperationResult, RowStructure, SelectQueryOperation, ServerMessage,
        ServerSelectMessage,
    },
};
use pgx::{
    pg_sys::{
        add_path, shm_toc, EquivalenceClass, EquivalenceMember, ForeignPath, ForeignScan,
        ForeignScanState, JoinPathExtraData, List, Node, Oid, ParallelContext, Path, PathKey, Plan,
        PlannerInfo, RangeTblEntry, RelOptInfo, RestrictInfo, Size, TargetEntry, TupleTableSlot,
        UpperRelationKind,
    },
    *,
};

use crate::{
    fdw::{common, ctx::*},
    sqlil::{
        convert, convert_list, from_datum, into_datum, parse_entity_version_id_from_foreign_table,
        parse_entity_version_id_from_rel, ConversionContext, PlannerContext,
    },
    util::list::vec_to_pg_list,
};

macro_rules! unexpected_response {
    ($ctx:expr, $res:expr) => {
        error!("Unexpected response from server while {}: {:?}", $ctx, $res)
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
#[pg_guard]
pub unsafe extern "C" fn get_foreign_rel_size(
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    foreigntableid: Oid,
) {
    let mut ctx = common::connect(foreigntableid);
    let planner = PlannerContext::base_rel(root, baserel);

    let baserel_conds = PgList::<RestrictInfo>::from_pg((*baserel).baserestrictinfo);

    // If no conditions we can use the cheap path
    let entity = ctx.entity.clone();
    let res = ctx.send(ClientMessage::EstimateSize(entity)).unwrap();

    let mut base_cost = match res {
        ServerMessage::EstimatedSizeResult(e) => e,
        _ => unexpected_response!("estimating size", res),
    };

    // We have to evaluate the possibility and costs of pushing down the restriction clauses
    let mut query = FdwQueryContext::select();
    let conds = baserel_conds
        .iter_ptr()
        .filter_map(|i| {
            let expr = convert((*i).clause as *const _, &mut query.cvt, &planner, &ctx).ok();

            // Store conditions requiring local evaluation for later
            if expr.is_none() {
                query.local_conds.push(i);
                return None;
            }

            Some((SelectQueryOperation::AddWhere(expr.unwrap()), i))
        })
        .collect::<Vec<_>>();

    estimate_path_cost(
        &mut ctx,
        &mut query,
        conds.iter().map(|(i, _)| i).cloned().collect(),
    );

    for (cond, ri) in conds.into_iter() {
        if query.as_select().unwrap().remote_ops.contains(&cond) {
            query.remote_conds.push(ri);
        } else {
            query.local_conds.push(ri);
        }
    }

    // Default to base cost
    {
        let cost = &mut query.cost;
        cost.rows = cost.rows.or(base_cost.rows).or(Some(DEFAULT_ROW_VOLUME));
        cost.row_width = cost.row_width.or(base_cost.row_width);
        cost.connection_cost = cost
            .connection_cost
            .or(base_cost.connection_cost)
            .or(Some(DEFAULT_FDW_STARTUP_COST));
        cost.total_cost = cost.total_cost.or(base_cost.total_cost).or(Some(
            (cost.connection_cost.unwrap() as f64
                + cost.rows.unwrap() as f64 * DEFAULT_FDW_TUPLE_COST) as u64,
        ));
    }

    if let Some(rows) = query.cost.rows {
        (*baserel).rows = rows as _;
    }

    if let Some(row_width) = query.cost.row_width {
        (*(*baserel).reltarget).width = row_width as _;
    }

    (*baserel).fdw_private = into_fdw_private_rel(ctx, query) as *mut _;
}

/// Create possible scan paths for a scan on the foreign table
///
/// @see https://doxygen.postgresql.org/postgres__fdw_8c.html#a5e0a23f5638e9b82a7e8c6c5be3389a2
#[pg_guard]
pub unsafe extern "C" fn get_foreign_paths(
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    foreigntableid: Oid,
) {
    let (mut ctx, base_query) = from_fdw_private_rel((*baserel).fdw_private as *mut _);
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
        into_fdw_private_path(planner.clone(), base_query.clone()),
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

        estimate_path_cost(&mut ctx, &mut query, ops);

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
            into_fdw_private_path(planner.clone(), query.clone()),
        );
        add_path(baserel, path as *mut pg_sys::Path);
    }

    // TODO: explore value of exploiting query_pathkeys
}

/// Add possible ForeignPath to joinrel, if join is safe to push down.
///
/// @see https://doxygen.postgresql.org/postgres__fdw_8c.html#a37cae9c397f76945ef22779c7c566002
#[pg_guard]
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

    let (mut outer_ctx, outer_query) = from_fdw_private_rel((*outerrel).fdw_private as *mut _);
    let (inner_ctx, inner_query) = from_fdw_private_rel((*innerrel).fdw_private as *mut _);

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
            join_query.remote_conds.push(restriction);
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

    estimate_path_cost(&mut outer_ctx, &mut join_query, inner_ops);

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
        into_fdw_private_path(planner, join_query.clone()),
    );
    add_path(joinrel, join_path as *mut _);

    (*joinrel).fdw_private = into_fdw_private_rel(outer_ctx, join_query) as *mut _;
}

/// Add paths for post-join operations like aggregation, grouping etc. if
/// corresponding operations are safe to push down.
///
/// @see https://doxygen.postgresql.org/postgres__fdw_8c.html#a80eb48019ea69aaf90a87a6027d3bdba
#[pg_guard]
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

#[pg_guard]
pub unsafe extern "C" fn get_foreign_grouping_paths(
    root: *mut PlannerInfo,
    inputrel: *mut RelOptInfo,
    outputrel: *mut RelOptInfo,
    extra: *mut pg_sys::GroupPathExtraData,
    planner: &PlannerContext,
) {
    let (mut ctx, input_query) = from_fdw_private_rel((*inputrel).fdw_private as *mut _);

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
    for (i, node) in PgList::<Node>::from_pg((*groupedrel).exprs)
        .iter_ptr()
        .enumerate()
    {
        let expr = convert(node, &mut group_query.cvt, &planner, &ctx);

        let sort_group_ref = if (*groupedrel).sortgrouprefs.is_null() {
            0
        } else {
            *((*groupedrel).sortgrouprefs.add(i))
        };
        let sort_group =
            pg_sys::get_sortgroupref_clause_noerr(sort_group_ref, (*(*root).parse).groupClause);

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
            // Retrieve the vars/aggrefs from the expression
            let required_vars = pull_vars([node].into_iter());

            // Try map each to an expression to be pushed down
            for var in required_vars {
                let expr = convert(var, &mut group_query.cvt, &planner, &ctx);

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
                    // Failed to convert to expression, we cannot push down this grouping
                    return;
                }
            }
        }
    }

    // TODO: add in checks for aggregates in local conditions?

    if query_ops.is_empty() {
        return;
    }

    estimate_path_cost(&mut ctx, &mut group_query, query_ops.clone());

    // If failed to push down then abort
    if query_ops
        .iter()
        .any(|i| !group_query.as_select().unwrap().remote_ops.contains(i))
    {
        return;
    }

    // Success, we forget the AddColumn operations as this is performed later in get_foreign_plan
    let select = group_query.as_select_mut().unwrap();
    select.remote_ops = select
        .remote_ops
        .iter()
        .filter(|i| !i.is_add_column())
        .cloned()
        .collect::<Vec<_>>();

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
        into_fdw_private_path(planner.clone(), group_query.clone()),
    );
    pg_sys::add_path(outputrel, path as *mut _);

    (*outputrel).fdw_private = into_fdw_private_rel(ctx, group_query) as *mut _;
}

#[pg_guard]
pub unsafe extern "C" fn get_foreign_ordered_paths(
    root: *mut PlannerInfo,
    inputrel: *mut RelOptInfo,
    outputrel: *mut RelOptInfo,
    planner: &PlannerContext,
) {
    let (mut ctx, input_query) = from_fdw_private_rel((*inputrel).fdw_private as *mut _);
    let mut order_query = input_query.clone();
    let mut query_ops = vec![];

    for path_key in PgList::<PathKey>::from_pg((*root).sort_pathkeys).iter_ptr() {
        let ec = (*path_key).pk_eclass;

        // We cant push down non-deterministic sorts
        if (*ec).ec_has_volatile {
            return;
        }

        let em = find_em_for_rel_target(root, ec, inputrel, planner, &mut order_query, &mut ctx);

        if em.is_none() {
            return;
        }

        let (expr, em) = em.unwrap();

        // We intentionally ignore (*path_key).pk_nulls_first and leave the order
        // the behaviour as unspecified, so that the data source can apply its platform-specific
        // behaviour
        let oprid = pg_sys::get_opfamily_member(
            (*path_key).pk_opfamily,
            (*em).em_datatype,
            (*em).em_datatype,
            (*path_key).pk_strategy as _,
        );

        if oprid == pg_sys::InvalidOid {
            panic!("Failed to determine sort order operator");
        }


        let expr_type = pg_sys::exprType((*em).em_expr as *mut _);
        let opr_item = pg_sys::lookup_type_cache(
            expr_type,
            (pg_sys::TYPECACHE_LT_OPR | pg_sys::TYPECACHE_GT_OPR) as _,
        );

        let sort_type = if oprid == (*opr_item).lt_opr {
            OrderingType::Asc
        } else if  oprid ==(*opr_item).gt_opr  {
            OrderingType::Desc
        } else {
            // Custom sort operators are not supported
            return;
        };

        query_ops.push(SelectQueryOperation::AddOrderBy(Ordering::new(
            sort_type, expr,
        )));
    }

    estimate_path_cost(&mut ctx, &mut order_query, query_ops.clone());

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

    // We could theoriticall pass sort_pathkeys to this path
    // However this could mean the query optimiser may leverage this information
    // to perform merge joins. Given we cannot 100% guarantee thats the sort
    // order will be respected by the data sources so we do not apply the path keys
    // to this path.
    // TODO[low]: We could probably implement a response flag to determine if data source
    // will guarantee the requested sort ordering at some point.
    let path = pg_sys::create_foreign_upper_path(
        root,
        inputrel,
        (*root).upper_targets[pg_sys::UpperRelationKind_UPPERREL_ORDERED as usize],
        order_query.cost.rows.unwrap() as f64,
        order_query.cost.connection_cost.unwrap() as f64,
        order_query.cost.total_cost.unwrap() as f64,
        ptr::null_mut(),
        ptr::null_mut(),
        into_fdw_private_path(planner.clone(), order_query.clone()),
    );
    pg_sys::add_path(outputrel, path as *mut _);

    (*outputrel).fdw_private = into_fdw_private_rel(ctx, order_query) as *mut _;
}

#[pg_guard]
pub unsafe extern "C" fn get_foreign_final_paths(
    root: *mut PlannerInfo,
    inputrel: *mut RelOptInfo,
    outputrel: *mut RelOptInfo,
    extra: *mut pg_sys::FinalPathExtraData,
    planner: &PlannerContext,
) {
    let parse = (*root).parse;
    let (mut ctx, input_query) = from_fdw_private_rel((*inputrel).fdw_private as *mut _);

    // Only supported for select
    if (*parse).commandType != pg_sys::CmdType_CMD_SELECT {
        return;
    }

    // No work needed
    if !(*extra).limit_needed {
        return;
    }

    // Dont support SRF's
    if (*parse).hasTargetSRFs {
        return;
    }

    // Only support const limits and offsets
    unsafe fn as_const_u64(node: *mut Node) -> Result<Option<u64>> {
        if node.is_null() {
            return Ok(None);
        }

        let node = if pgx::is_a(node, pg_sys::NodeTag_T_Const) {
            node as *mut pg_sys::Const
        } else {
            bail!("Must be const");
        };

        let val = from_datum((*node).consttype, (*node).constvalue)?;

        Ok(Some(match val {
            DataValue::Int32(i) if i >= 0 => i as u64,
            DataValue::Int64(i) if i >= 0 => i as u64,
            _ => bail!("Invalid const data type or value"),
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

    estimate_path_cost(&mut ctx, &mut limit_query, query_ops.clone());

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
        into_fdw_private_path(planner.clone(), limit_query.clone()),
    );
    pg_sys::add_path(outputrel, path as *mut _);

    (*outputrel).fdw_private = into_fdw_private_rel(ctx, limit_query) as *mut _;
}

/// Create ForeignScan plan node which implements selected best path
///
/// @see https://doxygen.postgresql.org/postgres__fdw_8c.html#a59f8af85f3e7696f2d44910600ff2463
#[pg_guard]
pub unsafe extern "C" fn get_foreign_plan(
    root: *mut PlannerInfo,
    foreignrel: *mut RelOptInfo,
    foreigntableid: Oid,
    best_path: *mut ForeignPath,
    tlist: *mut List,
    scan_clauses: *mut List,
    outer_plan: *mut Plan,
) -> *mut ForeignScan {
    let (mut ctx, _) = from_fdw_private_rel((*foreignrel).fdw_private as *mut _);
    let (planner, mut query) = from_fdw_private_path((*best_path).fdw_private);

    let scan_relid = if (*foreignrel).reloptkind == pg_sys::RelOptKind_RELOPT_BASEREL
        || (*foreignrel).reloptkind == pg_sys::RelOptKind_RELOPT_OTHER_MEMBER_REL
    {
        (*foreignrel).relid
    } else {
        0
    };

    // If any scan clauses not pushed down, add to query local conds
    if !scan_clauses.is_null() {
        for clause in PgList::<RestrictInfo>::from_pg(scan_clauses).iter_ptr() {
            if !query
                .remote_conds
                .iter()
                .chain(query.local_conds.iter())
                .any(|i| pg_sys::equal((*i) as *mut _, clause as *mut _))
            {
                query.local_conds.push(clause);
            }
        }
    }

    // fdw_scan_tlist is a targetlist describing the contents of the scan tuple
    // returned by the FDW; it can be NIL if the scan tuple matches the declared
    // rowtype of the foreign table, which is the normal case for a simple foreign
    // table scan.  (If the plan node represents a foreign join, fdw_scan_tlist
    // is required since there is no rowtype available from the system catalogs.)
    // When fdw_scan_tlist is provided, Vars in the node's tlist and quals must
    // have varno INDEX_VAR, and their varattnos correspond to resnos in the
    // fdw_scan_tlist (which are also column numbers in the actual scan tuple).
    // fdw_scan_tlist is never actually executed; it just holds expression trees
    // describing what is in the scan tuple's columns.
    let mut fdw_scan_list: Vec<*mut TargetEntry> = vec![];
    let mut result_tlist = PgList::<pg_sys::TargetEntry>::from_pg(tlist);
    let mut resno = 1;

    apply_query_state(&mut ctx, &query);

    // First, pull out all cols/aggrefs required for the query (tlist, local conds and target expr's)
    let required_cols = pull_vars(
        result_tlist
            .iter_ptr()
            .map(|i| (*i).expr as *mut Node)
            .chain(
                query
                    .local_conds
                    .clone()
                    .into_iter()
                    .map(|i| (*i).clause as *mut Node),
            )
            .chain(PgList::<Node>::from_pg((*(*foreignrel).reltarget).exprs).iter_ptr()),
    );

    for col in required_cols {
        // If we already have added this col for selection, skip it
        if fdw_scan_list
            .iter()
            .any(|i| pg_sys::equal((*i) as *mut _, col as *mut _))
        {
            continue;
        }

        let expr = match convert(col as *mut _, &mut query.cvt, &planner, &ctx) {
            Ok(expr) => expr,
            Err(err) => {
                panic!("Failed to push down column required for local condition evaluation: {err}");
            }
        };

        let col_alias = query.as_select_mut().unwrap().new_column_alias();
        let query_op = SelectQueryOperation::AddColumn((col_alias.clone(), expr));

        if apply_query_operation(&mut ctx, query.as_select_mut().unwrap(), query_op).is_none() {
            panic!("Failed to push down column required for local condition evaluation: rejected by remote");
        }

        fdw_scan_list.push(col as *mut _);
    }

    // Convert expr nodes to target entry list
    let fdw_scan_list = pg_sys::add_to_flat_tlist(ptr::null_mut(), vec_to_pg_list(fdw_scan_list));

    // Ensure outer plan generates tuples with the matching desc
    let mut outer_plan = outer_plan;
    if !outer_plan.is_null() {
        outer_plan = pg_sys::change_plan_targetlist(
            outer_plan,
            fdw_scan_list,
            (*best_path).path.parallel_safe,
        );
    }

    let fdw_private = into_fdw_private_rel(ctx, query.clone());

    pg_sys::make_foreignscan(
        tlist,
        pg_sys::extract_actual_clauses(vec_to_pg_list(query.local_conds.clone()), false),
        scan_relid,
        vec_to_pg_list(query.cvt.param_nodes()),
        fdw_private,
        fdw_scan_list,
        pg_sys::extract_actual_clauses(vec_to_pg_list(query.remote_conds.clone()), false),
        outer_plan,
    )
}

/// Retrieves all vars (columns) and aggref's from the supplied node iterator
unsafe fn pull_vars(nodes: impl std::iter::Iterator<Item = *mut Node>) -> Vec<*mut Node> {
    nodes
        .map(|i| {
            pg_sys::pull_var_clause(
                i,
                (pg_sys::PVC_RECURSE_PLACEHOLDERS | pg_sys::PVC_INCLUDE_AGGREGATES) as _,
            )
        })
        .flat_map(|i| PgList::<Node>::from_pg(i).iter_ptr().collect::<Vec<_>>())
        .collect::<Vec<_>>()
}

#[pg_guard]
pub unsafe extern "C" fn begin_foreign_scan(
    node: *mut ForeignScanState,
    eflags: ::std::os::raw::c_int,
) {
    // Ignore EXPLAIN queries
    if eflags & pg_sys::EXEC_FLAG_EXPLAIN_ONLY as i32 != 0 {
        return;
    }

    let plan = (*node).ss.ps.plan as *mut ForeignScan;
    let (mut ctx, mut query) = from_fdw_private_rel((*plan).fdw_private);
    let mut scan = FdwScanContext::new();

    // Prepare the query for the chosen path
    apply_query_state(&mut ctx, &query);
    let input_structure = ctx.prepare_query().unwrap();

    // Send query params, if any
    if !input_structure.params.is_empty() {
        send_query_params(&mut ctx, &mut scan, &query, &input_structure, node);
    }

    let row_structure = ctx.execute_query().unwrap();

    scan.row_structure = Some(row_structure);
    (*node).fdw_state = into_fdw_private_scan(ctx, query, scan) as *mut _;
}

unsafe fn send_query_params(
    ctx: &mut FdwContext,
    scan: &mut FdwScanContext,
    query: &FdwQueryContext,
    input_structure: &QueryInputStructure,
    node: *mut ForeignScanState,
) {
    // Prepare the query param expr's if it has not been done
    // If a scan is restarted, they should already be present
    if scan.param_exprs.is_none() {
        let param_nodes = query.cvt.param_nodes();
        let param_exprs = PgList::<pg_sys::ExprState>::from_pg(pg_sys::ExecInitExprList(
            vec_to_pg_list(param_nodes.clone()),
            node as _,
        ));

        // Collect list of param id's to their respective ExprState nodes
        let param_map = param_exprs
            .iter_ptr()
            .zip(param_nodes.into_iter())
            .enumerate()
            .flat_map(|(idx, (expr, node))| {
                query
                    .cvt
                    .param_ids(node)
                    .into_iter()
                    .map(|id| (id, (expr, pg_sys::exprType(node))))
                    .collect::<Vec<_>>()
                    .into_iter()
            })
            .collect::<HashMap<_, _>>();

        scan.param_exprs = Some(param_map);
    }

    // Evaluate each parameter to a datum
    // We do so in a short-lived memory context so as not to leak the memory
    let param_exprs = scan.param_exprs.as_ref().unwrap();
    let econtext = (*node).ss.ps.ps_ExprContext;
    let input_data =
        PgMemoryContexts::For((*econtext).ecxt_per_tuple_memory).switch_to(|context| {
            input_structure
                .params
                .iter()
                .map(|(id, r#type)| {
                    let (expr, type_oid) = *param_exprs.get(id).unwrap();
                    let mut is_null = false;

                    let datum = (*expr).evalfunc.unwrap()(expr, econtext, &mut is_null as *mut _);
                    from_datum(type_oid, datum).unwrap()
                })
                .collect::<Vec<_>>()
        });

    // Finally, serialise and send the query params
    ctx.write_query_input(input_data);
}

/// Retrieve next row from the result set, or clear tuple slot to indicate EOF
///
/// @see https://doxygen.postgresql.org/postgres__fdw_8c.html#a9fcea554f6ec98e0c00e214f6d933392
#[pg_guard]
pub unsafe extern "C" fn iterate_foreign_scan(node: *mut ForeignScanState) -> *mut TupleTableSlot {
    let slot = (*node).ss.ss_ScanTupleSlot;

    let (mut ctx, _, scan) = from_fdw_private_scan((*node).fdw_state as _);
    let row_structure = scan.row_structure.as_ref().unwrap();
    let tupdesc = (*slot).tts_tupleDescriptor;
    let nattrs = (*tupdesc).natts as usize;

    assert!(row_structure.cols.len() == nattrs);

    // equivalent of ExecClearTuple(slot) (symbol is not exposed)
    (*(*slot).tts_ops).clear.unwrap()(slot);

    let attrs = (*tupdesc).attrs.as_slice(nattrs);
    (*slot).tts_values = pg_sys::palloc(nattrs * mem::size_of::<pg_sys::Datum>()) as *mut _;
    (*slot).tts_isnull = pg_sys::palloc(nattrs * mem::size_of::<bool>()) as *mut _;

    // Read the next row into the tuple
    for i in 0..nattrs {
        let data = ctx
            .read_result_data()
            .context("Failed to read data value")
            .unwrap();

        // Check if we have reached the last data value
        if data.is_none() {
            // If this is the first attribute we have reached the end so return an empty tuple
            if i == 0 {
                return slot;
            }

            // Else, we have a read a partial row, abort
            panic!("Unexpected EOF reached while reading next row");
        }

        // Convert the retrieved value to a pg datum and store in the tuple
        into_datum(
            attrs[i].atttypid,
            &row_structure.cols[i].1,
            data.unwrap(),
            (*slot).tts_isnull.add(i),
            (*slot).tts_values.add(i),
        )
        .unwrap();
    }

    pg_sys::ExecStoreVirtualTuple(slot);
    slot
}

/// Execute a local join execution plan for a foreign join
///
/// @see https://doxygen.postgresql.org/postgres__fdw_8c.html#abf164069f2b8ed8277045060b66b98ab
#[pg_guard]
pub unsafe extern "C" fn recheck_foreign_scan(
    node: *mut ForeignScanState,
    slot: *mut TupleTableSlot,
) -> bool {
    let scanrelid = (*((*node).ss.ps.plan as *mut pg_sys::Scan)).scanrelid;
    let outerplan = (*(node as *mut pg_sys::PlanState)).lefttree;

    // For base foreign relations, it suffices to set fdw_recheck_quals
    if scanrelid > 0 {
        return true;
    }

    assert!(!outerplan.is_null());

    /* Execute a local join execution plan */
    let result = {
        if !(*outerplan).chgParam.is_null() {
            pg_sys::ExecReScan(outerplan)
        }

        (*outerplan).ExecProcNode.unwrap()(outerplan)
    };

    if result.is_null() || (*result).tts_flags & pg_sys::TTS_FLAG_EMPTY as u16 != 0 {
        return false;
    }

    /* Store result in the given slot */
    (*(*slot).tts_ops).copyslot.unwrap()(slot, result);

    return true;
}

/// Restart the scan.
///
/// @see https://doxygen.postgresql.org/postgres__fdw_8c_source.html#l01641
#[pg_guard]
pub unsafe extern "C" fn re_scan_foreign_scan(node: *mut ForeignScanState) {
    let (mut ctx, query, mut scan) = from_fdw_private_scan((*node).fdw_state as _);

    ctx.restart_query().unwrap();

    // Rewrite query params, if changed
    if !(*node).ss.ps.chgParam.is_null() {
        let input_structure = ctx.query_writer.as_ref().unwrap().get_structure().clone();
        if !input_structure.params.is_empty() {
            send_query_params(&mut ctx, &mut scan, &query, &input_structure, node);
        }
    }

    ctx.execute_query().unwrap();
}

/// Finish scanning foreign table and dispose objects used for this scan
///
/// @see https://doxygen.postgresql.org/postgres__fdw_8c.html#a5a14f8d89c5b76e02df2e8615f7a6835
#[pg_guard]
pub unsafe extern "C" fn end_foreign_scan(node: *mut ForeignScanState) {
    // Check if this is an EXPLAIN query and skip if so
    if (*node).fdw_state.is_null() {
        return;
    }

    let (mut ctx, _, _) = from_fdw_private_scan((*node).fdw_state as _);

    ctx.disconnect().unwrap();

    // TODO: verify no mem leaks
}

#[pg_guard]
pub unsafe extern "C" fn shutdown_foreign_scan(node: *mut ForeignScanState) {
    unimplemented!()
}

#[pg_guard]
pub unsafe extern "C" fn estimate_dsm_foreign_scan(
    node: *mut ForeignScanState,
    pcxt: *mut ParallelContext,
) -> Size {
    unimplemented!()
}

#[pg_guard]
pub unsafe extern "C" fn initialize_dsm_foreign_scan(
    node: *mut ForeignScanState,
    pcxt: *mut ParallelContext,
    coordinate: *mut ::std::os::raw::c_void,
) {
    unimplemented!()
}

#[pg_guard]
pub unsafe extern "C" fn re_initialize_dsm_foreign_scan(
    node: *mut ForeignScanState,
    pcxt: *mut ParallelContext,
    coordinate: *mut ::std::os::raw::c_void,
) {
    unimplemented!()
}

#[pg_guard]
pub unsafe extern "C" fn initialize_worker_foreign_scan(
    node: *mut ForeignScanState,
    toc: *mut shm_toc,
    coordinate: *mut ::std::os::raw::c_void,
) {
    unimplemented!()
}

#[pg_guard]
pub unsafe extern "C" fn is_foreign_scan_parallel_safe(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    rte: *mut RangeTblEntry,
) -> bool {
    unimplemented!()
}

#[pg_guard]
pub unsafe extern "C" fn reparameterize_foreign_path_by_child(
    root: *mut PlannerInfo,
    fdw_private: *mut List,
    child_rel: *mut RelOptInfo,
) -> *mut List {
    unimplemented!()
}

// Sends the query
unsafe fn apply_query_state(ctx: &mut FdwContext, query: &FdwQueryContext) {
    let select = query.as_select().unwrap();

    // Initialise a new select query
    let res = ctx
        .send(ClientMessage::Select(ClientSelectMessage::Create(
            ctx.entity.clone(),
        )))
        .unwrap();

    let cost = match res {
        ServerMessage::Select(ServerSelectMessage::Result(
            QueryOperationResult::PerformedRemotely(cost),
        )) => cost,
        _ => unexpected_response!("creating select query", res),
    };

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
}

// Generate a path cost estimation based on the supplied conditions
unsafe fn estimate_path_cost(
    ctx: &mut FdwContext,
    query: &mut FdwQueryContext,
    new_query_ops: Vec<SelectQueryOperation>,
) {
    apply_query_state(ctx, query);

    let select = query.as_select_mut().unwrap();
    let mut cost = None;

    // Apply each of the query operations and evaluate the cost
    for query_op in new_query_ops {
        if !can_push_down(select, &query_op) {
            select.local_ops.push(query_op);
            continue;
        }

        if let Some(new_cost) = apply_query_operation(ctx, select, query_op) {
            cost = Some(new_cost);
        }
    }

    if let Some(cost) = cost {
        query.cost = cost;
    }
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
        _ => unexpected_response!("applying query operation", response),
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

unsafe fn find_em_for_rel_target(
    root: *mut PlannerInfo,
    ec: *mut EquivalenceClass,
    rel: *mut RelOptInfo,
    planner: &PlannerContext,
    query: &mut FdwQueryContext,
    ctx: &mut FdwContext,
) -> Option<(sqlil::Expr, *mut pg_sys::EquivalenceMember)> {
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
                    return Some((expr, em));
                }
            }
        }
    }

    None
}

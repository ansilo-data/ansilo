use std::{cmp, collections::HashMap, ffi::c_void, mem, ops::ControlFlow, ptr};

use ansilo_core::{
    data::DataValue,
    err::{bail, Context, Result},
    sqlil::{self, JoinType, Ordering, OrderingType},
};
use ansilo_pg::fdw::{
    data::DataWriter,
    proto::{
        ClientMessage, OperationCost, QueryInputStructure, QueryOperationResult, RowStructure,
        SelectQueryOperation, ServerMessage,
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
        convert, convert_list, from_datum, into_datum, into_pg_type,
        parse_entity_version_id_from_foreign_table, parse_entity_version_id_from_rel,
        ConversionContext,
    },
    util::{list::vec_to_pg_list, table::PgTable},
};

/// Default cost values in case they cant be estimated
/// Values borroed from
/// @see https://doxygen.postgresql.org/postgres__fdw_8c_source.html#l03570
const DEFAULT_FDW_STARTUP_COST: f64 = 100.0;
const DEFAULT_FDW_TUPLE_COST: f64 = 0.01;

/// We want to favour doing work remotely rather the locally
/// so we apply the following cost multiplier to all remote work
const DEFAULT_FDW_REMOTE_WORK_MULTIPLIER: f64 = 0.25;

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

    let mut base_cost = ctx.estimate_size().unwrap();

    // Default row number if not supplied from data source
    base_cost.rows = base_cost.rows.or(Some(DEFAULT_ROW_VOLUME));

    // We have to evaluate the possibility and costs of pushing down the restriction clauses
    let mut query = FdwQueryContext::select((*baserel).relid, base_cost.clone());

    let baserel_conds = PgList::<RestrictInfo>::from_pg((*baserel).baserestrictinfo);
    apply_query_conds(
        &mut ctx,
        &mut query,
        &planner,
        baserel_conds.iter_ptr().collect(),
    );

    let cost = calculate_query_cost(&mut query, &planner);

    if let Some(rows) = cost.rows {
        (*baserel).rows = rows as _;
    }

    if let Some(row_width) = cost.row_width {
        (*(*baserel).reltarget).width = row_width as _;
    }

    (*baserel).fdw_private = into_fdw_private_rel(ctx, query, planner) as *mut _;
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
    let (mut ctx, mut base_query, _) = from_fdw_private_rel((*baserel).fdw_private as *mut _);
    let planner = PlannerContext::base_rel(root, baserel);
    let base_cost = calculate_query_cost(&mut base_query, &planner);

    // Create a default full-scan path for the rel
    let path = pg_sys::create_foreignscan_path(
        root,
        baserel,
        ptr::null_mut(),
        base_cost.rows.unwrap() as f64,
        base_cost.startup_cost.unwrap(),
        base_cost.total_cost.unwrap(),
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

        apply_query_conds(
            &mut ctx,
            &mut query,
            &planner,
            PgList::<RestrictInfo>::from_pg((*ppi).ppi_clauses)
                .iter_ptr()
                .collect(),
        );

        let cost = calculate_query_cost(&mut query, &planner);

        let path = pg_sys::create_foreignscan_path(
            root,
            baserel,
            ptr::null_mut(),
            cost.rows.unwrap() as f64,
            cost.startup_cost.unwrap(),
            cost.total_cost.unwrap(),
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

    let (mut outer_ctx, outer_query, _) = from_fdw_private_rel((*outerrel).fdw_private as *mut _);
    let (inner_ctx, inner_query, _) = from_fdw_private_rel((*innerrel).fdw_private as *mut _);

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

    // Skip where local conditions need to be evaluated before the join
    if !outer_query.local_conds.is_empty() || !inner_query.local_conds.is_empty() {
        return;
    }

    let mut inner_ops = inner_query.as_select().unwrap().remote_ops.clone();

    // We only support joining to a base table with conditions (no grouping, windowing etc)
    if inner_ops.iter().any(|i| !i.is_add_where()) {
        return;
    }

    let mut join_query = outer_query.clone();

    // We need to recalculate the retrieved row estimate
    // (done later in the function)
    join_query.retrieved_rows = None;

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

    let target_alias = join_query.cvt.register_alias(inner_query.base_relid);
    let join_op = SelectQueryOperation::AddJoin(sqlil::Join::new(
        join_type,
        sqlil::EntitySource::new(inner_ctx.entity.clone(), target_alias),
        join_clauses,
    ));

    // Apply the join before the conditions
    apply_query_operations(&mut outer_ctx, &mut join_query, vec![join_op.clone()]);

    // Apply the base conditions of the inner query to the join query
    // It is important we redo the mapping of RestrictInfo's to sqlil expr's
    // as any query parameters, table aliases could be different when merged
    // into the join query.
    apply_query_conds(
        &mut outer_ctx,
        &mut join_query,
        &planner,
        inner_query.remote_conds.clone(),
    );

    /// If we failed to push down the join then dont generate the path
    if !join_query
        .as_select()
        .unwrap()
        .remote_ops
        .contains(&join_op)
    {
        return;
    }

    // If retrieved rows is not calculated by the data source we
    // estimate it here
    let cross_product = outer_query.retrieved_rows.unwrap() * inner_query.retrieved_rows.unwrap();

    if join_query.retrieved_rows.is_none() {
        let (selectivity, _) = calculate_cond_costs(&planner, join_query.remote_conds.clone());

        join_query.retrieved_rows =
            Some(pg_sys::clamp_row_est(cross_product as f64 * selectivity) as u64);
    }

    // Calculate the costs of performing the join operation remotely
    {
        let join_conds = (*extra).restrictlist;
        join_query.add_cost(move |_, mut cost| {
            let mut cond_cost = pg_sys::QualCost::default();
            pg_sys::cost_qual_eval(&mut cond_cost, join_conds, root);

            cost.startup_cost = cost
                .startup_cost
                .map(|c| c + cond_cost.startup * DEFAULT_FDW_REMOTE_WORK_MULTIPLIER);

            // Calculate the costs for performing the join remotely can blow up quadratically
            // so in order favour remote joins over local joins but still compare multiple
            // remote joins equally we apply a sqrt to shift it to a linear growth
            cost.total_cost = cost.total_cost.map(|c| {
                c + cond_cost.per_tuple
                    * (cross_product as f64).sqrt()
                    * DEFAULT_FDW_REMOTE_WORK_MULTIPLIER
            });

            cost
        });
    }

    let cost = calculate_query_cost(&mut join_query, &planner);

    // Finally create the new path
    let join_path = pg_sys::create_foreign_join_path(
        root,
        joinrel,
        ptr::null_mut(), /* default pathtarget */
        cost.rows.unwrap() as f64,
        cost.startup_cost.unwrap(),
        cost.total_cost.unwrap(),
        ptr::null_mut(), /* no pathkeys */
        (*joinrel).lateral_relids,
        epq_path,
        into_fdw_private_path(planner.clone(), join_query.clone()),
    );
    add_path(joinrel, join_path as *mut _);

    (*joinrel).fdw_private = into_fdw_private_rel(outer_ctx, join_query, planner) as *mut _;
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
    let (mut ctx, input_query, _) = from_fdw_private_rel((*inputrel).fdw_private as *mut _);

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

    // Invalidate the retrieved rows estimate and, if required,
    // estimate it below
    group_query.retrieved_rows = None;

    let groupedrel = (*outputrel).reltarget;
    let mut group_by_exprs = vec![];
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
            group_by_exprs.push(node);
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

    apply_query_operations(&mut ctx, &mut group_query, query_ops.clone());

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

    // If the row estimate is not retrieved from the source
    // estimate it below
    if group_query.retrieved_rows.is_none() {
        group_query.retrieved_rows = Some(pg_sys::estimate_num_groups(
            root,
            vec_to_pg_list(group_by_exprs),
            // We can assume retreived rows estimate is equal to the
            // input row estimate as we have checked there are no
            // conds requiring local evaluation
            input_query.retrieved_rows.unwrap() as _,
            ptr::null_mut(),
            ptr::null_mut(),
        ) as u64);
    }

    // Calculate costs for calculating aggregates remotely
    extern "C" {
        fn get_agg_clause_costs(
            root: *mut PlannerInfo,
            aggsplit: pg_sys::AggSplit,
            costs: *mut pg_sys::AggClauseCosts,
        );
    }

    let mut aggcosts = pg_sys::AggClauseCosts::default();
    get_agg_clause_costs(
        root,
        pg_sys::AggSplit_AGGSPLIT_SIMPLE,
        &mut aggcosts as *mut _,
    );

    group_query.add_cost(move |_, mut cost| {
        cost.startup_cost = cost
            .startup_cost
            .map(|c| c + aggcosts.transCost.startup * DEFAULT_FDW_REMOTE_WORK_MULTIPLIER);
        cost.total_cost = cost.total_cost.map(|c| {
            c + aggcosts.transCost.per_tuple
                * input_query.retrieved_rows.unwrap() as f64
                * DEFAULT_FDW_REMOTE_WORK_MULTIPLIER
        });

        cost
    });

    let cost = calculate_query_cost(&mut group_query, &planner);

    let path = pg_sys::create_foreign_upper_path(
        root,
        outputrel,
        groupedrel,
        cost.rows.unwrap() as f64,
        cost.startup_cost.unwrap(),
        cost.total_cost.unwrap(),
        ptr::null_mut(),
        ptr::null_mut(),
        into_fdw_private_path(planner.clone(), group_query.clone()),
    );
    pg_sys::add_path(outputrel, path as *mut _);

    (*outputrel).fdw_private = into_fdw_private_rel(ctx, group_query, planner.clone()) as *mut _;
}

#[pg_guard]
pub unsafe extern "C" fn get_foreign_ordered_paths(
    root: *mut PlannerInfo,
    inputrel: *mut RelOptInfo,
    outputrel: *mut RelOptInfo,
    planner: &PlannerContext,
) {
    let (mut ctx, input_query, _) = from_fdw_private_rel((*inputrel).fdw_private as *mut _);

    // We cannot ordering if conditions require local evaluation
    if !input_query.local_conds.is_empty() {
        return;
    }

    let mut order_query = input_query.clone();
    let mut query_ops = vec![];
    let mut path_keys = vec![];

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
        } else if oprid == (*opr_item).gt_opr {
            OrderingType::Desc
        } else {
            // Custom sort operators are not supported
            return;
        };

        query_ops.push(SelectQueryOperation::AddOrderBy(Ordering::new(
            sort_type, expr,
        )));
        path_keys.push(path_key);
    }

    apply_query_operations(&mut ctx, &mut order_query, query_ops.clone());

    // If failed to push down then abort
    if query_ops
        .iter()
        .any(|i| !order_query.as_select().unwrap().remote_ops.contains(i))
    {
        return;
    }

    // Calculate the cost of sorting the rows
    {
        let input_rows = input_query.retrieved_rows.unwrap();
        let row_width = input_query
            .base_cost
            .row_width
            .unwrap_or((*(*inputrel).reltarget).width as _);
        let path_keys = vec_to_pg_list(path_keys);

        order_query.add_cost(move |query, mut cost| {
            // Retrieve the LIMIT clause if supplied
            let limit = if let Some(limit) = query
                .as_select()
                .unwrap()
                .remote_ops
                .iter()
                .find(|i| i.is_set_row_limit())
            {
                match limit {
                    SelectQueryOperation::SetRowLimit(lim) => *lim as f64,
                    _ => unreachable!(),
                }
            } else {
                -1.0
            };

            let mut sort_path = pg_sys::Path::default();
            pg_sys::cost_sort(
                &mut sort_path as *mut _,
                root,
                path_keys,
                0.0,
                input_rows as _,
                row_width as _,
                0.0,
                pg_sys::work_mem,
                limit as _,
            );

            // Add sort costs
            cost.startup_cost = cost
                .startup_cost
                .map(|c| c + sort_path.startup_cost * DEFAULT_FDW_REMOTE_WORK_MULTIPLIER);
            cost.total_cost = cost
                .total_cost
                .map(|c| c + sort_path.total_cost * DEFAULT_FDW_REMOTE_WORK_MULTIPLIER);

            cost
        });
    }

    // Ordering should not affect the number of rows so we just
    // calculate the cost using the existing estimate
    let cost = calculate_query_cost(&mut order_query, &planner);

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
        cost.rows.unwrap() as f64,
        cost.startup_cost.unwrap(),
        cost.total_cost.unwrap(),
        ptr::null_mut(),
        ptr::null_mut(),
        into_fdw_private_path(planner.clone(), order_query.clone()),
    );
    pg_sys::add_path(outputrel, path as *mut _);

    (*outputrel).fdw_private = into_fdw_private_rel(ctx, order_query, planner.clone()) as *mut _;
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
    let (mut ctx, input_query, _) = from_fdw_private_rel((*inputrel).fdw_private as *mut _);

    // Only supported for select
    if (*parse).commandType != pg_sys::CmdType_CMD_SELECT {
        return;
    }

    // We cannot apply limit if conditions require local evaluation
    if !input_query.local_conds.is_empty() {
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

    // Invalidate retrieved rows so it can be estimated later
    limit_query.retrieved_rows = None;

    let mut query_ops = vec![];

    if let Some(offset) = offset {
        query_ops.push(SelectQueryOperation::SetRowOffset(offset));
    }

    if let Some(limit) = limit {
        query_ops.push(SelectQueryOperation::SetRowLimit(limit));
    }

    apply_query_operations(&mut ctx, &mut limit_query, query_ops.clone());

    // If failed to push down then abort
    if query_ops
        .iter()
        .any(|i| !limit_query.as_select().unwrap().remote_ops.contains(i))
    {
        return;
    }

    // Calculate the retrieved rows
    if limit_query.retrieved_rows.is_none() {
        let mut input_rows = input_query.retrieved_rows.unwrap();
        input_rows -= offset.unwrap_or(0);

        limit_query.retrieved_rows =
            Some(limit.map_or_else(|| input_rows, |limit| cmp::min(limit, input_rows)));
    }

    // The optimizer doesn't seem to like pushing down LIMIT clauses
    // we would want to do this most times so let's give it a bit of encouragement
    // by reducing the base connection cost for this path.
    if let Some(limit) = limit {
        limit_query.add_cost(|_, mut cost| {
            cost.startup_cost = cost.total_cost.map(|c| c - DEFAULT_FDW_STARTUP_COST * 0.5);

            cost.total_cost = cost.total_cost.map(|c| c - DEFAULT_FDW_STARTUP_COST * 0.5);

            cost
        });
    }

    let mut cost = calculate_query_cost(&mut limit_query, &planner);

    let path = pg_sys::create_foreign_upper_path(
        root,
        inputrel,
        (*root).upper_targets[pg_sys::UpperRelationKind_UPPERREL_FINAL as usize],
        cost.rows.unwrap() as f64,
        cost.startup_cost.unwrap(),
        cost.total_cost.unwrap(),
        ptr::null_mut(),
        ptr::null_mut(),
        into_fdw_private_path(planner.clone(), limit_query.clone()),
    );
    pg_sys::add_path(outputrel, path as *mut _);

    (*outputrel).fdw_private = into_fdw_private_rel(ctx, limit_query, planner.clone()) as *mut _;
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
    let (mut ctx, _, _) = from_fdw_private_rel((*foreignrel).fdw_private as *mut _);
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

    restore_query_state(&mut ctx, &query);

    // These checks are used the validate that tuple state is still expected when operating under
    // READ COMMITTED isolation level (EPQ = EvalPlanQual)
    let fdw_recheck_quals = if scan_relid > 0 {
        // In the case of base foreign rels we want to support EPQ checks so pull out the vars
        pg_sys::extract_actual_clauses(vec_to_pg_list(query.remote_conds.clone()), false)
    } else {
        // In the case of join/upper rels we assume EPQ will level never be required
        ptr::null_mut()
    };

    // First, pull out all cols/aggrefs required for the query (tlist, local conds and target expr's)
    let mut required_cols = pull_vars(
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
            .chain(PgList::<Node>::from_pg((*(*foreignrel).reltarget).exprs).iter_ptr())
            .chain(PgList::<Node>::from_pg(fdw_recheck_quals).iter_ptr()),
    );

    // If we find whole-row references we resolve those down to the columns in the base tables
    for (varno, rte) in find_whole_row_vars(root, &mut required_cols) {
        let rel = PgTable::open((*rte).relid as _, pg_sys::NoLock as _).unwrap();

        for att in rel.attrs() {
            required_cols.push(pg_sys::makeVar(
                varno,
                att.attnum,
                att.atttypid,
                att.atttypmod,
                att.attnum as _,
                0,
            ) as *mut Node);
        }
    }

    // If this in an update/delete command we will need to include the row id's
    if let pg_sys::CmdType_CMD_UPDATE | pg_sys::CmdType_CMD_DELETE = (*(*root).parse).commandType {
        let row_ids = match ctx.get_row_id_exprs(query.base_rel_alias()) {
            Ok(r) => r,
            Err(err) => panic!("Failed to get row ID's for table: {err}"),
        };

        let exprs = row_ids
            .into_iter()
            .enumerate()
            .map(|(idx, (expr, r#type))| {
                (
                    expr,
                    pg_sys::makeVar(
                        0, // TODO
                        pg_sys::SelfItemPointerAttributeNumber as _,
                        into_pg_type(&r#type).unwrap(),
                        -1,
                        pg_sys::InvalidOid,
                        0,
                    ) as *mut Node,
                )
            })
            .collect::<Vec<_>>();

        for (expr, col) in exprs {
            let col_alias = query.as_select_mut().unwrap().new_column_alias();
            let query_op = SelectQueryOperation::AddColumn((col_alias.clone(), expr));

            if apply_query_operation(&mut ctx, query.as_select_mut().unwrap(), query_op).is_none() {
                panic!("Failed to push down column required for local condition evaluation: rejected by remote");
            }

            let tle = pg_sys::makeTargetEntry(
                pg_sys::copyObjectImpl(col as *mut _) as *mut _,
                (fdw_scan_list.len() + 1) as _,
                ptr::null_mut(),
                true,
            );

            fdw_scan_list.push(tle as *mut _);
        }
    }

    for col in required_cols {
        // If we already have added this col for selection, skip it
        if fdw_scan_list
            .iter()
            .any(|i| pg_sys::equal((*i) as *mut _, col as *mut _))
        {
            continue;
        }

        // If this is a whole row reference we have already handled this earlier
        // so just add straight to the tlist
        if is_whole_row(col) {
            let tle = pg_sys::makeTargetEntry(
                pg_sys::copyObjectImpl(col as *mut _) as *mut _,
                (fdw_scan_list.len() + 1) as _,
                ptr::null_mut(),
                false,
            );

            fdw_scan_list.push(tle as *mut _);
            continue;
        }

        // Otherwise this is a standard column var or aggregate that needs to retrieved
        // from the data source
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

        let tle = pg_sys::makeTargetEntry(
            pg_sys::copyObjectImpl(col as *mut _) as *mut _,
            (fdw_scan_list.len() + 1) as _,
            ptr::null_mut(),
            false,
        );

        fdw_scan_list.push(tle as *mut _);
    }

    // Convert to pg list
    let fdw_scan_list = vec_to_pg_list(fdw_scan_list);

    // Ensure outer plan generates tuples with the matching desc
    let mut outer_plan = outer_plan;
    if !outer_plan.is_null() {
        outer_plan = pg_sys::change_plan_targetlist(
            outer_plan,
            fdw_scan_list,
            (*best_path).path.parallel_safe,
        );
    }

    let fdw_private = into_fdw_private_rel(ctx, query.clone(), planner.clone());

    pg_sys::make_foreignscan(
        tlist,
        pg_sys::extract_actual_clauses(vec_to_pg_list(query.local_conds.clone()), false),
        scan_relid,
        vec_to_pg_list(query.cvt.param_nodes()),
        fdw_private,
        fdw_scan_list,
        fdw_recheck_quals,
        outer_plan,
    )
}

unsafe fn find_whole_row_vars(
    root: *mut PlannerInfo,
    required_cols: &Vec<*mut Node>,
) -> Vec<(u32, *mut pg_sys::RangeTblEntry)> {
    required_cols
        .iter()
        .filter(|c| is_whole_row(**c))
        .map(|r| (*((*r) as *mut pg_sys::Var)).varno)
        .map(|varno| (varno, pg_sys::planner_rt_fetch(varno, root)))
        .collect()
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

unsafe fn is_self_item_ptr(node: *mut Node) -> bool {
    (*node).type_ == pg_sys::NodeTag_T_Var as u32
        && (*(node as *mut pg_sys::Var)).varattno == pg_sys::SelfItemPointerAttributeNumber as i16
}

unsafe fn is_whole_row(node: *mut Node) -> bool {
    (*node).type_ == pg_sys::NodeTag_T_Var as u32 && (*(node as *mut pg_sys::Var)).varattno == 0
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
    let (mut ctx, mut query, _) = from_fdw_private_rel((*plan).fdw_private);
    let mut scan = FdwScanContext::new();

    // Prepare the query for the chosen path
    restore_query_state(&mut ctx, &query);
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
    ctx.write_query_input(input_data).unwrap();
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

    // equivalent of ExecClearTuple(slot) (symbol is not exposed)
    (*(*slot).tts_ops).clear.unwrap()(slot);

    let attrs = (*tupdesc).attrs.as_slice(nattrs);
    (*slot).tts_values = pg_sys::palloc(nattrs * mem::size_of::<pg_sys::Datum>()) as *mut _;
    (*slot).tts_isnull = pg_sys::palloc(nattrs * mem::size_of::<bool>()) as *mut _;

    // Keep track of the column we are reading from the data source
    let mut col_idx = 0;
    let mut has_row_reference = false;

    for (attr_idx, attr) in attrs.iter().enumerate() {
        // If it's a whole row reference we dont need to perform anything here.
        // We first materalize the whole tuple slot then populate the attrs with copies
        if attr.atttypid == pg_sys::RECORDOID {
            has_row_reference = true;
            continue;
        }

        let data = ctx
            .read_result_data()
            .context("Failed to read data value")
            .unwrap();

        // Check if we have reached the last data value
        if data.is_none() {
            // If this is the first attribute we have reached the end so return an empty tuple
            if attr_idx == 0 {
                return slot;
            }

            // Else, we have a read a partial row, abort
            panic!("Unexpected EOF reached while reading next row");
        }

        // Convert the retrieved value to a pg datum and store in the tuple
        into_datum(
            attr.atttypid,
            &row_structure.cols[col_idx].1,
            data.unwrap(),
            (*slot).tts_isnull.add(attr_idx),
            (*slot).tts_values.add(attr_idx),
        )
        .unwrap();

        col_idx += 1;
    }

    // If there is a whole-row reference we materialise it here
    if has_row_reference {
        let econtext = (*node).ss.ps.ps_ExprContext;

        // Reconstruct row without system or record cols
        let tuple_datum = {
            let tupdesc = (*(*node).ss.ss_currentRelation).rd_att;
            let nattrs = (*tupdesc).natts as usize;
            let mut tts_values =
                pg_sys::palloc(nattrs * mem::size_of::<pg_sys::Datum>()) as *mut pg_sys::Datum;
            let mut tts_isnull = pg_sys::palloc(nattrs * mem::size_of::<bool>()) as *mut bool;
            let mut i = 0;

            for (attr_idx, attr) in attrs.iter().enumerate() {
                if attr_idx <= 1 { // TODO
                    continue;
                }

                *tts_values.add(i) = *(*slot).tts_values.add(attr_idx);
                *tts_isnull.add(i) = *(*slot).tts_isnull.add(attr_idx);

                i += 1;
            }

            let heap_tuple = pg_sys::heap_form_tuple(tupdesc, tts_values, tts_isnull);

            pg_sys::heap_copy_tuple_as_datum(heap_tuple, tupdesc)
        };

        for (attr_idx, attr) in attrs.iter().enumerate() {
            if attr.atttypid == pg_sys::RECORDOID {
                *(*slot).tts_isnull.add(attr_idx) = false;
                *(*slot).tts_values.add(attr_idx) = tuple_datum;
            }
        }
    }

    assert!(row_structure.cols.len() == col_idx);

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

    // For base foreign relations, it suffices to check fdw_recheck_quals
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
unsafe fn restore_query_state(ctx: &mut FdwContext, query: &FdwQueryContext) {
    let select = query.as_select().unwrap();

    // Initialise a new select query
    ctx.create_query(query.base_rel_alias(), sqlil::QueryType::Select)
        .unwrap();

    // We have already applied these ops to the query before but not on the
    // remote side
    // TODO: optimise so we dont perform duplicate work
    for query_op in select.remote_ops.iter() {
        ctx.apply_query_op(query_op.clone().into()).unwrap();
    }
}

// Generate a path cost estimation based on the supplied conditions
unsafe fn apply_query_operations(
    ctx: &mut FdwContext,
    query: &mut FdwQueryContext,
    new_query_ops: Vec<SelectQueryOperation>,
) {
    restore_query_state(ctx, query);

    let mut cost = None;

    // Apply each of the query operations and evaluate the cost
    for query_op in new_query_ops {
        if let Some(new_cost) = apply_query_operation(ctx, query.as_select_mut().unwrap(), query_op)
        {
            cost = Some(new_cost);
        }
    }

    if let Some(cost) = cost {
        if let Some(rows) = cost.rows {
            query.retrieved_rows = Some(rows);
        }
    }
}

fn apply_query_operation(
    ctx: &mut FdwContext,
    select: &mut FdwSelectQuery,
    query_op: SelectQueryOperation,
) -> Option<OperationCost> {
    let result = ctx.apply_query_op(query_op.clone().into()).unwrap();

    match result {
        QueryOperationResult::Ok(cost) => {
            select.remote_ops.push(query_op);
            Some(cost)
        }
        QueryOperationResult::Unsupported => None,
    }
}

unsafe fn apply_query_conds(
    ctx: &mut FdwContext,
    query: &mut FdwQueryContext,
    planner: &PlannerContext,
    conds: Vec<*mut RestrictInfo>,
) {
    let conds = conds
        .into_iter()
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

    apply_query_operations(ctx, query, conds.iter().map(|(i, _)| i).cloned().collect());

    for (cond, ri) in conds.into_iter() {
        if query.as_select().unwrap().remote_ops.contains(&cond) {
            query.remote_conds.push(ri);
        } else {
            query.local_conds.push(ri);
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

unsafe fn calculate_query_cost(
    query: &mut FdwQueryContext,
    planner: &PlannerContext,
) -> OperationCost {
    // TODO: store retrieved rows in query
    // use to calculate for join path
    // continue with other paths
    let mut cost = query.base_cost.clone();

    let (remote_sel, remote_qual_cost) = calculate_cond_costs(planner, query.remote_conds.clone());
    let (local_sel, local_qual_cost) = calculate_cond_costs(planner, query.local_conds.clone());

    let retrieved_rows = query
        .retrieved_rows
        .or(cost
            .rows
            .map(|rows| pg_sys::clamp_row_est(rows as f64 * remote_sel) as u64))
        .unwrap_or(DEFAULT_ROW_VOLUME) as f64;

    query.retrieved_rows = Some(retrieved_rows as u64);
    cost.rows = Some(pg_sys::clamp_row_est(retrieved_rows * local_sel) as u64);

    cost.startup_cost = cost
        .startup_cost
        .or(Some(DEFAULT_FDW_STARTUP_COST))
        .map(|c| (c + remote_qual_cost.startup + local_qual_cost.startup));

    cost.total_cost = Some(
        (cost.startup_cost.unwrap()
            + (retrieved_rows
                * (DEFAULT_FDW_TUPLE_COST + pg_sys::cpu_tuple_cost + local_qual_cost.per_tuple))),
    );

    let query_copy = query.clone();
    for cost_fn in query.cost_fns.iter_mut() {
        cost = cost_fn(&query_copy, cost);
    }

    cost
}

unsafe fn calculate_cond_costs(
    planner: &PlannerContext,
    conds: Vec<*mut RestrictInfo>,
) -> (f64, pg_sys::QualCost) {
    let base_relid = if let PlannerContext::BaseRel(rel) = planner {
        (*rel.base_rel).relid
    } else {
        0
    };

    let join_type = if let PlannerContext::JoinRel(join) = planner {
        join.join_type
    } else {
        pg_sys::JoinType_JOIN_INNER
    };

    let conds = vec_to_pg_list(conds);
    let selectivity = pg_sys::clauselist_selectivity(
        planner.root(),
        conds,
        base_relid as _,
        join_type,
        ptr::null_mut() as _,
    );

    let mut cost = pg_sys::QualCost::default();
    pg_sys::cost_qual_eval(&mut cost, conds, planner.root());

    (selectivity, cost)
}

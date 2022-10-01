//! Functions that provide the ability to perform "remote queries"
//! where supported by the connector. These are an escape hatch
//! to perform queries which are not supported by our FDW.
//!
//! SEC: Access to these functions means that a user could perform
//! any query against the data source, potentially bypassing any
//! security. Hence we disallow access to these functions by default
//! and make them opt-in using GRANT'ing EXECUTE access to them.
//! @see ansilo-pg/src/configure.rs for where the grants are configured.

use std::{
    collections::HashMap,
    mem, ptr,
    sync::{Arc, Mutex, Weak},
};

use ansilo_core::{
    data::{DataType, DataValue},
    err::{anyhow, Context, Error, Result},
    sqlil,
};
use ansilo_pg::fdw::{
    data::{QueryHandle, QueryHandleWriter, ResultSet, ResultSetReader},
    proto::{
        ClientMessage, ClientQueryMessage, QueryId, QueryInputStructure, ServerMessage,
        ServerQueryMessage,
    },
};
use lazy_static::lazy_static;
use pgx::{
    pg_sys::{Datum, FunctionCallInfo},
    *,
};

use crate::{
    fdw::{
        common::FdwIpcConnection,
        ctx::{mem::pg_transaction_scoped, FdwQueryHandle, FdwResultSet, QueryScopedConnection},
    },
    sqlil::{from_datum, into_datum},
};

#[cfg(any(test, feature = "pg_test"))]
mod tests;

// We also will cache prepared remote queries so they can be reused cheaply
//
// The cache key structure is (server_name, query_sql, param_types)
lazy_static! {
    static ref PREPARED_QUERIES: Mutex<
        HashMap<
            (String, String, Vec<DataType>),
            (Weak<FdwIpcConnection>, QueryId, QueryInputStructure),
        >,
    > = Mutex::new(HashMap::new());
}

extension_sql!(
    r#"
CREATE FUNCTION "remote_execute" (
	"server_name" text,
	"query" text,
    VARIADIC "params" "any"
) RETURNS bigint
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'remote_execute';

CREATE FUNCTION "remote_execute" (
	"server_name" text,
	"query" text
) RETURNS bigint
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'remote_execute';

CREATE FUNCTION "remote_query" (
	"server_name" text,
	"query" text,
    VARIADIC "params" "any"
) RETURNS setof record
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'remote_query';

CREATE FUNCTION "remote_query" (
	"server_name" text,
	"query" text
) RETURNS setof record
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'remote_query';
"#,
    name = "remote_query_functions"
);

#[pg_guard]
pub fn remote_execute(fcinfo: FunctionCallInfo) -> Option<i64> {
    let server_name = pg_getarg::<String>(fcinfo, 0).expect("server_name is null");
    let query = pg_getarg::<String>(fcinfo, 1).expect("query is null");
    let params = unsafe { parse_params(fcinfo, 2) }.unwrap();

    pgx::debug1!("Executing remote query: {}", query);

    try_remote_execute(server_name, query.clone(), params)
        .with_context(|| format!("Failed to execute remote query: '{query}'"))
        .unwrap()
}

#[no_mangle]
#[doc(hidden)]
pub extern "C" fn pg_finfo_remote_execute() -> &'static pg_sys::Pg_finfo_record {
    const V1_API: pg_sys::Pg_finfo_record = pg_sys::Pg_finfo_record { api_version: 1 };
    &V1_API
}

fn try_remote_execute(
    server_name: String,
    query: String,
    params: Vec<DataValue>,
) -> Result<Option<i64>> {
    let mut query = prepare_query(server_name, query, params)?;

    Ok(query.execute_modify()?.map(|i| i as i64))
}

#[pg_guard]
pub unsafe fn remote_query(fcinfo: FunctionCallInfo) -> Datum {
    // @see https://github.com/tcdi/pgx/blob/develop-v0.5.0/pgx-utils/src/rewriter.rs impl_table_srf
    struct ResultSetHolder {
        rs: *mut ResultSetReader<FdwResultSet>,
    }

    let mut funcctx: PgBox<pg_sys::FuncCallContext>;
    let mut rs_holder: PgBox<ResultSetHolder>;

    if srf_is_first_call(fcinfo) {
        // Get function args
        let server_name = pg_getarg::<String>(fcinfo, 0).expect("server_name is null");
        let query = pg_getarg::<String>(fcinfo, 1).expect("query is null");
        let params = parse_params(fcinfo, 2).unwrap();

        pgx::debug1!("Executing remote query: {}", query);

        // Init func context
        let mut funcctx = srf_first_call_init(fcinfo);
        funcctx.user_fctx = PgMemoryContexts::For(funcctx.multi_call_memory_ctx)
            .palloc_struct::<ResultSetHolder>() as void_mut_ptr;
        funcctx.tuple_desc = PgMemoryContexts::For(funcctx.multi_call_memory_ctx).switch_to(|_| {
            let mut tupdesc: *mut pg_sys::TupleDescData = std::ptr::null_mut();

            // Build a tuple descriptor for our result type
            if pg_sys::get_call_result_type(fcinfo, std::ptr::null_mut(), &mut tupdesc)
                != pg_sys::TypeFuncClass_TYPEFUNC_COMPOSITE
            {
                error!("return type must be a row type");
            }

            pg_sys::BlessTupleDesc(tupdesc)
        });

        rs_holder = PgBox::from_pg(funcctx.user_fctx as *mut ResultSetHolder);

        let result = PgMemoryContexts::For(funcctx.multi_call_memory_ctx).switch_to(|_| {
            try_remote_query(server_name, query.clone(), params)
                .with_context(|| format!("Failed to execute remote query: '{query}'"))
                .unwrap()
        });

        let req_atts = (*funcctx.tuple_desc).natts as usize;
        let actual_atts = result.get_structure().cols.len();
        
        if req_atts != actual_atts {
            pgx::error!("Failed to execute remote query: column count mismatch, defined {req_atts} columns on local query but remote query returned {actual_atts} columns");
        }

        rs_holder.rs = pgx::PgMemoryContexts::For(funcctx.multi_call_memory_ctx)
            .leak_and_drop_on_delete(result);
    }

    // Load reader from ctx
    funcctx = srf_per_call_setup(fcinfo);
    rs_holder = PgBox::from_pg(funcctx.user_fctx as *mut ResultSetHolder);

    // Read next row
    let row = match (*rs_holder.rs).read_row_vec() {
        Ok(Some(row)) => row,
        Ok(None) => {
            srf_return_done(fcinfo, &mut funcctx);
            return pg_return_null(fcinfo);
        }
        Err(err) => panic!("Failed to read from remote query: {:?}", err),
    };

    // Convert the row to a heap tuple
    let row_structure = (*rs_holder.rs).get_structure();
    let attrs = (*funcctx.tuple_desc)
        .attrs
        .as_slice(row_structure.cols.len());
    let datums = pg_sys::palloc(row.len() * mem::size_of::<Datum>()) as *mut Datum;
    let nulls = pg_sys::palloc(row.len() * mem::size_of::<bool>()) as *mut bool;

    for (idx, item) in row.into_iter().enumerate() {
        into_datum(
            attrs[idx].atttypid,
            &row_structure.cols[idx].1,
            item,
            nulls.add(idx),
            datums.add(idx),
        )
        .with_context(|| format!("Reading column '{}'", attrs[idx].name()))
        .unwrap();
    }

    let heap_tuple = pg_sys::heap_form_tuple(funcctx.tuple_desc, datums, nulls);

    // Finally convert the heap tuple into a datum and return
    let datum = pgx::heap_tuple_get_datum(heap_tuple);
    srf_return_next(fcinfo, &mut funcctx);
    Datum::from(datum)
}

#[no_mangle]
#[doc(hidden)]
pub extern "C" fn pg_finfo_remote_query() -> &'static pg_sys::Pg_finfo_record {
    const V1_API: pg_sys::Pg_finfo_record = pg_sys::Pg_finfo_record { api_version: 1 };
    &V1_API
}

fn try_remote_query(
    server_name: String,
    query: String,
    params: Vec<DataValue>,
) -> Result<ResultSetReader<FdwResultSet>> {
    let mut query = prepare_query(server_name, query, params)?;

    let result_set = query.execute_query()?;
    let reader = result_set.reader()?;

    Ok(reader)
}

/// Parses variadic parameters into a list of data values
unsafe fn parse_params(fcinfo: FunctionCallInfo, variadic_start: u32) -> Result<Vec<DataValue>> {
    // If this is a call without the variadic args
    if (*fcinfo).nargs <= variadic_start as _ {
        return Ok(vec![]);
    }

    // Extract the datums and types from the variadic param
    let mut datums = ptr::null_mut::<Datum>();
    let mut nulls = ptr::null_mut::<bool>();
    let mut types = ptr::null_mut::<pg_sys::Oid>();

    // Convert the datum into an array
    let args = pg_sys::extract_variadic_args(
        fcinfo,
        variadic_start as _,
        true,
        &mut datums as *mut _,
        &mut types as *mut _,
        &mut nulls as *mut _,
    );

    // If passed null, we assume that means no params
    if args < 0 {
        return Ok(vec![]);
    }

    // Convert the datums into data values
    let mut params = Vec::<DataValue>::with_capacity(args as _);
    for i in 0..(args as usize) {
        if *nulls.add(i) {
            params.push(DataValue::Null);
        } else {
            let val = from_datum(*types.add(i), *datums.add(i))
                .with_context(|| format!("Failed to parse arg #{}", i + 1))?;

            params.push(val);
        }
    }

    Ok(params)
}

/// Prepares the supplied query, sends all query params and returns the query handle
fn prepare_query(
    server_name: String,
    query: String,
    params: Vec<DataValue>,
) -> Result<FdwQueryHandle> {
    let param_types = params.iter().map(|p| p.r#type()).collect::<Vec<_>>();
    let param_exprs = param_types
        .iter()
        .enumerate()
        .map(|(id, p)| sqlil::Parameter::new(p.clone(), id as _))
        .collect::<Vec<_>>();

    // Now check if we have a cached prepared query that can be reused
    let cache_key = (server_name.clone(), query.clone(), param_types.clone());
    let entry = {
        PREPARED_QUERIES
            .lock()
            .expect("Failed to lock active prepared queries mutex")
            .get(&cache_key)
            .cloned()
    };

    let (con, query_input) = match entry {
        Some((con, query_id, query_input)) if con.upgrade().is_some() => {
            let con = QueryScopedConnection::new(query_id, con.upgrade().unwrap());

            // Lets restart the query so we can write new params
            let res = con
                .send(ClientQueryMessage::Restart)
                .context("Failed to restart query")?;

            match res {
                ServerQueryMessage::Restarted => {}
                _ => return Err(unexpected_response(res)).context("Failed to restart query"),
            }

            // Great, we can reuse the cached query
            (con, query_input.clone())
        }
        _ => {
            // No valid cache, we have to prepare a new query
            let con =
                unsafe { crate::fdw::common::try_connect_server_by_name(&server_name)?.connection };

            let res = con
                .send(ClientMessage::CreateStringQuery(query, param_exprs))
                .context("Failed to create remote query")?;

            let query_id = match res {
                ServerMessage::QueryCreated(id, _) => id,
                _ => {
                    return Err(unexpected_outer_response(res))
                        .context("Failed to create remote query")
                }
            };

            let con = QueryScopedConnection::new(query_id, con);

            let res = con
                .send(ClientQueryMessage::Prepare)
                .context("Failed to prepare remote query")?;

            let query_input = match res {
                ServerQueryMessage::Prepared(input) => input,
                _ => {
                    return Err(unexpected_response(res)).context("Failed to prepare remote query")
                }
            };

            // Save the prepared query in the cache for future reuse
            {
                PREPARED_QUERIES
                    .lock()
                    .expect("Failed to lock active prepared queries mutex")
                    .insert(
                        cache_key,
                        (
                            Arc::downgrade(&con.connection),
                            query_id,
                            query_input.clone(),
                        ),
                    );
            }

            // Keep the connection alive for this transaction
            // Otherwise it will get dropped prematurely and
            // prevent prepared queries being reused
            unsafe { pg_transaction_scoped(Arc::clone(&con.connection)) };

            (con, query_input)
        }
    };

    // Write the parameters to the query
    let mut writer = QueryHandleWriter::new(FdwQueryHandle::new(con.clone(), query_input.clone()))?;

    for (id, data_type) in query_input.params.iter() {
        // Since we defined the param id's as the index of the param we can simply
        // use them as indices here
        let param = params[(*id) as usize].clone();

        // If necessary, try coerce the param type
        let param = param.clone().try_coerce_into(data_type)
            .with_context(|| format!("Parameter type mismatch on remote query: on parameter #{} expecting type of {:?} but found {:?}", *id + 1, data_type, param.r#type()))?;

        // Write the param
        writer.write_data_value(param)?;
    }

    writer.flush()?;

    // Execute the query
    let query = writer.inner()?;

    Ok(query)
}

/// Clears all cached prepared queries
pub fn clear_rq_prepared_queries() {
    let mut cache = PREPARED_QUERIES
        .lock()
        .expect("Failed to lock active prepared queries mutext");

    cache.clear();
}

// Used for testing
#[allow(unused)]
pub(crate) fn get_prepared_queries_count() -> usize {
    PREPARED_QUERIES.lock().unwrap().len()
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

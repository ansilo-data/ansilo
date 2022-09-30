use ansilo_connectors_base::{common::query::QueryParam, interface::ResultSet};
use ansilo_connectors_native_postgres::PostgresQuery;
use ansilo_core::{
    data::{DataType, DataValue},
    err::Result,
    web::query::*,
};
use ansilo_logging::warn;
use axum::{extract::Json, Extension};
use hyper::StatusCode;
use itertools::Itertools;

use crate::middleware::pg_auth::ClientAuthenticatedPostgresConnection;

const ROW_LIMIT: usize = 1000;

enum SqlType {
    Query,
    Modify,
}

/// Executes a single sql query against postgres,
/// returning the results
pub(super) async fn handler(
    Extension(con): Extension<ClientAuthenticatedPostgresConnection>,
    Json(payload): Json<QueryRequest>,
) -> Result<(StatusCode, Json<QueryResponse>), (StatusCode, Json<QueryResponse>)> {
    let query_type = infer_query_type(&payload.sql);
    let mut con = con.0.lock().await;
    let mut query = con
        .prepare_async(PostgresQuery::new(
            payload.sql,
            payload
                .params
                .into_iter()
                .map(|p| QueryParam::Constant(DataValue::Utf8String(p)))
                .collect(),
        ))
        .await
        .map_err(|err| {
            (
                StatusCode::BAD_REQUEST,
                Json(QueryResponse::Error(err.to_string().into())),
            )
        })?;

    let (columns, data) = match query_type {
        SqlType::Query => {
            let results = query.execute_query_async().await.map_err(|err| {
                warn!("Query execute error: {:?}", err);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(QueryResponse::Error(err.to_string().into())),
                )
            })?;

            let cols = results.get_structure().map_err(|err| {
                warn!("Query read error: {:?}", err);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(QueryResponse::Error(err.to_string().into())),
                )
            })?;

            let columns = cols
                .cols
                .into_iter()
                .map(|(name, typ)| (name, typ.to_string()))
                .collect();

            let mut reader = results.reader().map_err(|err| {
                warn!("Query read error: {:?}", err);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(QueryResponse::Error(err.to_string().into())),
                )
            })?;

            let data = tokio::task::spawn_blocking(move || {
                Ok(reader
                    .iter_row_vecs()
                    .take(ROW_LIMIT)
                    .collect::<Result<Vec<_>>>()
                    .map_err(|err| {
                        warn!("Query read error: {:?}", err);
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(QueryResponse::Error(err.to_string().into())),
                        )
                    })?
                    .into_iter()
                    .map(|r| r.into_iter().map(|r| to_string(r)).collect_vec())
                    .collect())
            })
            .await
            .map_err(|err| {
                warn!("Query read error: {:?}", err);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(QueryResponse::Error(err.to_string().into())),
                )
            })??;

            (columns, data)
        }
        SqlType::Modify => {
            let affected_rows = query.execute_modify_async().await.map_err(|err| {
                warn!("Query execute error: {:?}", err);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(QueryResponse::Error(err.to_string().into())),
                )
            })?;

            match affected_rows {
                Some(rows) => {
                    let columns = vec![("affected_rows".to_string(), DataType::Int64.to_string())];
                    let data = vec![vec![rows.to_string()]];

                    (columns, data)
                }
                None => {
                    let columns =
                        vec![("message".to_string(), DataType::rust_string().to_string())];
                    let data = vec![vec!["Command completed successfully".to_string()]];

                    (columns, data)
                }
            }
        }
    };

    Ok((
        StatusCode::OK,
        Json(QueryResponse::Success(QueryResults { columns, data })),
    ))
}

/// Try infer the type of query
/// We take a best-effort approach as of now.
/// A solid approach would be to support retreiving the postgres protocol repsonses
/// which could contain notifications for result sets, modifications all in one.
fn infer_query_type(sql: &str) -> SqlType {
    // @see https://www.postgresql.org/docs/current/sql-commands.html
    let modify_keywords = [
        "update", "delete", "merge", "insert", "truncate", "alter", "drop", "create", "set", "lock", "discard",
    ];
    let query_keywords = ["select", "explain", "fetch"];

    let sql = sql.to_ascii_lowercase();

    let modify_idx = modify_keywords.iter().filter_map(|k| sql.find(*k)).min();

    let query_idx = query_keywords.iter().filter_map(|k| sql.find(*k)).min();

    match (modify_idx, query_idx) {
        (None, None) => SqlType::Query,
        (None, Some(_)) => SqlType::Query,
        (Some(_), None) => SqlType::Modify,
        (Some(m), Some(q)) => {
            if m < q {
                SqlType::Modify
            } else {
                SqlType::Query
            }
        }
    }
}

fn to_string(data: DataValue) -> String {
    match data {
        DataValue::Binary(data) => hex::encode(data),
        _ => match data.try_coerce_into(&DataType::rust_string()).unwrap() {
            DataValue::Utf8String(s) => s,
            DataValue::Null => "NULL".into(),
            _ => unreachable!(),
        },
    }
}

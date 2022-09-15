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

/// Executes a single sql query against postgres,
/// returning the results
pub(super) async fn handler(
    Extension(con): Extension<ClientAuthenticatedPostgresConnection>,
    Json(payload): Json<QueryRequest>,
) -> Result<(StatusCode, Json<QueryResponse>), (StatusCode, Json<QueryResponse>)> {
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

    // TODO[low]: detect modify queries and return row count
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

    Ok((
        StatusCode::OK,
        Json(QueryResponse::Success(QueryResults { columns, data })),
    ))
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

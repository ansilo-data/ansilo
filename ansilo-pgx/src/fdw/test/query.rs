use pgx::{FromDatum, Json, Spi, SpiHeapTupleData};
use serde::{de::DeserializeOwned, Serialize};

pub(crate) fn execute_query<
    F: Fn(SpiHeapTupleData) -> R,
    R: DeserializeOwned + Serialize + Clone,
>(
    query: impl Into<String>,
    f: F,
) -> Vec<R> {
    let query = query.into();
    let json = Spi::connect(|client| {
        let res = client
            .select(query.as_str(), None, None)
            .map(f)
            .collect::<Vec<R>>();
        let res = serde_json::to_string(&res).unwrap();

        Ok(Some(res))
    })
    .unwrap();

    serde_json::from_str(json.as_str()).unwrap()
}

pub(crate) fn execute_modify<R: DeserializeOwned + Serialize + Clone + FromDatum>(
    query: impl Into<String>,
) -> R {
    let query = query.into();
    let json = Spi::connect(|mut client| {
        let res = client.update(query.as_str(), None, None).first().get_one::<R>();
        let res = serde_json::to_string(&res).unwrap();

        Ok(Some(res))
    })
    .unwrap();

    serde_json::from_str(json.as_str()).unwrap()
}

pub(crate) fn explain_query(query: impl Into<String>) -> serde_json::Value {
    explain_query_opt(query, false)
}

pub(crate) fn explain_query_verbose(query: impl Into<String>) -> serde_json::Value {
    explain_query_opt(query, true)
}

fn explain_query_opt(query: impl Into<String>, verbose: bool) -> serde_json::Value {
    let query = query.into();
    let json = Spi::connect(|mut client| {
        let table = client
            .update(
                &format!(
                    "EXPLAIN (format json, verbose {}) {}",
                    if verbose { "true" } else { "false" },
                    query.as_str()
                ),
                None,
                None,
            )
            .first();
        Ok(Some(
            table
                .get_one::<Json>()
                .expect("failed to get json EXPLAIN result"),
        ))
    })
    .unwrap();

    json.0.as_array().take().unwrap().get(0).unwrap().clone()
}

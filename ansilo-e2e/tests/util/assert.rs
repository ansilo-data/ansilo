use std::collections::HashMap;

use ansilo_core::data::DataValue;
use itertools::Itertools;
use pretty_assertions::assert_eq;

#[track_caller]
pub fn assert_rows_equal(
    left: Vec<HashMap<String, DataValue>>,
    right: Vec<HashMap<String, DataValue>>,
) {
    let left = normalise_rows(left);
    let right = normalise_rows(right);

    assert_eq!(left, right);
}

fn normalise_rows(rows: Vec<HashMap<String, DataValue>>) -> Vec<Vec<(String, DataValue)>> {
    rows.into_iter()
        .into_iter()
        .map(|row| row.into_iter().sorted_by_key(|i| i.0.clone()).collect_vec())
        .collect_vec()
}

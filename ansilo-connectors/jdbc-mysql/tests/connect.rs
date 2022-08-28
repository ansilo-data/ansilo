use ansilo_connectors_base::{
    common::data::ResultSetReader,
    interface::{Connection, QueryHandle},
};

use ansilo_connectors_jdbc_base::JdbcQuery;
use ansilo_core::data::DataValue;

mod common;

#[test]
fn test_oracle_jdbc_open_connection_and_execute_query() {
    let containers = common::start_oracle();
    let mut con = common::connect_to_oracle(&containers);

    let mut query = con
        .prepare(JdbcQuery::new("SELECT * FROM DUAL", vec![]))
        .unwrap();
    let res = query.execute().unwrap();

    let mut res = ResultSetReader::new(res).unwrap();

    assert_eq!(
        res.read_data_value().unwrap(),
        Some(DataValue::Utf8String("X".into()))
    );
    assert_eq!(res.read_data_value().unwrap(), None);
}

use ansilo_connectors_base::{
    common::data::ResultSetReader,
    interface::{Connection, QueryHandle},
};

use ansilo_connectors_jdbc_base::JdbcQuery;
use ansilo_core::data::DataValue;

mod common;

#[test]
fn test_mysql_jdbc_open_connection_and_execute_query() {
    let containers = common::start_mysql();
    let mut con = common::connect_to_mysql(&containers);

    let mut query = con
        .prepare(JdbcQuery::new("SELECT 1", vec![]))
        .unwrap();
    let res = query.execute().unwrap();

    let mut res = ResultSetReader::new(res).unwrap();

    assert_eq!(
        res.read_data_value().unwrap(),
        Some(DataValue::Int64(1))
    );
    assert_eq!(res.read_data_value().unwrap(), None);
}

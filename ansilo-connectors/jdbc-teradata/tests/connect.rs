use ansilo_connectors_base::{
    common::data::ResultSetReader,
    interface::{Connection, QueryHandle},
};

use ansilo_connectors_jdbc_base::JdbcQuery;
use ansilo_core::data::DataValue;

mod common;

#[test]
fn test_teradata_jdbc_open_connection_and_execute_query() {
    common::start_teradata();
    let mut con = common::connect_to_teradata();

    let mut query = con.prepare(JdbcQuery::new("SELECT 1", vec![])).unwrap();
    let res = query.execute_query().unwrap();

    let mut res = ResultSetReader::new(res).unwrap();

    assert_eq!(res.read_data_value().unwrap(), Some(DataValue::Int8(1)));
    assert_eq!(res.read_data_value().unwrap(), None);
}

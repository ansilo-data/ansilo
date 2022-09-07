use ansilo_connectors_base::common::data::ResultSetReader;

use ansilo_core::data::DataValue;

mod common;

#[test]
fn test_postgres_open_connection_and_execute_query() {
    let containers = common::start_postgres();
    let mut con = common::connect_to_postgres(&containers);

    let res = con.execute("SELECT 1", vec![]).unwrap();
    let mut res = ResultSetReader::new(res).unwrap();

    assert_eq!(res.read_data_value().unwrap(), Some(DataValue::Int32(1)));
    assert_eq!(res.read_data_value().unwrap(), None);
}

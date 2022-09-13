use ansilo_connectors_base::common::data::ResultSetReader;

use ansilo_core::data::DataValue;

mod common;

#[test]
fn test_sqlite_open_connection_and_execute_query() {
    let mut con = common::connect_to_sqlite();

    let res = con.execute("SELECT 1", vec![]).unwrap();
    let mut res = ResultSetReader::new(res).unwrap();

    // Since the column wont be typed it will arrive as a binary utf8 string
    assert_eq!(
        res.read_data_value().unwrap(),
        Some(DataValue::Binary(b"1".to_vec()))
    );
    assert_eq!(res.read_data_value().unwrap(), None);
}

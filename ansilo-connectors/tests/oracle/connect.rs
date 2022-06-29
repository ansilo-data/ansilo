use std::collections::HashMap;

use ansilo_connectors::{
    common::data::ResultSetReader,
    interface::{Connection, ConnectionOpener, Connector, QueryHandle},
    jdbc::JdbcQuery,
    jdbc_oracle::{OracleJdbcConnectionConfig, OracleJdbcConnector},
};
use ansilo_core::{common::data::DataValue, config::NodeConfig};

use crate::oracle::start_oracle;

#[test]
fn test_oracle_jdbc_open_connection_and_execute_query() {
    let containers = start_oracle();

    let config = OracleJdbcConnectionConfig::new(
        format!(
            "jdbc:oracle:thin:@{}:1521/db",
            containers.services.get("oracle").unwrap()
        ),
        {
            let mut props = HashMap::<String, String>::new();
            props.insert("oracle.jdbc.user".to_string(), "ansilo_admin".to_string());
            props.insert(
                "oracle.jdbc.password".to_string(),
                "ansilo_testing".to_string(),
            );
            props
        },
    );

    let con = OracleJdbcConnector::create_connection_opener(&config)
        .unwrap()
        .open(config, &NodeConfig::default())
        .unwrap();
    let mut query = con
        .prepare(JdbcQuery::new("SELECT * FROM DUAL", vec![]))
        .unwrap();
    let res = query.execute().unwrap();
    let mut res = ResultSetReader::new(res).unwrap();

    assert_eq!(
        res.read_data_value().unwrap(),
        Some(DataValue::Varchar("X".as_bytes().to_vec()))
    );
    assert_eq!(res.read_data_value().unwrap(), None);
}

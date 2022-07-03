use std::{collections::HashMap, env};

use ansilo_connectors::{
    common::data::ResultSetReader,
    interface::{Connection, ConnectionPool, Connector, QueryHandle},
    jdbc::JdbcQuery,
    jdbc_oracle::{OracleJdbcConnectionConfig, OracleJdbcConnector},
};
use ansilo_core::{common::data::DataValue, config::NodeConfig};

use crate::{oracle::start_oracle, common::get_current_target_dir};

#[test]
fn test_oracle_jdbc_open_connection_and_execute_query() {
    env::set_var("ANSILO_CLASSPATH", get_current_target_dir().to_str().unwrap());

    let containers = start_oracle();

    let config = OracleJdbcConnectionConfig::new(
        format!(
            "jdbc:oracle:thin:@{}:1522/db",
            containers.get("oracle").unwrap().ip
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
        None
    );

    let con = OracleJdbcConnector::create_connection_pool(config.clone(), &NodeConfig::default())
        .unwrap()
        .acquire()
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

use ansilo_connectors::{
    common::entity::{ConnectorEntityConfig, EntitySource},
    interface::{Connection, ConnectionPool, Connector, QueryHandle},
    memory::{MemoryConnectionConfig, MemoryConnector, MemoryQuery, MemoryResultSet, MemoryConnectorEntitySourceConfig},
};
use ansilo_core::{
    data::{DataType, DataValue},
    config::{EntityAttributeConfig, EntitySourceConfig, EntityVersionConfig, NodeConfig},
    sqlil,
};

fn mock_data() -> (ConnectorEntityConfig<MemoryConnectorEntitySourceConfig>, MemoryConnectionConfig) {
    let conf = MemoryConnectionConfig::new();
    let mut entities = ConnectorEntityConfig::new();

    entities.add(EntitySource::minimal(
        "people",
        EntityVersionConfig::minimal(
            "1.0",
            vec![
                EntityAttributeConfig::minimal("first_name", DataType::rust_string()),
                EntityAttributeConfig::minimal("last_name", DataType::rust_string()),
            ],
            EntitySourceConfig::minimal(""),
        ),
        MemoryConnectorEntitySourceConfig::default()
    ));

    conf.set_data(
        "people",
        "1.0",
        vec![
            vec![DataValue::from("Mary"), DataValue::from("Jane")],
            vec![DataValue::from("John"), DataValue::from("Smith")],
            vec![DataValue::from("Gary"), DataValue::from("Gregson")],
        ],
    );

    (entities, conf)
}

#[test]
fn test_memory_select_query_execution() {
    let (entities, data) = mock_data();

    let mut pool =
        MemoryConnector::create_connection_pool(data, &NodeConfig::default(), &entities).unwrap();

    let connection = pool.acquire().unwrap();

    let mut select = sqlil::Select::new(sqlil::source("people", "1.0", "people"));
    select.cols.push((
        "first_name".to_string(),
        sqlil::Expr::attr("people", "first_name"),
    ));
    select.cols.push((
        "last_name".to_string(),
        sqlil::Expr::attr("people", "last_name"),
    ));

    select
        .r#where
        .push(sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
            sqlil::Expr::attr("people", "first_name"),
            sqlil::BinaryOpType::Equal,
            sqlil::Expr::Constant(sqlil::Constant::new(DataValue::from("Gary"))),
        )));

    let mut query = connection
        .prepare(MemoryQuery::new(select.into(), vec![]))
        .unwrap();

    let res = query.execute().unwrap();

    assert_eq!(
        res,
        MemoryResultSet::new(
            vec![
                ("first_name".to_string(), DataType::rust_string()),
                ("last_name".to_string(), DataType::rust_string())
            ],
            vec![vec![DataValue::from("Gary"), DataValue::from("Gregson")]]
        )
        .unwrap()
    )
}

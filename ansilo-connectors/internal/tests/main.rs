use ansilo_connectors_base::{
    common::entity::ConnectorEntityConfig,
    interface::{ConnectionPool, Connector, QueryCompiler, QueryHandle},
};
use ansilo_connectors_internal::{InternalConnector, InternalQueryCompiler, InternalResultSet};
use ansilo_core::{
    config::{
        ConstantServiceUserPassword, CronTriggerConfig, JobConfig, JobTriggerConfig, NodeConfig,
        ServiceUserConfig, ServiceUserPasswordMethod,
    },
    data::{DataType, DataValue},
    sqlil,
};
use pretty_assertions::assert_eq;

#[test]
fn test_internal_select_jobs() {
    let mut nc = NodeConfig::default();

    nc.jobs.push(JobConfig {
        id: "job1".into(),
        name: Some("Job 1".into()),
        description: None,
        service_user_id: None,
        sql: "SQL".into(),
        triggers: vec![],
    });

    nc.jobs.push(JobConfig {
        id: "job2".into(),
        name: Some("Job 2".into()),
        description: None,
        service_user_id: None,
        sql: "SQL".into(),
        triggers: vec![],
    });

    let mut pool =
        InternalConnector::create_connection_pool((), &nc, &ConnectorEntityConfig::new()).unwrap();

    let mut connection = pool.acquire(None).unwrap();

    let mut select = sqlil::Select::new(sqlil::source("jobs", "j"));
    select
        .cols
        .push(("id".to_string(), sqlil::Expr::attr("j", "id")));
    select
        .cols
        .push(("name".to_string(), sqlil::Expr::attr("j", "name")));
    select
        .cols
        .push(("sql".to_string(), sqlil::Expr::attr("j", "sql")));

    let mut query = InternalQueryCompiler::compile_query(
        &mut connection,
        &ConnectorEntityConfig::new(),
        select.into(),
    )
    .unwrap();

    let res = query.execute_query().unwrap();

    assert_eq!(
        res,
        InternalResultSet::new(
            vec![
                ("id".to_string(), DataType::rust_string()),
                ("name".to_string(), DataType::rust_string()),
                ("sql".to_string(), DataType::rust_string())
            ],
            vec![
                DataValue::from("job1"),
                DataValue::from("Job 1"),
                DataValue::from("SQL"),
                //
                DataValue::from("job2"),
                DataValue::from("Job 2"),
                DataValue::from("SQL"),
            ]
        )
        .unwrap()
    )
}

#[test]
fn test_internal_select_job_triggers() {
    let mut nc = NodeConfig::default();

    nc.jobs.push(JobConfig {
        id: "job1".into(),
        name: Some("Job 1".into()),
        description: None,
        service_user_id: None,
        sql: "SQL".into(),
        triggers: vec![
            JobTriggerConfig::Cron(CronTriggerConfig {
                cron: "cron 1".into(),
            }),
            JobTriggerConfig::Cron(CronTriggerConfig {
                cron: "cron 2".into(),
            }),
        ],
    });

    let mut pool =
        InternalConnector::create_connection_pool((), &nc, &ConnectorEntityConfig::new()).unwrap();

    let mut connection = pool.acquire(None).unwrap();

    let mut select = sqlil::Select::new(sqlil::source("job_triggers", "j"));
    select
        .cols
        .push(("job_id".to_string(), sqlil::Expr::attr("j", "job_id")));
    select
        .cols
        .push(("cron".to_string(), sqlil::Expr::attr("j", "cron")));

    let mut query = InternalQueryCompiler::compile_query(
        &mut connection,
        &ConnectorEntityConfig::new(),
        select.into(),
    )
    .unwrap();

    let res = query.execute_query().unwrap();

    assert_eq!(
        res,
        InternalResultSet::new(
            vec![
                ("job_id".to_string(), DataType::rust_string()),
                ("cron".to_string(), DataType::rust_string())
            ],
            vec![
                DataValue::from("job1"),
                DataValue::from("cron 1"),
                //
                DataValue::from("job1"),
                DataValue::from("cron 2"),
            ]
        )
        .unwrap()
    )
}

#[test]
fn test_internal_select_service_users() {
    let mut nc = NodeConfig::default();

    nc.auth.service_users.push(ServiceUserConfig::new(
        "svc1".into(),
        "svc1".into(),
        None,
        ServiceUserPasswordMethod::Constant(ConstantServiceUserPassword {
            password: "pw".into(),
        }),
    ));

    nc.auth.service_users.push(ServiceUserConfig::new(
        "svc2".into(),
        "svc2".into(),
        None,
        ServiceUserPasswordMethod::Constant(ConstantServiceUserPassword {
            password: "pw".into(),
        }),
    ));

    let mut pool =
        InternalConnector::create_connection_pool((), &nc, &ConnectorEntityConfig::new()).unwrap();

    let mut connection = pool.acquire(None).unwrap();

    let mut select = sqlil::Select::new(sqlil::source("service_users", "j"));
    select
        .cols
        .push(("id".to_string(), sqlil::Expr::attr("j", "id")));
    select.cols.push((
        "description".to_string(),
        sqlil::Expr::attr("j", "description"),
    ));

    let mut query = InternalQueryCompiler::compile_query(
        &mut connection,
        &ConnectorEntityConfig::new(),
        select.into(),
    )
    .unwrap();

    let res = query.execute_query().unwrap();

    assert_eq!(
        res,
        InternalResultSet::new(
            vec![
                ("id".to_string(), DataType::rust_string()),
                ("description".to_string(), DataType::rust_string())
            ],
            vec![
                DataValue::from("svc1"),
                DataValue::Null,
                //
                DataValue::from("svc2"),
                DataValue::Null,
            ]
        )
        .unwrap()
    )
}

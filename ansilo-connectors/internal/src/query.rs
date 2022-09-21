use std::str::FromStr;

use ansilo_connectors_base::interface::{LoggedQuery, QueryHandle, QueryInputStructure};
use ansilo_core::{
    config::{JobTriggerConfig, NodeConfig},
    data::{DataType, DataValue},
    err::{bail, Error, Result},
};
use itertools::Itertools;
use serde::Serialize;

use crate::InternalResultSet;

#[derive(Clone, Debug, Serialize)]
pub struct InternalQuery {
    pub nc: &'static NodeConfig,
    pub query: InternalQueryType,
}

#[derive(Clone, Debug, Serialize)]
pub enum InternalQueryType {
    Job(Vec<(String, JobColumn)>),
    JobTrigger(Vec<(String, JobTriggerColumn)>),
    ServiceUser(Vec<(String, ServiceUserColumn)>),
}

#[derive(Clone, Copy, Debug, Serialize)]
pub enum JobColumn {
    Id,
    Name,
    Description,
    ServiceUserId,
    Sql,
}

impl FromStr for JobColumn {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "id" => Self::Id,
            "name" => Self::Name,
            "description" => Self::Description,
            "service_user_id" => Self::ServiceUserId,
            "sql" => Self::Sql,
            _ => bail!("Unsupported"),
        })
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
pub enum JobTriggerColumn {
    JobId,
    Cron,
}

impl FromStr for JobTriggerColumn {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "job_id" => Self::JobId,
            "cron" => Self::Cron,
            _ => bail!("Unsupported"),
        })
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
pub enum ServiceUserColumn {
    Id,
    Username,
    Description,
}

impl FromStr for ServiceUserColumn {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "id" => Self::Id,
            "username" => Self::Username,
            "description" => Self::Description,
            _ => bail!("Unsupported"),
        })
    }
}

impl QueryHandle for InternalQuery {
    type TResultSet = InternalResultSet;

    fn get_structure(&self) -> Result<QueryInputStructure> {
        Ok(QueryInputStructure::new(vec![]))
    }

    fn write(&mut self, _buff: &[u8]) -> Result<usize> {
        bail!("Unsupported")
    }

    fn restart(&mut self) -> Result<()> {
        Ok(())
    }

    fn execute_query(&mut self) -> Result<Self::TResultSet> {
        let data: Vec<Option<String>> = match &self.query {
            InternalQueryType::Job(cols) => self
                .nc
                .jobs
                .iter()
                .flat_map(|job| {
                    cols.iter().map(|(_, c)| match c {
                        JobColumn::Id => Some(job.id.clone()),
                        JobColumn::Name => job.name.clone(),
                        JobColumn::Description => job.description.clone(),
                        JobColumn::ServiceUserId => job.service_user_id.clone(),
                        JobColumn::Sql => Some(job.sql.clone()),
                    })
                })
                .collect(),
            InternalQueryType::JobTrigger(cols) => self
                .nc
                .jobs
                .iter()
                .flat_map(|job| {
                    job.triggers.iter().flat_map(|trigger| {
                        cols.iter()
                            .map(|(_, c)| match c {
                                JobTriggerColumn::JobId => Some(job.id.clone()),
                                JobTriggerColumn::Cron => match trigger {
                                    JobTriggerConfig::Cron(c) => Some(c.cron.clone()),
                                },
                            })
                            .collect_vec()
                    })
                })
                .collect(),
            InternalQueryType::ServiceUser(cols) => self
                .nc
                .auth
                .service_users
                .iter()
                .flat_map(|user| {
                    cols.iter().map(|(_, c)| match c {
                        ServiceUserColumn::Id => Some(user.id().to_string()),
                        ServiceUserColumn::Username => Some(user.username.clone()),
                        ServiceUserColumn::Description => user.description.clone(),
                    })
                })
                .collect(),
        };

        let cols: Vec<_> = match &self.query {
            InternalQueryType::Job(cols) => cols
                .iter()
                .map(|(a, _)| (a.clone(), DataType::rust_string()))
                .collect(),
            InternalQueryType::JobTrigger(cols) => cols
                .iter()
                .map(|(a, _)| (a.clone(), DataType::rust_string()))
                .collect(),
            InternalQueryType::ServiceUser(cols) => cols
                .iter()
                .map(|(a, _)| (a.clone(), DataType::rust_string()))
                .collect(),
        };

        let data = data
            .into_iter()
            .map(|d| match d {
                Some(d) => DataValue::Utf8String(d),
                None => DataValue::Null,
            })
            .collect::<Vec<_>>();

        InternalResultSet::new(cols, data)
    }

    fn execute_modify(&mut self) -> Result<Option<u64>> {
        bail!("Unsupported")
    }

    fn logged(&self) -> Result<LoggedQuery> {
        Ok(LoggedQuery::new_query(serde_json::to_string(&self.query)?))
    }
}

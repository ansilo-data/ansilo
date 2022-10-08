use serde::{Serialize, Deserialize};

/// A job is a pre-defined query which can be triggered repeatedly
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct JobConfig {
    /// The ID of the job
    pub id: String,
    /// The name of the job
    pub name: Option<String>,
    /// The description of the job
    pub description: Option<String>,
    /// The ID of the service user to authenticate as
    /// If not provided it will be executed as ansilo_admin
    pub service_user: Option<String>,
    /// The query/queries that are executed by the job
    pub sql: String,
    /// The trigger conditions for the job
    #[serde(default)]
    pub triggers: Vec<JobTriggerConfig>,
}

/// A trigger condition for a job
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum JobTriggerConfig {
    Cron(CronTriggerConfig),
}

/// A trigger which runs on a scheduled defined by a cron expression
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct CronTriggerConfig {
    /// The cron expression
    pub cron: String
}


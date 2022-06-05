use serde::{Serialize, Deserialize};

/// A job is a pre-defined query which can be triggered repeatedly
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct JobConfig {
    /// The ID of the job
    pub id: String,
    /// The name of the job
    pub name: String,
    /// The description of the job
    pub description: String,
    /// The query that is executed by the job
    pub query: JobQueryConfig,
    /// The trigger conditions for the job
    pub triggers: Vec<JobTriggerConfig>,
}

/// The query run by a job
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct JobQueryConfig {
    /// The SQL query as a string
    pub sql: String,
}

/// A trigger condition for a job
/// TODO: Options for structuring DAG's
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum JobTriggerConfig {
    Cron(CronTriggerConfig),
}

/// A trigger which runs on a scheduled defined by a cron expression
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct CronTriggerConfig {
    /// The cron expression
    pub cron_expression: String
}


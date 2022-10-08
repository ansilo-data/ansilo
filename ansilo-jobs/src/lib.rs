use ansilo_core::{
    config::{JobConfig, JobTriggerConfig},
    err::{Context, Result},
};
use ansilo_logging::{error, info, warn};
use ansilo_pg::handler::PostgresConnectionHandler;
use tokio::runtime::Handle;

use crate::job::Job;

pub mod job;

/// The entrypoint to the job scheduler subsystem
pub struct JobScheduler {
    /// The tokio runtime handle
    runtime: Handle,
    /// The postgres connection handler
    inner: Inner,
}

/// Inner state for async methods
struct Inner {
    /// The list of configured jobs
    jobs: &'static Vec<JobConfig>,
    /// The postgres connection handler
    pg: PostgresConnectionHandler,
    /// The inner scheduler instance
    scheduler: Option<tokio_cron_scheduler::JobScheduler>,
}

impl JobScheduler {
    pub fn new(
        jobs: &'static Vec<JobConfig>,
        runtime: Handle,
        pg: PostgresConnectionHandler,
    ) -> Self {
        Self {
            runtime,
            inner: Inner {
                jobs,
                pg,
                scheduler: None,
            },
        }
    }

    /// Start the job scheduler
    pub fn start(&mut self) -> Result<()> {
        self.runtime.block_on(self.inner.start())
    }

    /// Checks whether the scheduler is healthy
    pub fn healthy(&self) -> bool {
        match &self.inner.scheduler {
            // This heuristic could be improved
            Some(_s) => true,
            None => false,
        }
    }

    /// Terminate the job scheduler
    pub fn terminate(mut self) -> Result<()> {
        self.terminate_mut()
    }

    fn terminate_mut(&mut self) -> Result<()> {
        self.runtime.block_on(self.inner.terminate_mut())
    }
}

impl Drop for JobScheduler {
    fn drop(&mut self) {
        if let Err(err) = self.terminate_mut() {
            warn!("Failed to terminate job scheduler: {:?}", err);
        }
    }
}

impl Inner {
    async fn start(&mut self) -> Result<()> {
        let scheduler = tokio_cron_scheduler::JobScheduler::new().await?;

        for job in self.jobs.iter() {
            for trigger in job.triggers.iter() {
                let cron = match trigger {
                    JobTriggerConfig::Cron(c) => &c.cron,
                };

                info!("Installing job '{}' for schedule {}", job.id, cron);

                scheduler
                    .add(Job::new(job, self.pg.clone()).to_scheduler_job(&cron)?)
                    .await?;
            }
        }

        {
            let scheduler = scheduler.clone();
            tokio::spawn(async move {
                if let Err(e) = scheduler.start().await {
                    error!("Error occurred while running job scheduler: {:?}", e);
                }
            });
        }

        self.scheduler = Some(scheduler);
        Ok(())
    }

    async fn terminate_mut(&mut self) -> Result<()> {
        if self.scheduler.is_none() {
            return Ok(());
        }

        let mut scheduler = self.scheduler.take().unwrap();
        scheduler
            .shutdown()
            .await
            .context("Failed to shutdown scheduler")?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    use ansilo_auth::Authenticator;
    use ansilo_core::config::{AuthConfig, CronTriggerConfig};
    use ansilo_pg::{
        connection::PostgresConnection, handler::test::init_pg_handler, PostgresInstance,
    };

    pub fn mock_auth_empty() -> Authenticator {
        let conf = Box::leak(Box::new(AuthConfig::default()));

        Authenticator::init(conf).unwrap()
    }

    async fn query(instance: &mut PostgresInstance) -> PostgresConnection {
        instance.connections().admin().await.unwrap()
    }

    #[tokio::test]
    async fn test_job_scheduler_start_and_shutdown_empty() {
        ansilo_logging::init_for_tests();
        let (_instance, pg) = init_pg_handler("job-scheduler-empty", mock_auth_empty()).await;

        let mut scheduler = JobScheduler::new(
            Box::leak(Box::new(vec![])),
            tokio::runtime::Handle::current(),
            pg,
        );

        tokio::task::spawn_blocking(move || {
            scheduler.start().unwrap();
            scheduler.terminate().unwrap();
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_job_scheduler_start_and_shutdown_with_single_job() {
        ansilo_logging::init_for_tests();
        let (mut instance, pg) =
            init_pg_handler("job-scheduler-single-job", mock_auth_empty()).await;

        query(&mut instance)
            .await
            .batch_execute("CREATE TABLE job AS SELECT 0 as runs")
            .await
            .unwrap();

        // Increment the counter every second
        let mut scheduler = JobScheduler::new(
            Box::leak(Box::new(vec![JobConfig {
                id: "test".into(),
                name: None,
                description: None,
                service_user: None,
                sql: "UPDATE job SET runs = runs + 1".into(),
                triggers: vec![JobTriggerConfig::Cron(CronTriggerConfig {
                    cron: "* * * * * *".into(),
                })],
            }])),
            tokio::runtime::Handle::current(),
            pg,
        );

        tokio::task::spawn_blocking(move || {
            scheduler.start().unwrap();
            std::thread::sleep(Duration::from_secs(5));
            scheduler.terminate().unwrap();
        })
        .await
        .unwrap();

        let row = query(&mut instance)
            .await
            .query_one("SELECT * FROM job", &[])
            .await
            .unwrap();

        // Even though we slept for 5 seconds we allow
        // a tolerance since we could shutdown the scheduler
        // before the 5th run
        let runs = row.get::<_, i32>("runs");
        dbg!(runs);
        assert!(runs >= 4);
    }
}

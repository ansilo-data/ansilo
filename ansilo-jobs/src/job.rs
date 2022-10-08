use ansilo_core::{
    config::JobConfig,
    err::{Context, Result},
};
use ansilo_logging::{info, warn};
use ansilo_pg::handler::PostgresConnectionHandler;

/// A scheduled job
#[derive(Clone)]
pub struct Job {
    /// The job config
    conf: &'static JobConfig,
    /// The postgres connection handler
    pg: PostgresConnectionHandler,
}

impl Job {
    pub fn new(conf: &'static JobConfig, pg: PostgresConnectionHandler) -> Self {
        Self { conf, pg }
    }

    /// Run the job
    pub async fn run(&self) -> Result<()> {
        info!("Starting job {}", self.conf.id);

        // Acquire a connection to postgres and execute the queries
        let res = if let Some(svc_user) = self.conf.service_user.as_ref() {
            let con = self
                .pg
                .authenticate_as_service_user(svc_user.clone())
                .await?;

            con.batch_execute(&self.conf.sql).await
        } else {
            let con = self.pg.pool().admin().await?;

            con.batch_execute(&self.conf.sql).await
        };

        res.context("Failed to execute sql")?;

        info!("Completed job '{}'", self.conf.id);

        Ok(())
    }

    pub(crate) fn to_scheduler_job(self, cron: &str) -> Result<tokio_cron_scheduler::Job> {
        let job = tokio_cron_scheduler::Job::new_cron_job_async(cron, move |_, _| {
            let job = self.clone();

            Box::pin(async move {
                if let Err(err) = job.run().await {
                    warn!("Error while executing job '{}': {:?}", job.conf.id, err)
                }
            })
        })?;

        Ok(job)
    }
}

#[cfg(test)]
mod tests {
    use ansilo_auth::Authenticator;
    use ansilo_core::config::{
        AuthConfig, ConstantServiceUserPassword, PasswordUserConfig, ServiceUserConfig,
        ServiceUserPasswordMethod, UserConfig, UserTypeOptions,
    };
    use ansilo_pg::{
        connection::PostgresConnection, handler::test::init_pg_handler, PostgresInstance,
    };

    use super::*;

    pub fn mock_auth_empty() -> Authenticator {
        let conf = Box::leak(Box::new(AuthConfig::default()));

        Authenticator::init(conf).unwrap()
    }

    pub fn mock_auth_svc_user(user: &str, pass: &str) -> Authenticator {
        let conf = Box::leak(Box::new(AuthConfig {
            providers: vec![],
            users: vec![UserConfig {
                username: user.into(),
                description: None,
                provider: None,
                r#type: UserTypeOptions::Password(PasswordUserConfig {
                    password: pass.into(),
                }),
            }],
            service_users: vec![ServiceUserConfig::new(
                user.into(),
                user.into(),
                None,
                ServiceUserPasswordMethod::Constant(ConstantServiceUserPassword {
                    password: pass.into(),
                }),
            )],
        }));

        Authenticator::init(conf).unwrap()
    }

    pub fn mock_job(
        pg: PostgresConnectionHandler,
        sql: &str,
        service_user_id: Option<String>,
    ) -> Job {
        let conf = Box::leak(Box::new(JobConfig {
            id: "test".into(),
            name: None,
            description: None,
            service_user_id,
            sql: sql.into(),
            triggers: vec![],
        }));

        Job::new(conf, pg)
    }

    async fn query(instance: &mut PostgresInstance) -> PostgresConnection {
        instance.connections().admin().await.unwrap()
    }

    #[tokio::test]
    async fn test_job_run_success() {
        ansilo_logging::init_for_tests();
        let (mut instance, pg) = init_pg_handler("job-run-success", mock_auth_empty()).await;

        query(&mut instance)
            .await
            .batch_execute("CREATE TABLE job AS SELECT 0 as runs")
            .await
            .unwrap();

        let job = mock_job(pg, "UPDATE job SET runs = runs + 1", None);

        job.run().await.unwrap();

        let row = query(&mut instance)
            .await
            .query_one("SELECT * FROM job", &[])
            .await
            .unwrap();

        assert_eq!(row.get::<_, i32>("runs"), 1);
    }

    #[tokio::test]
    async fn test_job_run_success_service_user() {
        ansilo_logging::init_for_tests();
        let (mut instance, pg) = init_pg_handler(
            "job-run-success-svc-user",
            mock_auth_svc_user("svc", "pass"),
        )
        .await;

        query(&mut instance)
            .await
            .batch_execute("
                CREATE TABLE job AS SELECT 0 as runs, '' as usr;
                GRANT SELECT, INSERT, UPDATE, DELETE ON job TO svc;
            ")
            .await
            .unwrap();

        let job = mock_job(
            pg,
            "UPDATE job SET runs = runs + 1, usr = current_user",
            Some("svc".into()),
        );

        job.run().await.unwrap();

        let row = query(&mut instance)
            .await
            .query_one("SELECT * FROM job", &[])
            .await
            .unwrap();

        assert_eq!(row.get::<_, i32>("runs"), 1);
        assert_eq!(row.get::<_, String>("usr"), "svc");
    }

    #[tokio::test]
    async fn test_job_error() {
        ansilo_logging::init_for_tests();
        let (_instance, pg) = init_pg_handler("job-run-error", mock_auth_empty()).await;

        let job = mock_job(pg, "INVALID SQL", None);

        let err = job.run().await.unwrap_err();

        dbg!(err.to_string());
        assert!(err.to_string().contains("Failed to execute sql"))
    }
}

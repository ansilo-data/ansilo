use ansilo_core::err::{Context, Result};

use crate::{conf::PostgresConf, connection::PostgresConnection, PG_ADMIN_USER, PG_APP_USER, PG_DATABASE};

/// Configures a new postgres database such that is ready
/// for use by ansilo
pub(crate) fn configure(conf: &PostgresConf, mut superuser_con: PostgresConnection) -> Result<()> {
    configure_roles(conf, &mut superuser_con)?;
    configure_extension(conf, &mut superuser_con)?;
    Ok(())
}

fn configure_roles(_conf: &PostgresConf, superuser_con: &mut PostgresConnection) -> Result<()> {
    superuser_con
        .batch_execute(
            format!(
                r#"
            CREATE USER {PG_ADMIN_USER} PASSWORD NULL;
            GRANT CREATE ON DATABASE {PG_DATABASE} TO {PG_ADMIN_USER};
            GRANT ALL ON SCHEMA public TO {PG_ADMIN_USER};

            CREATE USER {PG_APP_USER} PASSWORD NULL;
            GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {PG_APP_USER};
            "#
            )
            .as_str(),
        )
        .context("Failed to initialise roles")
}

fn configure_extension(_conf: &PostgresConf, _superuser_con: &mut PostgresConnection) -> Result<()> {
    // TODO: enable once extension is built
    // superuser_con
    //     .batch_execute(
    //         format!(
    //             r#"
    //         CREATE EXTENSION ansilo;
    //         "#
    //         )
    //         .as_str(),
    //     )
    //     .context("Failed to initialise ansilo extension")
    Ok(())
}
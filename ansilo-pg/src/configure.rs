use ansilo_core::err::{Context, Result};

use crate::{
    conf::PostgresConf, connection::PostgresConnection, PG_ADMIN_USER, PG_DATABASE,
};

/// Configures a new postgres database such that is ready for use
pub(crate) fn configure(conf: &PostgresConf, mut superuser_con: PostgresConnection) -> Result<()> {
    configure_roles(conf, &mut superuser_con)?;
    configure_extension(conf, &mut superuser_con)?;

    for sql in conf.init_db_sql.iter() {
        superuser_con
            .batch_execute(sql)
            .context("Failed run db initialisation sql")?;
    }

    Ok(())
}

fn configure_roles(conf: &PostgresConf, superuser_con: &mut PostgresConnection) -> Result<()> {
    // Create standard users
    superuser_con
        .batch_execute(
            format!(
                r#"
            -- Important: remove default CREATE on public schema
            REVOKE CREATE ON SCHEMA public FROM PUBLIC;

            -- Create admin user
            CREATE USER {PG_ADMIN_USER} PASSWORD NULL;
            GRANT CREATE ON DATABASE {PG_DATABASE} TO {PG_ADMIN_USER} WITH GRANT OPTION;
            GRANT ALL ON SCHEMA public TO {PG_ADMIN_USER} WITH GRANT OPTION;
            "#
            )
            .as_str(),
        )
        .context("Failed to initialise roles")?;

    // Configure user-provided users
    for user in conf.app_users.iter() {
        superuser_con
            .batch_execute(format!(r#"
            CREATE USER {user} PASSWORD NULL;
            "#).as_str())
            .context("Failed to initialise app user")?;
    }

    Ok(())
}

/// We cannot rely this extension being available when we build run tests
/// for this crate
#[cfg(not(test))]
fn configure_extension(_conf: &PostgresConf, superuser_con: &mut PostgresConnection) -> Result<()> {
    superuser_con
        .batch_execute(
            format!(
                r#"
                CREATE EXTENSION ansilo_pgx;
                
                GRANT USAGE ON FOREIGN DATA WRAPPER ansilo_fdw to {PG_ADMIN_USER};
            "#
            )
            .as_str(),
        )
        .context("Failed to initialise ansilo extension")
}

#[cfg(test)]
fn configure_extension(
    _conf: &PostgresConf,
    _superuser_con: &mut PostgresConnection,
) -> Result<()> {
    Ok(())
}

use ansilo_core::err::{Context, Result};
use ansilo_util_pg::query::pg_quote_identifier;

use crate::{conf::PostgresConf, connection::PostgresConnection, PG_ADMIN_USER, PG_DATABASE};

/// Configures a new postgres database such that is ready for use
pub(crate) async fn configure(
    conf: &PostgresConf,
    mut superuser_con: PostgresConnection,
) -> Result<()> {
    configure_roles(conf, &mut superuser_con).await?;
    configure_extension(conf, &mut superuser_con).await?;
    configure_internal_catalog(conf, &mut superuser_con).await?;

    for sql in conf.init_db_sql.iter() {
        superuser_con
            .batch_execute(sql)
            .await
            .context("Failed run db initialisation sql")?;
    }

    Ok(())
}

async fn configure_roles(
    conf: &PostgresConf,
    superuser_con: &mut PostgresConnection,
) -> Result<()> {
    // Create standard users
    superuser_con
        .batch_execute(
            format!(
                r#"
            -- Important: remove default CREATE on public schema
            REVOKE CREATE ON SCHEMA public FROM public;

            -- Create admin user
            CREATE USER {PG_ADMIN_USER} PASSWORD NULL;
            GRANT CREATE ON DATABASE {PG_DATABASE} TO {PG_ADMIN_USER} WITH GRANT OPTION;
            GRANT ALL ON SCHEMA public TO {PG_ADMIN_USER} WITH GRANT OPTION;
            "#
            )
            .as_str(),
        )
        .await
        .context("Failed to initialise roles")?;

    // Configure user-provided users
    for user in conf.app_users.iter() {
        let user = pg_quote_identifier(user);
        superuser_con
            .batch_execute(
                format!(
                    r#"
            CREATE USER {user} PASSWORD NULL;
            "#
                )
                .as_str(),
            )
            .await
            .context("Failed to initialise app user")?;
    }

    Ok(())
}

async fn configure_extension(
    conf: &PostgresConf,
    superuser_con: &mut PostgresConnection,
) -> Result<()> {
    superuser_con
        .batch_execute(
            format!(
                r#"
                CREATE EXTENSION ansilo_pgx;
                
                GRANT USAGE ON FOREIGN DATA WRAPPER ansilo_fdw TO {PG_ADMIN_USER};
                GRANT USAGE ON SCHEMA __ansilo_private TO {PG_ADMIN_USER};
                GRANT USAGE ON SCHEMA __ansilo_auth TO {PG_ADMIN_USER};

                -- Important: remove default EXECUTE on remote query functions
                REVOKE EXECUTE ON FUNCTION remote_query(text, text), remote_query(text, text, variadic "any") FROM public;
                REVOKE EXECUTE ON FUNCTION remote_execute(text, text), remote_execute(text, text, variadic "any") FROM public;
                GRANT EXECUTE ON FUNCTION remote_query(text, text), remote_query(text, text, variadic "any") TO {PG_ADMIN_USER};
                GRANT EXECUTE ON FUNCTION remote_execute(text, text), remote_execute(text, text, variadic "any") TO {PG_ADMIN_USER};
            "#
            )
            .as_str(),
        )
        .await
        .context("Failed to initialise ansilo extension")?;

    // Configure user-provided users
    for user in conf.app_users.iter() {
        let user = pg_quote_identifier(user);
        superuser_con
            .batch_execute(
                format!(
                    r#"
            GRANT USAGE ON SCHEMA __ansilo_auth TO {user};
            "#
                )
                .as_str(),
            )
            .await
            .context("Failed to initialise app user")?;
    }

    Ok(())
}

/// Configure the internal connector to expose ansilo-internal objects
/// Currently this supports jobs and service users but may include more
/// in future.
///
/// @see ansilo-connectors/internal/
async fn configure_internal_catalog(
    conf: &PostgresConf,
    superuser_con: &mut PostgresConnection,
) -> Result<()> {
    superuser_con
        .batch_execute(
            format!(
                r#"
                CREATE SCHEMA ansilo_catalog;

                CREATE SERVER ansilo_catalog_srv
                FOREIGN DATA WRAPPER ansilo_fdw
                OPTIONS (data_source 'internal');
                
                IMPORT FOREIGN SCHEMA "%"
                FROM SERVER ansilo_catalog_srv
                INTO ansilo_catalog;
                
                GRANT USAGE ON SCHEMA ansilo_catalog TO {PG_ADMIN_USER} WITH GRANT OPTION;
                GRANT SELECT ON ALL TABLES IN SCHEMA ansilo_catalog TO {PG_ADMIN_USER} WITH GRANT OPTION;
            "#
            )
            .as_str(),
        )
        .await
        .context("Failed to initialise ansilo internal catalog")?;

    // Allow users to read the catalog by default
    for user in conf.app_users.iter() {
        let user = pg_quote_identifier(user);
        superuser_con
            .batch_execute(
                format!(
                    r#"
                GRANT USAGE ON SCHEMA ansilo_catalog TO {user};
                GRANT SELECT ON ALL TABLES IN SCHEMA ansilo_catalog TO {user};
                "#
                )
                .as_str(),
            )
            .await
            .context("Failed to grant app user access to catalog")?;
    }

    Ok(())
}

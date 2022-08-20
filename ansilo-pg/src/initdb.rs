use std::{
    fs::{self, Permissions},
    os::unix::prelude::PermissionsExt,
    process::{Command, ExitStatus},
    time::Duration,
};

use ansilo_core::err::{Context, Result};
use ansilo_logging::info;
use nix::sys::signal::Signal;

use crate::{conf::PostgresConf, proc::ChildProc, PG_SUPER_USER};

/// initdb creates a new postgres data director
#[derive(Debug)]
pub(crate) struct PostgresInitDb {
    /// The configuration used to init the database
    pub conf: &'static PostgresConf,
    /// The child postgres process
    pub proc: ChildProc,
}

impl PostgresInitDb {
    /// Runs the initdb process
    pub fn run(conf: &'static PostgresConf) -> Result<Self> {
        info!("Running initdb...");
        let mut cmd = Command::new(conf.install_dir.join("bin/initdb"));
        cmd.arg("-D")
            .arg(conf.data_dir.as_os_str())
            .arg("--encoding=UTF8")
            .arg("-U")
            .arg(PG_SUPER_USER);

        Ok(Self {
            conf: conf,
            proc: ChildProc::new("[initdb]", Signal::SIGINT, Duration::from_secs(1), cmd)?,
        })
    }

    /// Clears out the data directory so it can be reset
    pub fn reset(conf: &PostgresConf) -> Result<()> {
        if conf.data_dir.exists() {
            fs::remove_dir_all(conf.data_dir.as_path()).context("Failed to clear directory")?;
        }
        fs::create_dir_all(conf.data_dir.as_path()).context("Failed to create directory")?;
        fs::set_permissions(conf.data_dir.as_path(), Permissions::from_mode(0o700))
            .context("Failed to set directory permissions")?;
        Ok(())
    }

    /// Waits for the process to exit and streams any stdout/stderr to the logs
    /// And overrides the default postgres configuration file with the one supplied
    pub fn complete(&mut self) -> Result<ExitStatus> {
        let status = self.proc.wait()?;

        if status.success() {
            // Copy the postgres.conf file if it exists
            if let Some(conf_path) = self.conf.postgres_conf_path.as_ref() {
                let dest_path = self.conf.data_dir.join("postgresql.conf");
                fs::copy(conf_path.as_path(), dest_path.as_path()).with_context(|| {
                    format!(
                        "Failed to copy the postgres.conf config: {}",
                        conf_path.display()
                    )
                })?;
                fs::set_permissions(dest_path.as_path(), Permissions::from_mode(0o600))
                    .context("Failed to set perms on postgres.conf file")?;
            }

            // Default postgres.conf files have "include_dir 'conf.d'"
            // lets make sure it doesn't break our install
            fs::create_dir_all(self.conf.data_dir.join("conf.d"))
                .context("Failed to create conf.d directory in postgres install dir")?;
        }

        Ok(status)
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Write, path::PathBuf};

    use super::*;

    fn test_pg_config(test_name: &'static str) -> &'static PostgresConf {
        let conf = PostgresConf {
            install_dir: PathBuf::from("/usr/lib/postgresql/14"),
            postgres_conf_path: None,
            data_dir: PathBuf::from(format!("/tmp/ansilo-tests/initdb-test/{}", test_name)),
            socket_dir_path: PathBuf::from("/tmp/"),
            fdw_socket_path: PathBuf::from("not-used"),
        };
        Box::leak(Box::new(conf))
    }

    #[test]
    fn test_initdb() {
        ansilo_logging::init_for_tests();
        let conf = test_pg_config("initdb");
        PostgresInitDb::reset(conf).unwrap();
        let mut initdb = PostgresInitDb::run(conf).unwrap();

        assert!(initdb.complete().unwrap().success());

        assert!(conf.data_dir.join("postgresql.conf").exists());
        assert!(conf.data_dir.join("PG_VERSION").exists());
        assert_eq!(
            conf.data_dir.metadata().unwrap().permissions().mode() & 0o777,
            0o700
        );
    }

    #[test]
    fn test_initdb_with_conf() {
        ansilo_logging::init_for_tests();
        let custom_conf_path = PathBuf::from("/tmp/ansilo-tests/postgres-custom.conf");
        let mut custom_conf = fs::File::create(custom_conf_path.as_path()).unwrap();
        custom_conf.write_all("custom".as_bytes()).unwrap();

        let mut conf = test_pg_config("initdb_with_conf").clone();
        conf.postgres_conf_path = Some(custom_conf_path);
        let conf = Box::leak(Box::new(conf));

        PostgresInitDb::reset(conf).unwrap();
        let mut initdb = PostgresInitDb::run(conf).unwrap();

        assert!(initdb.complete().unwrap().success());

        assert_eq!(
            String::from_utf8_lossy(
                fs::read(conf.data_dir.join("postgresql.conf"))
                    .unwrap()
                    .as_slice()
            )
            .to_string(),
            "custom".to_string()
        );
        assert_eq!(
            conf.data_dir
                .join("postgresql.conf")
                .metadata()
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
}

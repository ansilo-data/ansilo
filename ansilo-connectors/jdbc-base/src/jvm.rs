use std::{env, fs, path::PathBuf};

use ansilo_core::{
    config::ResourceConfig,
    err::{bail, Context, Error, Result},
};
use ansilo_logging::{debug, warn};
use jni::{
    objects::{JObject, JString},
    InitArgsBuilder, JNIEnv, JNIVersion, JavaVM,
};
use once_cell::sync::OnceCell;

// Global JVM instance
static JVM: OnceCell<JavaVM> = OnceCell::new();

/// Starts the JVM
fn boot_jvm(conf: Option<&ResourceConfig>) -> Result<JavaVM> {
    let jars = find_jars(None)
        .map_err(|e| warn!("Failed to find jars: {:?}", e))
        .unwrap_or(vec![])
        .iter()
        .map(|i| i.to_string_lossy().to_string())
        .collect::<Vec<_>>();

    let mut jvm_args = InitArgsBuilder::new()
        .version(JNIVersion::V8)
        .option(format!("-Djava.class.path={}", jars.join(":")).as_str());

    // Tell JVM to reduce its signal usage
    // @see https://github.com/openjdk/jdk/blob/master/src/hotspot/os/posix/signals_posix.cpp
    // @see https://www.oracle.com/java/technologies/javase/signals.html
    jvm_args = jvm_args.option("-Xrs");

    // Set the max heap size based off the allocated memory
    if let Some(conf) = conf {
        debug!("Setting JVM -Xmx{}m", conf.jvm_memory_mb());
        jvm_args = jvm_args.option(format!("-Xmx{}m", conf.jvm_memory_mb()).as_str());
    }

    // Escape hatch for configuring the jvm
    if let Some(opts) = env::var("ANSILO_JVM_ARGS").ok() {
        for arg in opts.split("||") {
            jvm_args = jvm_args.option(arg);
        }
    }

    #[cfg(debug_assertions)]
    let jvm_args = {
        let port = 49152
            + std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
                % 1000;

        ansilo_logging::info!("JVM Debug port: {}", port);

        jvm_args
            .option(&format!(
                "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address={}",
                port
            ))
            .option("-Xcheck:jni")
        // .option("-verbose:jni")
    };

    let jvm_args = jvm_args.build().context("Failed to init JVM args")?;

    let jvm = JavaVM::new(jvm_args).context("Failed to boot JVM")?;

    Ok(jvm)
}

/// Finds jars to add to the JVM class path
fn find_jars(class_path: Option<&str>) -> Result<Vec<PathBuf>> {
    let class_paths = class_path
        .map(|s| vec![PathBuf::from(s)])
        .unwrap_or_else(get_default_class_paths);

    let mut jars = vec![];

    for path in class_paths {
        let files = fs::read_dir(&path)
            .context(format!("Failed to read files in {}", path.display()))?
            .filter(|i| i.is_ok())
            .map(|i| i.unwrap().path())
            .filter(|i| i.is_file());

        for file in files {
            let ends_with_jar = file
                .file_name()
                .map(|i| i.to_string_lossy().ends_with(".jar"))
                .unwrap_or(false);

            if ends_with_jar {
                jars.push(file);
            }
        }
    }

    debug!("Found following jars: {:?}", jars);
    Ok(jars)
}

/// Gets the default class path to search for jars
fn get_default_class_paths() -> Vec<PathBuf> {
    #[cfg(not(test))]
    let default_class_path = get_current_exe_path;
    #[cfg(test)]
    let default_class_path = get_current_target_dir;

    let paths = env::var("ANSILO_CLASSPATH")
        .context("ANSILO_CLASSPATH not set")
        .or_else(|_| default_class_path().map(|i| i.to_string_lossy().to_string()))
        .map_err(|e| warn!("Failed to get current class path {:?}", e))
        .unwrap_or_else(|_| "".to_owned());

    let paths = paths.split(":");

    paths.map(|i| PathBuf::from(i)).collect()
}

/// Gets the parent dir of the currently running binary
fn get_current_exe_path() -> Result<PathBuf> {
    env::current_exe()
        .context("Failed to get current bin path")
        .and_then(|p| {
            p.parent()
                .map(|p| p.to_path_buf())
                .context("Failed to get parent path")
        })
}

/// Gets the current target directory for the build artifacts
/// During testing mode we default to target dir as this is where the jdbc jar is outputted
/// @see ansilo-connectors/build.rs
#[cfg(test)]
fn get_current_target_dir() -> Result<PathBuf> {
    env::current_exe()
        .context("Failed to get current bin path")
        .and_then(|mut p| {
            while p
                .parent()
                .context("Failed to get target dir")?
                .file_name()
                .context("Failed to get target dir")?
                .to_string_lossy()
                != "target"
            {
                p = p.parent().unwrap().to_path_buf();
            }

            Ok(p)
        })
}

/// Wrapper for booting and interaction with the JVM
pub struct Jvm {
    jvm: &'static JavaVM,
}

impl Jvm {
    /// Boots a jvm on the current thread
    pub fn boot(conf: Option<&ResourceConfig>) -> Result<Self> {
        let jvm = JVM.get_or_try_init(|| boot_jvm(conf))?;

        Ok(Self { jvm })
    }

    /// Gets the jvm for the current thread
    pub fn env(&self) -> Result<JNIEnv> {
        self.jvm
            .attach_current_thread_permanently()
            .context("Failed to attach current thread to JVM")
    }

    /// Executes the supplied function in a local frame
    ///
    /// This will all check for java exceptions after the callback returns.
    pub fn with_local_frame<F, R>(&self, local_ref_capacity: i32, cb: F) -> Result<R>
    where
        F: FnOnce(&JNIEnv) -> Result<R>,
    {
        let env = self.env()?;
        env.push_local_frame(local_ref_capacity)
            .context("Failed to push local frame")?;

        let ret = cb(&env);

        let exception = self.check_exceptions(&env);

        env.pop_local_frame(JObject::null())
            .context("Failed to pop local frame")?;

        exception.and(ret)
    }

    /// Checks for any pending Java exceptions and clears them if present
    pub fn check_exceptions(&self, env: &JNIEnv) -> Result<()> {
        if env
            .exception_check()
            .context("Failed to check for exception")?
        {
            let exception = env.exception_occurred();

            env.exception_describe()
                .context("Failed to describe exception")?;

            env.exception_clear().context("Failed to clear exception")?;

            match exception {
                Ok(ex) => {
                    let msg = self.with_local_frame(32, |env| {
                        let param = env.auto_local(
                            env.call_method(ex, "getMessage", "()Ljava/lang/String;", &[])?
                                .l()
                                .context("Failed to convert to object")?,
                        );

                        if let Err(_) | Ok(true) = env.exception_check() {
                            bail!("Exception while converting exception to string");
                        }

                        let exception_msg =
                            env.get_string(JString::from(param.as_obj())).map(|i| {
                                cesu8::from_java_cesu8(i.to_bytes())
                                    .unwrap_or_else(|_| String::from_utf8_lossy(i.to_bytes()))
                                    .to_string()
                            })?;

                        Ok(exception_msg)
                    });

                    match msg {
                        Ok(msg) => {
                            return Err(Error::msg(msg)
                                .context("Java exception occurred, review logs for full trace"))
                        }
                        Err(err) => {
                            warn!("Error while converting java exception to string: {:?}", err);
                        }
                    }
                }
                Err(err) => {
                    warn!("Error retreiving java exception to string: {:?}", err);
                }
            }

            bail!("Unknown java exception occurred, review logs for more details");
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use jni::{objects::JValue, sys::jint};

    use super::*;

    #[test]
    fn test_boot_jvm() {
        let jvm = Jvm::boot(None).unwrap();

        let x = JValue::from(-10i32);
        let val: jint = jvm
            .env()
            .unwrap()
            .call_static_method("java/lang/Math", "abs", "(I)I", &[x])
            .unwrap()
            .i()
            .unwrap();

        assert_eq!(val, 10);
    }

    #[test]
    fn find_jars_invalid_path() {
        find_jars(Some("/invalid-path/")).unwrap_err();
    }

    #[test]
    fn find_jars_no_jars() {
        let _ = fs::remove_dir_all("/tmp/ansilo-empty-jars");
        fs::create_dir_all("/tmp/ansilo-empty-jars").unwrap();
        let res = find_jars(Some("/tmp/ansilo-empty-jars")).unwrap();

        assert_eq!(res, Vec::<PathBuf>::new());
    }

    #[test]
    fn find_jars_with_jars() {
        let _ = fs::remove_dir_all("/tmp/ansilo-with-jars");
        fs::create_dir_all("/tmp/ansilo-with-jars").unwrap();
        fs::File::create("/tmp/ansilo-with-jars/test.jar").unwrap();
        fs::File::create("/tmp/ansilo-with-jars/file.txt").unwrap();

        let res = find_jars(Some("/tmp/ansilo-with-jars")).unwrap();

        assert_eq!(res, vec![PathBuf::from("/tmp/ansilo-with-jars/test.jar")]);
    }

    #[test]
    fn get_default_class_path_env() {
        env::set_var("ANSILO_CLASSPATH", "/a:/b:/c");
        let paths = get_default_class_paths();

        assert_eq!(
            paths,
            vec![
                PathBuf::from("/a"),
                PathBuf::from("/b"),
                PathBuf::from("/c")
            ]
        );
    }

    #[test]
    fn test_get_current_exe_path() {
        let res = get_current_exe_path().unwrap();

        assert_ne!(res.to_string_lossy(), "");
    }

    #[test]
    fn test_get_target_dir() {
        let target_dir = get_current_target_dir().unwrap();

        assert_eq!(target_dir.parent().unwrap().file_name().unwrap(), "target");
    }

    #[test]
    fn test_jvm_with_local_frame() {
        let jvm = Jvm::boot(None).unwrap();

        let ret = jvm.with_local_frame(10, |env| {
            env.new_object("java/lang/Object", "()V", &[]).unwrap();
            Ok(())
        });

        assert_eq!(ret.unwrap(), ())
    }
}

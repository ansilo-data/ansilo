use std::{
    env,
    fs::{self, FileType},
    path::{Path, PathBuf},
};

use ansilo_core::err::{Context, Result};
use ansilo_logging::warn;
use jni::{AttachGuard, InitArgsBuilder, JNIVersion, JavaVM};

// Global JVM instance
// According to the docs JavaVM is thread-safe and Sync so once instance
// should be fine to be shared across threads
lazy_static::lazy_static! {
    static ref JVM: Result<JavaVM> = {
        let jars = find_jars(None).map_err(|e| warn!("Failed to find jars: {:?}", e)).unwrap_or(vec![])
            .iter().map(|i| i.to_string_lossy().to_string()).collect::<Vec<_>>();

        let jvm_args = InitArgsBuilder::new()
            .version(JNIVersion::V8)
            .option(format!("-Djava.class.path={}", jars.join(";")).as_str())
            // .option("-Xcheck:jni")
            .build()
            .context("Failed to init JVM args")?;

        let jvm = JavaVM::new(jvm_args).context("Failed to boot JVM")?;

        Ok(jvm)
    };
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

    Ok(jars)
}

/// Gets the default class path to search for jars
fn get_default_class_paths() -> Vec<PathBuf> {
    let paths = env::var("ANSILO_CLASSPATH")
        .context("ANSILO_CLASSPATH not set")
        .or_else(|_| get_current_exe_path().map(|i| i.to_string_lossy().to_string()))
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

/// Wrapper for booting and interaction with the JVM
pub struct Jvm<'a> {
    pub env: AttachGuard<'a>,
}

impl<'a> Jvm<'a> {
    /// Boots a jvm on the current thread
    pub fn boot() -> Result<Self> {
        let jvm = JVM.as_ref().unwrap();

        let env = jvm
            .attach_current_thread()
            .context("Failed to attach current thread to JVM")?;

        Ok(Self { env })
    }
}

#[cfg(test)]
mod tests {
    use jni::{objects::JValue, sys::jint};

    use super::*;

    #[test]
    fn test_boot_jvm() {
        let jvm = Jvm::boot().unwrap();

        let x = JValue::from(-10i32);
        let val: jint = jvm
            .env
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
}

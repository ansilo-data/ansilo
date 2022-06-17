use std::{env, fs, path::Path};

use ansilo_core::err::{Context, Result};
use ansilo_logging::warn;
use jni::{AttachGuard, InitArgsBuilder, JNIVersion, JavaVM};

// Global JVM instance
// According to the docs JavaVM is thread-safe and Sync so once instance
// should be fine to be shared across threads
lazy_static::lazy_static! {
    static ref JVM: Result<JavaVM> = {
        let jars = find_jars(None).map_err(|e| warn!("Failed to find jars: {:?}", e)).unwrap_or(vec![]);

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
fn find_jars(class_path: Option<&str>) -> Result<Vec<String>> {
    let class_path = class_path.map(|s| s.to_owned()).unwrap_or_else(|| {
        env::var("ANSILO_CLASSPATH")
            .context("ANSILO_CLASSPATH not set")
            .or_else(|_| {
                env::current_exe()
                    .context("Failed to get current bin path")
                    .and_then(|p| p.parent().map(|p| p.to_path_buf()).context("Failed to get parent path"))
                    .map(|p| p.to_string_lossy().to_string())
            })
            .map_err(|e| warn!("Failed to get current class path {:?}", e))
            .unwrap_or_else(|_| "".to_owned())
    });

    let jars = vec![];

    for dir in class_path.split(':') {
        // let path = &Path::from(dir);

        // fs::wa
        // TODO: find paths
    }

    Ok(jars)
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
}

use ansilo_core::err::{Context, Result};
use jni::{AttachGuard, InitArgsBuilder, JNIEnv, JNIVersion, JavaVM};

/// Wrapper for booting and interaction with the JVM
pub struct Jvm {
    pub instance: JavaVM,
}

impl Jvm {
    /// Boots a jvm on the current thread
    pub fn boot() -> Result<Self> {
        let jvm_args = InitArgsBuilder::new()
            .version(JNIVersion::V8)
            // .option("-Xcheck:jni")
            .build()
            .context("Failed to init JVM args")?;

        let jvm = JavaVM::new(jvm_args).context("Failed to boot JVM")?;

        let _ = jvm
            .attach_current_thread_permanently()
            .context("Failed to attach current thread to JVM")?;

        Ok(Self { instance: jvm })
    }
}

impl Drop for Jvm {
    fn drop(&mut self) {
        // TODO: verify correct
        self.instance.detach_current_thread()
    }
}

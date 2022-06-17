use ansilo_core::err::{Context, Result};
use jni::{AttachGuard, InitArgsBuilder, JNIVersion, JavaVM};

// Global JVM instance
// According to the docs JavaVM is thread-safe and Sync so once instance
// should be fine to be shared across threads
lazy_static::lazy_static! {
    static ref JVM: Result<JavaVM> = {
        let jvm_args = InitArgsBuilder::new()
            .version(JNIVersion::V8)
            // .option("-Xcheck:jni")
            .build()
            .context("Failed to init JVM args")?;

        let jvm = JavaVM::new(jvm_args).context("Failed to boot JVM")?;

        Ok(jvm)
    };
}

/// Wrapper for booting and interaction with the JVM
pub struct Jvm<'a>
{
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

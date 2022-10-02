use ansilo_core::config::ResourceConfig;
use ansilo_e2e::current_dir;
use pretty_assertions::assert_eq;
use serial_test::serial;

// Test the default memory of 512mb is allocated appropriately to the jvm
// Since the JVM is initialised once for the entire process we can only
// test the default number.
#[test]
#[serial]
fn test_jvm_memory_setting() {
    ansilo_logging::init_for_tests();
    let (_instance, _client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let jvm = ansilo_connectors_jdbc_base::Jvm::boot(Some(&ResourceConfig::default())).unwrap();
    let jvm = jvm.env().unwrap();

    let runtime = jvm
        .call_static_method(
            "java/lang/Runtime",
            "getRuntime",
            "()Ljava/lang/Runtime;",
            &[],
        )
        .unwrap()
        .l()
        .unwrap();

    let xmx = jvm
        .call_method(runtime, "maxMemory", "()J", &[])
        .unwrap()
        .j()
        .unwrap();

    assert_eq!(
        xmx / 1024 / 1024,
        ResourceConfig::default().jvm_memory_mb() as i64
    );
}

#[test]
#[serial]
fn test_postgres_shared_buffers_setting() {
    ansilo_logging::init_for_tests();
    let (_instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let shared_buff = client
        .query_one("SHOW shared_buffers", &[])
        .unwrap()
        .get::<_, String>(0);

    assert_eq!(
        shared_buff,
        format!("{}MB", ResourceConfig::default().pg_memory_mb() / 2)
    );
}

#[test]
#[serial]
fn test_postgres_work_mem_setting() {
    ansilo_logging::init_for_tests();
    let (_instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let work_mem = client
        .query_one("SHOW work_mem", &[])
        .unwrap()
        .get::<_, String>(0);

    assert_eq!(
        work_mem,
        format!(
            "{}MB",
            ResourceConfig::default().pg_memory_mb() / 2 / ResourceConfig::default().connections()
        )
    );
}

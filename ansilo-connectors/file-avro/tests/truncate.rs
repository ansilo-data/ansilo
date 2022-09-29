use std::fs::{self, OpenOptions};
use std::sync::Arc;

use ansilo_connectors_base::interface::{Connection, QueryHandle};
use ansilo_connectors_file_avro::{AvroConfig, AvroIO};
use ansilo_connectors_file_base::{FileConnection, FileQuery, FileQueryType};
use ansilo_core::config::{EntityConfig, EntitySourceConfig};
use pretty_assertions::assert_eq;
use serial_test::serial;

mod common;

#[test]
#[serial]
fn test_avro_write_truncate() {
    ansilo_logging::init_for_tests();

    // Existing file is a copy of the output of the above test
    fs::copy(
        current_dir!().join("data/existing.avro"),
        "/tmp/ansilo-test-truncate.avro",
    )
    .unwrap();

    let mut con = FileConnection::<AvroIO>::new(Arc::new(AvroConfig::new("/tmp/".into())));

    let mut query = con
        .prepare(FileQuery::new(
            EntityConfig::minimal("unused", vec![], EntitySourceConfig::minimal("")),
            con.conf().path.join("ansilo-test-truncate.avro"),
            FileQueryType::Truncate,
        ))
        .unwrap();

    query.execute_modify().unwrap();

    // Check file truncated
    let file = OpenOptions::new()
        .read(true)
        .open("/tmp/ansilo-test-truncate.avro")
        .unwrap();
    assert_eq!(file.metadata().unwrap().len(), 0);
}

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::sync::Arc;

use ansilo_connectors_base::interface::{Connection, QueryHandle};
use ansilo_connectors_file_avro::{AvroConfig, AvroIO};
use ansilo_connectors_file_base::{FileConnection, FileQuery, FileQueryType, InsertRowsQuery};
use ansilo_core::{
    config::{EntityAttributeConfig, EntityConfig, EntitySourceConfig},
    data::{DataType, DataValue},
    sqlil,
};
use apache_avro::schema::RecordField;
use apache_avro::types::Value;
use apache_avro::Schema;
use pretty_assertions::assert_eq;
use serial_test::serial;

mod common;

#[test]
#[serial]
fn test_avro_write_new() {
    ansilo_logging::init_for_tests();
    let _ = fs::remove_file("/tmp/ansilo-test-new.avro");

    let mut con = FileConnection::<AvroIO>::new(Arc::new(AvroConfig::new("/tmp/".into())));

    let query = con
        .prepare(FileQuery::new(
            EntityConfig::minimal(
                "unused",
                vec![
                    EntityAttributeConfig::minimal("int", DataType::Int32),
                    EntityAttributeConfig::minimal("long", DataType::Int64),
                    EntityAttributeConfig::minimal(
                        "string",
                        DataType::Utf8String(Default::default()),
                    ),
                ],
                EntitySourceConfig::minimal(""),
            ),
            con.conf().path.join("ansilo-test-new.avro"),
            FileQueryType::InsertRows(InsertRowsQuery::new(
                vec!["int".into(), "long".into(), "string".into()],
                vec![
                    sqlil::Parameter::new(DataType::Int32, 1),
                    sqlil::Parameter::new(DataType::Int64, 2),
                    sqlil::Parameter::new(DataType::Utf8String(Default::default()), 3),
                ],
            )),
        ))
        .unwrap();

    let mut query = query.writer().unwrap();
    query
        .write_all(
            [
                DataValue::Int32(123),
                DataValue::Int64(123456),
                DataValue::Utf8String("str".into()),
            ]
            .into_iter(),
        )
        .unwrap();

    let affected = query.inner().unwrap().execute_modify().unwrap();

    assert_eq!(affected, Some(1));

    // Check records written
    let mut reader = apache_avro::Reader::new(
        OpenOptions::new()
            .read(true)
            .open("/tmp/ansilo-test-new.avro")
            .unwrap(),
    )
    .unwrap();

    assert_eq!(
        reader.writer_schema(),
        &Schema::Record {
            name: "record".into(),
            aliases: None,
            doc: None,
            fields: vec![
                RecordField {
                    name: "int".into(),
                    doc: None,
                    default: None,
                    schema: Schema::Int,
                    order: apache_avro::schema::RecordFieldOrder::Ignore,
                    position: 0,
                    custom_attributes: Default::default()
                },
                RecordField {
                    name: "long".into(),
                    doc: None,
                    default: None,
                    schema: Schema::Long,
                    order: apache_avro::schema::RecordFieldOrder::Ignore,
                    position: 1,
                    custom_attributes: Default::default()
                },
                RecordField {
                    name: "string".into(),
                    doc: None,
                    default: None,
                    schema: Schema::String,
                    order: apache_avro::schema::RecordFieldOrder::Ignore,
                    position: 2,
                    custom_attributes: Default::default()
                }
            ],
            lookup: BTreeMap::from_iter([
                ("int".into(), 0),
                ("long".into(), 1),
                ("string".into(), 2)
            ]),
            attributes: Default::default()
        }
    );

    assert_eq!(
        reader.next().unwrap().unwrap(),
        Value::Record(vec![
            ("int".into(), Value::Int(123)),
            ("long".into(), Value::Long(123456)),
            ("string".into(), Value::String("str".into())),
        ])
    );

    assert_eq!(reader.next().is_none(), true);
}

#[test]
#[serial]
fn test_avro_write_existing() {
    ansilo_logging::init_for_tests();

    // Existing file is a copy of the output of the above test
    fs::copy(
        current_dir!().join("data/existing.avro"),
        "/tmp/ansilo-test-existing.avro",
    )
    .unwrap();

    let mut con = FileConnection::<AvroIO>::new(Arc::new(AvroConfig::new("/tmp/".into())));

    let query = con
        .prepare(FileQuery::new(
            EntityConfig::minimal("unused", vec![], EntitySourceConfig::minimal("")),
            con.conf().path.join("ansilo-test-existing.avro"),
            FileQueryType::InsertRows(InsertRowsQuery::new(
                vec!["int".into(), "long".into(), "string".into()],
                vec![
                    sqlil::Parameter::new(DataType::Int32, 2),
                    sqlil::Parameter::new(DataType::Int64, 3),
                    sqlil::Parameter::new(DataType::Utf8String(Default::default()), 4),
                ],
            )),
        ))
        .unwrap();

    let mut query = query.writer().unwrap();
    query
        .write_all(
            [
                DataValue::Int32(-123),
                DataValue::Int64(-123456),
                DataValue::Utf8String("another".into()),
            ]
            .into_iter(),
        )
        .unwrap();

    let affected = query.inner().unwrap().execute_modify().unwrap();

    assert_eq!(affected, Some(1));

    // Check records written
    let mut reader = apache_avro::Reader::new(
        OpenOptions::new()
            .read(true)
            .open("/tmp/ansilo-test-existing.avro")
            .unwrap(),
    )
    .unwrap();

    assert_eq!(
        reader.next().unwrap().unwrap(),
        Value::Record(vec![
            ("int".into(), Value::Int(123)),
            ("long".into(), Value::Long(123456)),
            ("string".into(), Value::String("str".into())),
        ])
    );
    assert_eq!(
        reader.next().unwrap().unwrap(),
        Value::Record(vec![
            ("int".into(), Value::Int(-123)),
            ("long".into(), Value::Long(-123456)),
            ("string".into(), Value::String("another".into())),
        ])
    );
    assert_eq!(reader.next().is_none(), true);
}

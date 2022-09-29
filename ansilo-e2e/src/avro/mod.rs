use std::{
    fs::{self, OpenOptions},
    path::PathBuf,
    sync::Arc,
};

use ansilo_connectors_file_avro::{apache_avro, data::into_avro_value, AvroConfig, AvroIO};
use ansilo_connectors_file_base::FileConnection;
use ansilo_core::data::DataValue;
use ansilo_logging::info;
use glob::glob;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tempfile::TempDir;

#[derive(Clone, Serialize, Deserialize)]
struct AvroFileInit {
    schema: Value,
    records: Vec<Vec<(String, DataValue)>>,
}

// Initialises avro files in a temporary directory
pub fn init_avro_files(init_arvo_path: PathBuf) -> (FileConnection<AvroIO>, PathBuf) {
    let tmpdir = TempDir::new().unwrap().into_path();

    for path in glob(init_arvo_path.to_str().unwrap())
        .unwrap()
        .map(|i| i.unwrap())
    {
        let file_name = path.file_name().unwrap().to_string_lossy().to_string();
        let avro_path = tmpdir.join(file_name).with_extension("avro");

        info!(
            "Creating avro file from json {} at {}",
            path.display(),
            avro_path.display()
        );

        let json = fs::read_to_string(path).unwrap();
        let avro_init = serde_json::from_str::<AvroFileInit>(&json).unwrap();
        let avro_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&avro_path)
            .unwrap();

        let schema = apache_avro::Schema::parse(&avro_init.schema).unwrap();
        let mut writer = apache_avro::Writer::new(&schema, avro_file);

        // HACK: force avro header to write if no records are written
        if avro_init.records.is_empty() {
            writer.append(apache_avro::types::Value::Null).unwrap_err();
        }

        for record in avro_init.records {
            let record = apache_avro::types::Value::Record(
                record
                    .into_iter()
                    .map(|(k, v)| (k, into_avro_value(v)))
                    .collect(),
            );

            writer.append(record).unwrap();
        }

        writer.flush().unwrap();
    }

    let con = FileConnection::<AvroIO>::new(Arc::new(AvroConfig::new(tmpdir.clone())));

    (con, tmpdir)
}

use std::{fs, path::Path};

use ansilo_connectors_file_base::{FileColumn, FileStructure};
use ansilo_core::err::{bail, ensure, Result};
use apache_avro::{
    schema::{RecordField, RecordFieldOrder},
    Schema,
};

use crate::data::{from_avro_type, into_avro_type};

use ansilo_logging::warn;

pub fn parse_avro_schema(path: &Path) -> Result<FileStructure> {
    let file = fs::OpenOptions::new().read(true).open(&path)?;

    let reader = apache_avro::Reader::new(file)?;
    let schema = reader.writer_schema();
    let (doc, fields) = match schema {
        apache_avro::Schema::Record { doc, fields, .. } => (doc.clone(), fields.clone()),
        _ => bail!("Found non-record scheam in avro file: {:?}", schema),
    };

    let attrs = fields
        .into_iter()
        .filter_map(|f| match parse_avro_field(f.clone()) {
            Ok(a) => Some(a),
            Err(err) => {
                warn!("Could not parse column '{}': {:?}", f.name, err);
                None
            }
        })
        .collect::<Vec<_>>();

    ensure!(!attrs.is_empty(), "Could not parse any columns");

    Ok(FileStructure::new(attrs, doc))
}

fn parse_avro_field(f: RecordField) -> Result<FileColumn> {
    let (r#type, nullable) = from_avro_type(&f.schema)?;

    Ok(FileColumn::new(f.name, r#type, nullable, f.doc))
}

pub fn into_avro_schema(structure: &FileStructure) -> Result<Schema> {
    let fields = structure
        .cols
        .iter()
        .enumerate()
        .map(|(idx, a)| {
            let schema = into_avro_type(&a.r#type, a.nullable)?;
            Ok(RecordField {
                name: a.name.clone(),
                doc: a.desc.clone(),
                default: None,
                schema,
                order: RecordFieldOrder::Ignore,
                position: idx,
                custom_attributes: Default::default(),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Schema::Record {
        name: "record".into(),
        aliases: None,
        doc: structure.desc.clone(),
        fields: fields.clone(),
        lookup: fields
            .into_iter()
            .enumerate()
            .map(|(i, f)| (f.name, i))
            .collect(),
        attributes: Default::default(),
    })
}

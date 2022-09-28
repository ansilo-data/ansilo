use std::{fs, path::PathBuf};

use ansilo_core::{
    config::{EntityAttributeConfig, EntityConfig, EntitySourceConfig},
    err::{bail, ensure, Context, Result},
};
use apache_avro::{
    schema::{RecordField, RecordFieldOrder},
    Schema,
};

use crate::data::{from_arvo_type, into_arvo_type};

use ansilo_logging::warn;

pub fn parse_arvo_schema(path: PathBuf) -> Result<EntityConfig> {
    let file = fs::OpenOptions::new().read(true).open(&path)?;
    let name = path
        .file_name()
        .context("Failed to get file name")?
        .to_string_lossy()
        .to_string();

    let reader = apache_avro::Reader::new(file)?;
    let schema = reader.writer_schema();
    let (doc, fields) = match schema {
        apache_avro::Schema::Record { doc, fields, .. } => (doc.clone(), fields.clone()),
        _ => bail!("Found non-record scheam in arvo file: {:?}", schema),
    };

    let attrs = fields
        .into_iter()
        .filter_map(|f| match parse_arvo_field(f.clone()) {
            Ok(a) => Some(a),
            Err(err) => {
                warn!("Could not parse column '{}': {:?}", f.name, err);
                None
            }
        })
        .collect::<Vec<_>>();

    ensure!(!attrs.is_empty(), "Could not parse any columns");

    Ok(EntityConfig::new(
        name.to_string(),
        None,
        doc,
        vec![],
        attrs,
        vec![],
        EntitySourceConfig::minimal(""),
    ))
}

fn parse_arvo_field(f: RecordField) -> Result<EntityAttributeConfig> {
    let (r#type, nullable) = from_arvo_type(&f.schema)?;

    Ok(EntityAttributeConfig::new(
        f.name, f.doc, r#type, false, nullable,
    ))
}

pub fn into_arvo_schema(entity: &EntityConfig) -> Result<Schema> {
    let fields = entity
        .attributes
        .iter()
        .enumerate()
        .map(|(idx, a)| {
            let schema = into_arvo_type(&a.r#type, a.nullable)?;
            Ok(RecordField {
                name: a.id.clone(),
                doc: a.description.clone(),
                default: None,
                schema,
                order: RecordFieldOrder::Ignore,
                position: idx,
                custom_attributes: Default::default(),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Schema::Record {
        name: entity.id.as_str().into(),
        aliases: None,
        doc: entity.description.clone(),
        fields,
        lookup: Default::default(),
        attributes: Default::default(),
    })
}

use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom},
    path::Path,
    pin::Pin,
};

use ansilo_connectors_file_base::{FileIO, FileReader, FileStructure, FileWriter};
use ansilo_core::{
    data::DataValue,
    err::{bail, Context, Result},
};
use apache_avro::{schema::RecordField, types::Value as AvroValue, Schema};

use crate::{
    data::{from_avro_value, into_avro_value},
    estimate::estimate_row_count,
    schema::{into_avro_schema, parse_avro_schema},
    AvroConfig,
};

#[derive(Clone)]
pub struct AvroIO;

impl FileIO for AvroIO {
    type Conf = AvroConfig;
    type Reader = AvroReader;
    type Writer = AvroWriter;

    fn get_structure(_conf: &Self::Conf, path: &Path) -> Result<FileStructure> {
        parse_avro_schema(path)
    }

    fn estimate_row_count(_conf: &Self::Conf, path: &Path) -> Result<Option<u64>> {
        Ok(Some(estimate_row_count(path)?))
    }

    fn get_extension(_conf: &Self::Conf) -> Option<&'static str> {
        Some(".avro")
    }

    fn reader(_conf: &Self::Conf, structure: &FileStructure, path: &Path) -> Result<Self::Reader> {
        AvroReader::new(structure, path)
    }

    fn writer(_conf: &Self::Conf, structure: &FileStructure, path: &Path) -> Result<Self::Writer> {
        AvroWriter::new(structure, path)
    }

    fn truncate(_conf: &Self::Conf, _structure: &FileStructure, path: &Path) -> Result<()> {
        OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(path)
            .context("Failed to truncate file")?;

        Ok(())
    }
}

/// Avro file reader
pub struct AvroReader {
    structure: FileStructure,
    /// Workaround of lifetime restriction for apache_avro::Reader
    _schema: Pin<Box<Schema>>,
    inner: apache_avro::Reader<'static, BufReader<File>>,
    fields: Vec<RecordField>,
}

impl AvroReader {
    fn new(structure: &FileStructure, path: &Path) -> Result<Self> {
        let schema = into_avro_schema(structure).context("Failed to convert into avro schema")?;

        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .with_context(|| format!("Failed to open file {}", path.display()))?;

        // SAFETY: We transmute this reference into a 'static
        // which should be ok as we maintain the validity of this reference
        // for as long as the inner Reader is alive by owning the box in this struct
        let schema = Box::pin(schema);
        let inner = apache_avro::Reader::with_schema(
            unsafe { std::mem::transmute::<&Schema, &'static Schema>(&schema) },
            BufReader::new(file),
        )?;

        let fields = match &*schema {
            Schema::Record { fields, .. } => fields.clone(),
            _ => bail!("Unexpected schema: {:?}", schema),
        };

        Ok(Self {
            structure: structure.clone(),
            _schema: schema,
            fields,
            inner,
        })
    }
}

impl FileReader for AvroReader {
    fn read_row(&mut self) -> Result<Option<Vec<DataValue>>> {
        let row = match self.inner.next() {
            Some(Ok(r)) => r,
            Some(Err(e)) => return Err(e)?,
            None => return Ok(None),
        };

        let row = match row {
            AvroValue::Record(fields) => fields.into_iter().collect::<HashMap<_, _>>(),
            row => bail!("Unexpected avro value: {:?}", row),
        };

        let mut output = vec![];
        for (idx, field) in self.fields.iter().enumerate() {
            let val = from_avro_value(row.get(&field.name).unwrap_or(&AvroValue::Null).clone())?;

            let val = val
                .try_coerce_into(&self.structure.cols[idx].r#type)
                .with_context(|| format!("Parsing column '{}'", field.name))?;

            output.push(val);
        }

        Ok(Some(output))
    }
}

/// Avro file writer
pub struct AvroWriter {
    /// Workaround of lifetime restriction for apache_avro::Writer
    _schema: Pin<Box<Schema>>,
    inner: apache_avro::Writer<'static, BufWriter<File>>,
    fields: Vec<RecordField>,
}

impl AvroWriter {
    fn new(structure: &FileStructure, path: &Path) -> Result<Self> {
        let schema = into_avro_schema(structure).context("Failed to convert into avro schema")?;

        let mut file = fs::OpenOptions::new()
            .read(true)
            .create(true)
            .write(true)
            .open(path)
            .with_context(|| format!("Failed to open file {}", path.display()))?;
        let meta = file.metadata().context("Failed to get file metadata")?;

        // SAFETY: We transmute this reference into a 'static
        // which should be ok as we maintain the validity of this reference
        // for as long as the inner Reader is alive by owning the box in this struct
        let schema = Box::pin(schema);
        let schema_ref = unsafe { std::mem::transmute::<&Schema, &'static Schema>(&schema) };
        let inner = if meta.len() == 0 {
            // If this is an empty/new file we initialise it
            apache_avro::Writer::new(schema_ref, BufWriter::new(file))
        } else {
            // If this is a populated avro file we append new records to the end
            // First we have to read the marker at the end of the file
            let mut marker = [0u8; 16];
            file.seek(SeekFrom::End(-16))
                .context("Failed to seek to end of avro file")?;
            file.read_exact(&mut marker)
                .context("Failed to read marker from avro file")?;
            // Now we can pass the marker to the Writer, and conveniently
            // we have read to the end of the file.
            apache_avro::Writer::append_to(schema_ref, BufWriter::new(file), marker)
        };

        let fields = match &*schema {
            Schema::Record { fields, .. } => fields.clone(),
            _ => bail!("Unexpected schema: {:?}", schema),
        };

        Ok(Self {
            _schema: schema,
            fields,
            inner,
        })
    }
}

impl FileWriter for AvroWriter {
    fn write_row(&mut self, row: Vec<DataValue>) -> Result<()> {
        let row = row
            .iter()
            .enumerate()
            .map(|(idx, d)| {
                let field = &self.fields[idx];
                Ok((
                    field.name.clone(),
                    into_avro_value(d.clone())
                        .resolve(&field.schema)
                        .with_context(|| format!("Serialising column '{}'", field.name))?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        let row = apache_avro::types::Value::Record(row);

        self.inner
            .append(row)
            .context("Failed to write avro record")?;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.inner.flush()?;
        Ok(())
    }
}

use std::{
    fs::{self, DirEntry},
    io,
    marker::PhantomData,
    path::PathBuf,
};

use ansilo_core::{
    config::{EntityAttributeConfig, EntityConfig, EntitySourceConfig, NodeConfig},
    err::{Context, Result},
};

use ansilo_connectors_base::interface::{EntityDiscoverOptions, EntitySearcher};
use ansilo_logging::warn;
use wildmatch::WildMatch;

use crate::{FileConfig, FileConnection, FileIO, FileSourceConfig};

pub struct FileEntitySearcher<F: FileIO> {
    _io: PhantomData<F>,
}

impl<F: FileIO> EntitySearcher for FileEntitySearcher<F> {
    type TConnection = FileConnection<F>;
    type TEntitySourceConfig = FileSourceConfig;

    fn discover(
        con: &mut Self::TConnection,
        _nc: &NodeConfig,
        opts: EntityDiscoverOptions,
    ) -> Result<Vec<EntityConfig>> {
        let files = Self::find_files(con, &opts)?;
        let mut entities = vec![];

        for path in files {
            let file_name = path.file_name().unwrap().to_string_lossy();
            let structure = match F::get_structure(con.conf(), path.as_path()) {
                Ok(s) => s,
                Err(err) => {
                    warn!("Error while parsing file {}: {:?}", path.display(), err);
                    continue;
                }
            };

            entities.push(EntityConfig::new(
                file_name.to_string(),
                None,
                structure.desc,
                vec![],
                structure
                    .cols
                    .into_iter()
                    .map(|c| {
                        EntityAttributeConfig::new(c.name, c.desc, c.r#type, false, c.nullable)
                    })
                    .collect(),
                vec![],
                EntitySourceConfig::from(FileSourceConfig::new(file_name.to_string()))?,
            ))
        }

        Ok(entities)
    }
}

impl<F: FileIO> FileEntitySearcher<F> {
    fn find_files(
        con: &mut FileConnection<F>,
        opts: &EntityDiscoverOptions,
    ) -> Result<Vec<PathBuf>> {
        let pattern = WildMatch::new(opts.remote_schema.as_ref().unwrap_or(&"*".into()));
        let mut files = vec![];

        // Find files in the configured path
        for file in fs::read_dir(con.conf().get_path()).context("Failed to read dir")? {
            let path = match Self::check_file(con, file, &pattern) {
                Ok(Some(p)) => p,
                Ok(None) => continue,
                Err(e) => {
                    warn!("Error while checking file: {:?}", e);
                    continue;
                }
            };

            files.push(path);
        }

        Ok(files)
    }

    fn check_file(
        con: &mut FileConnection<F>,
        file: io::Result<DirEntry>,
        pattern: &WildMatch,
    ) -> Result<Option<PathBuf>> {
        let file = file?;
        let name = file.file_name().to_string_lossy().to_string();
        let ext = F::get_extension(con.conf());

        if ext.is_some() && !name.ends_with(ext.unwrap()) {
            return Ok(None);
        }

        if !pattern.matches(&name) {
            return Ok(None);
        }

        if !file.file_type()?.is_file() {
            return Ok(None);
        }

        Ok(Some(file.path()))
    }
}

#[cfg(test)]
mod tests {
    use ansilo_core::data::DataType;
    use tempfile::TempDir;

    use crate::{test::MockConfig, FileColumn, FileStructure};

    use super::*;

    use pretty_assertions::assert_eq;

    fn discover(conf: MockConfig, opts: EntityDiscoverOptions) -> Result<Vec<EntityConfig>> {
        FileEntitySearcher::discover(&mut conf.con(), &Default::default(), opts)
    }

    fn create_test_dir() -> PathBuf {
        TempDir::new().unwrap().into_path()
    }

    #[test]
    fn test_search_non_existant_dir_error() {
        let res = discover(
            MockConfig {
                path: "/this/dir/does/not/exist".into(),
                extension: None,
                mock_structure: Default::default(),
                reader: None,
                writer: None,
            },
            EntityDiscoverOptions::new("*", Default::default()),
        );

        res.unwrap_err();
    }

    #[test]
    fn test_search_empty_dir() {
        let tmpdir = create_test_dir();
        let res = discover(
            MockConfig {
                path: tmpdir,
                extension: None,
                mock_structure: Default::default(),
                reader: None,
                writer: None,
            },
            EntityDiscoverOptions::new("*", Default::default()),
        );

        assert_eq!(res.unwrap(), vec![]);
    }

    #[test]
    fn test_search_wrong_ext() {
        let tmpdir = create_test_dir();
        fs::write(tmpdir.join("file.wrong"), "").unwrap();

        let res = discover(
            MockConfig {
                path: tmpdir,
                extension: Some(".correct"),
                mock_structure: Default::default(),
                reader: None,
                writer: None,
            },
            EntityDiscoverOptions::new("*", Default::default()),
        );

        assert_eq!(res.unwrap(), vec![]);
    }

    #[test]
    fn test_search_ignores_sub_dir() {
        let tmpdir = create_test_dir();
        fs::create_dir_all(tmpdir.join("subdir")).unwrap();

        let res = discover(
            MockConfig {
                path: tmpdir,
                extension: None,
                mock_structure: Default::default(),
                reader: None,
                writer: None,
            },
            EntityDiscoverOptions::new("*", Default::default()),
        );

        assert_eq!(res.unwrap(), vec![]);
    }

    #[test]
    fn test_search_unknown_file() {
        let tmpdir = create_test_dir();
        fs::write(tmpdir.join("some-file"), "").unwrap();

        let res = discover(
            MockConfig {
                path: tmpdir,
                extension: None,
                mock_structure: Default::default(),
                reader: None,
                writer: None,
            },
            EntityDiscoverOptions::new("*", Default::default()),
        );

        assert_eq!(res.unwrap(), vec![]);
    }

    #[test]
    fn test_search_wildcard_filter() {
        let tmpdir = create_test_dir();
        fs::write(tmpdir.join("some-file"), "").unwrap();

        let res = discover(
            MockConfig {
                path: tmpdir,
                extension: None,
                mock_structure: Default::default(),
                reader: None,
                writer: None,
            },
            EntityDiscoverOptions::new("other-*", Default::default()),
        );

        assert_eq!(res.unwrap(), vec![]);
    }

    #[test]
    fn test_search_with_valid_file() {
        let tmpdir = create_test_dir();
        fs::write(tmpdir.join("some-file"), "").unwrap();

        let res = discover(
            MockConfig {
                path: tmpdir.clone(),
                extension: None,
                mock_structure: [(
                    tmpdir.join("some-file"),
                    FileStructure::new(
                        vec![FileColumn::new(
                            "col".into(),
                            DataType::Int32,
                            false,
                            Some("col desc".into()),
                        )],
                        Some("entity desc".into()),
                    ),
                )]
                .into_iter()
                .collect(),
                reader: None,
                writer: None,
            },
            EntityDiscoverOptions::new("some-*", Default::default()),
        );

        assert_eq!(
            res.unwrap(),
            vec![EntityConfig::new(
                "some-file".into(),
                None,
                Some("entity desc".into()),
                vec![],
                vec![EntityAttributeConfig::new(
                    "col".into(),
                    Some("col desc".into()),
                    DataType::Int32,
                    false,
                    false
                )],
                vec![],
                EntitySourceConfig::from(FileSourceConfig::new("some-file".into())).unwrap()
            )]
        );
    }
}

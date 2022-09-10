use ansilo_core::{
    config::EntityAttributeConfig,
    data::{DataType, StringOptions},
};
use ansilo_e2e::{current_dir, web::url};
use ansilo_web::api::v1::catalog::get::{
    Catalog, CatalogEntity, CatalogEntityAttribue, CatalogEntitySource,
};
use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
fn test() {
    ansilo_logging::init_for_tests();
    let (instance, _port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    let res = reqwest::blocking::get(url(&instance, "/api/v1/catalog"))
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<Catalog>()
        .unwrap();

    assert_eq!(
        res.entities,
        vec![CatalogEntity {
            id: "people".into(),
            name: None,
            description: Some("This is the list of people".into()),
            tags: vec![],
            attributes: vec![
                CatalogEntityAttribue {
                    attribute: EntityAttributeConfig {
                        id: "name".into(),
                        description: None,
                        r#type: DataType::Utf8String(StringOptions::default()),
                        primary_key: false,
                        nullable: true,
                    },
                },
                CatalogEntityAttribue {
                    attribute: EntityAttributeConfig {
                        id: "age".into(),
                        description: None,
                        r#type: DataType::Int32,
                        primary_key: false,
                        nullable: true,
                    },
                },
            ],
            constraints: vec![],
            source: CatalogEntitySource {
                url: None,
                source: None,
            },
        },]
    );
}

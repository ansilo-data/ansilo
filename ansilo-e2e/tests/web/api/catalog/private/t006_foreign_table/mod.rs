use ansilo_core::{
    config::EntityAttributeConfig,
    data::{DataType, StringOptions},
    web::catalog::*,
};
use ansilo_e2e::{current_dir, web::url};
use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
fn test() {
    ansilo_logging::init_for_tests();
    let (instance, _port) =
        ansilo_e2e::util::main::run_instance_without_connect(current_dir!().join("config.yml"));

    let client = reqwest::blocking::Client::new();
    let res = client
        .get(url(&instance, "/api/v1/catalog/private"))
        .basic_auth("app", Some("pass"))
        .send()
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<Catalog>()
        .unwrap();

    assert_eq!(
        res.entities,
        vec![CatalogEntity {
            id: "private.people".into(),
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
                        nullable: false,
                    },
                },
                CatalogEntityAttribue {
                    attribute: EntityAttributeConfig {
                        id: "age".into(),
                        description: None,
                        r#type: DataType::Int64,
                        primary_key: false,
                        nullable: false,
                    },
                },
            ],
            constraints: vec![],
            source: CatalogEntitySource::table("private.people".into()),
        },]
    );
}

use ansilo_core::config::EntityAttributeConfig;
use ansilo_core::{
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
            id: "internal.people".into(),
            name: None,
            description: None,
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
            source: CatalogEntitySource::table("internal.people".into()),
        },]
    );
}

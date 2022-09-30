use std::env;

use ansilo_core::{
    config::EntityAttributeConfig,
    data::{DataType, StringOptions},
    web::catalog::{Catalog, CatalogEntity, CatalogEntityAttribue, CatalogEntitySource},
};
use ansilo_e2e::{current_dir, web::url};
use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
fn test() {
    ansilo_logging::init_for_tests();
    #[allow(unused)]
    let [(peer_instance, peer_client), (main_instance, main_client)] =
        ansilo_e2e::peer::run_instances([
            ("PEER", current_dir!().join("peer-config.yml")),
            ("MAIN", current_dir!().join("main-config.yml")),
        ]);

    let client = reqwest::blocking::Client::new();
    let res = client
        .get(url(&main_instance, "/api/v1/catalog/private"))
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
            id: "sources.people".into(),
            name: None,
            description: None,
            tags: vec![],
            attributes: vec![
                CatalogEntityAttribue {
                    attribute: EntityAttributeConfig {
                        id: "name".into(),
                        description: Some("This is the name of the person".into()),
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
            source: CatalogEntitySource::parent(
                "people".into(),
                url(&peer_instance, "/"),
                CatalogEntitySource::table("people".into())
            )
        },]
    );
}

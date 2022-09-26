use ansilo_connectors_base::interface::{EntityDiscoverOptions, EntitySearcher};
use ansilo_connectors_native_mongodb::{
    MongodbCollectionOptions, MongodbEntitySearcher, MongodbEntitySourceConfig,
};
use ansilo_core::{
    config::{EntityAttributeConfig, EntityConfig, EntitySourceConfig, NodeConfig},
    data::DataType,
};

use pretty_assertions::assert_eq;

mod common;

#[test]
fn test_mongodb_import_collection() {
    let instance = common::start_mongo();
    let mut con = common::connect_to_mongo(&instance);
    let db = con.client().default_database().unwrap();

    let _ = db.collection::<()>("test_import_col").drop(None);
    db.create_collection("test_import_col", None).unwrap();

    let entities = MongodbEntitySearcher::discover(
        &mut con,
        &NodeConfig::default(),
        EntityDiscoverOptions::schema("db.test_import_col"),
    )
    .unwrap();

    assert_eq!(
        entities,
        vec![EntityConfig::minimal(
            "test_import_col",
            vec![EntityAttributeConfig::minimal("doc", DataType::JSON)],
            EntitySourceConfig::from(MongodbEntitySourceConfig::Collection(
                MongodbCollectionOptions::new("db".into(), "test_import_col".into())
            ))
            .unwrap()
        )]
    )
}

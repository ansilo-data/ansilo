use ansilo_connectors_native_postgres::PostgresEntitySourceConfig;
use ansilo_core::{
    config::{EntityConfig, NodeConfig},
    err::Result,
    web::catalog::*,
};

pub(super) fn to_catalog(
    conf: &NodeConfig,
    e: EntityConfig,
    table_name: String,
) -> Result<CatalogEntity> {
    let source = conf
        .sources
        .iter()
        .find(|i| i.id == e.source.data_source)
        .and_then(|i| {
            if i.r#type.as_str() == "peer" {
                serde_yaml::from_value::<PostgresEntitySourceConfig>(e.source.options).ok()
            } else {
                None
            }
        })
        .and_then(|i| i.as_table().cloned())
        .and_then(|i| i.source)
        .unwrap_or_else(|| CatalogEntitySource::table(table_name));

    Ok(CatalogEntity {
        id: e.id,
        name: e.name,
        description: e.description,
        tags: e.tags,
        attributes: e
            .attributes
            .into_iter()
            .map(|a| CatalogEntityAttribue { attribute: a })
            .collect(),
        constraints: e.constraints,
        source,
    })
}

use ansilo_core::{
    config::{EntityAttributeConfig, EntityConfig, EntitySourceConfig, NodeConfig},
    data::DataType,
    err::Result,
};

use ansilo_connectors_base::interface::{EntityDiscoverOptions, EntitySearcher};

use crate::InternalConnection;

pub struct InternalEntitySearcher;

impl EntitySearcher for InternalEntitySearcher {
    type TConnection = InternalConnection;
    type TEntitySourceConfig = ();

    fn discover(
        _connection: &mut InternalConnection,
        _nc: &NodeConfig,
        _opts: EntityDiscoverOptions,
    ) -> Result<Vec<EntityConfig>> {
        Ok(vec![
            EntityConfig::new(
                "jobs".into(),
                Some("Jobs".into()),
                Some("Queries configured to execute on a regular basis".into()),
                vec![],
                vec![
                    EntityAttributeConfig::nullable("id", DataType::rust_string()),
                    EntityAttributeConfig::nullable("name", DataType::rust_string()),
                    EntityAttributeConfig::nullable("description", DataType::rust_string()),
                    EntityAttributeConfig::nullable("service_user_id", DataType::rust_string()),
                    EntityAttributeConfig::nullable("sql", DataType::rust_string()),
                ],
                vec![],
                EntitySourceConfig::minimal(""),
            ),
            EntityConfig::new(
                "job_triggers".into(),
                Some("Job Triggers".into()),
                Some("Triggers define when a job is to be run".into()),
                vec![],
                vec![
                    EntityAttributeConfig::nullable("job_id", DataType::rust_string()),
                    EntityAttributeConfig::nullable("cron", DataType::rust_string()),
                ],
                vec![],
                EntitySourceConfig::minimal(""),
            ),
            EntityConfig::new(
                "service_users".into(),
                Some("Service Users".into()),
                Some("Service users define how the service authenticates itself to run scheduled jobs".into()),
                vec![],
                vec![
                    EntityAttributeConfig::nullable("id", DataType::rust_string()),
                    EntityAttributeConfig::nullable("username", DataType::rust_string()),
                    EntityAttributeConfig::nullable("description", DataType::rust_string()),
                ],
                vec![],
                EntitySourceConfig::minimal(""),
            ),
        ])
    }
}

use ansilo_core::{err::Result, sqlil as sql};

use crate::{common::entity::ConnectorEntityConfig, interface::QueryCompiler};

use super::{MemoryConnection, MemoryQuery};

pub struct MemoryQueryCompiler;

impl QueryCompiler for MemoryQueryCompiler {
    type TConnection = MemoryConnection;
    type TQuery = MemoryQuery;
    type TEntitySourceConfig = ();

    fn compile_select(
        _con: &MemoryConnection,
        conf: &ConnectorEntityConfig<()>,
        select: sql::Select,
    ) -> Result<MemoryQuery> {
        // TODO[low]: param support
        Ok(MemoryQuery::new(select, vec![]))
    }
}

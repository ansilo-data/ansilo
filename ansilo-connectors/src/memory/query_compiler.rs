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
        _conf: &ConnectorEntityConfig<()>,
        select: sql::Select,
    ) -> Result<MemoryQuery> {
        let mut params = vec![];

        select.exprs().for_each(|e| {
            e.walk(&mut |e| {
                if let sql::Expr::Parameter(p) = e {
                    params.push((p.id, p.r#type.clone()))
                }
            })
        });

        Ok(MemoryQuery::new(select, params))
    }
}

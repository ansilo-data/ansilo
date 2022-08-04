use std::{collections::HashMap, ptr};

use itertools::Itertools;
use pgx::pg_sys::{self, Node};

/// Mapping data that is accrued while converting pg expr's to sqlil
#[derive(Debug, Clone, PartialEq)]
pub struct ConversionContext {
    /// Query table relid's to alias mappings
    /// We record the aliases for any relations within the query here
    aliases: HashMap<pg_sys::Oid, String>,

    /// Query parameter mappings
    /// Postgres query params (paramkind, paramid) to SQLIL parameter id's
    params: Vec<(*mut Node, u32)>,
}

impl ConversionContext {
    pub fn new() -> Self {
        Self {
            aliases: HashMap::new(),
            params: vec![],
        }
    }

    /// Gets a unique table alias for the supplied relid
    pub(crate) fn register_alias(&mut self, relid: pg_sys::Oid) -> &str {
        if !self.aliases.contains_key(&relid) {
            self.aliases
                .insert(relid, format!("t{}", self.aliases.len() + 1));
        }

        self.aliases.get(&relid).unwrap()
    }

    /// Gets a unique table alias for the supplied relid
    pub(crate) fn get_alias(&self, relid: pg_sys::Oid) -> Option<&str> {
        self.aliases.get(&relid).map(|i| i.as_str())
    }

    /// Gets all rel id's and aliases in the query
    pub(crate) fn aliases(&self) -> &HashMap<u32, String> {
        &self.aliases
    }

    /// Registers a new param or retrieves the existing param associated to the supplied node
    pub(crate) unsafe fn register_param(&mut self, node: *mut Node) -> u32 {
        if let Some((_, param_id)) = self
            .params
            .iter()
            .find(|(other, _)| pg_sys::equal(*other as _, node as _))
        {
            *param_id
        } else {
            let param_id = (self.params.len() + 1) as u32;
            self.params.push((node, param_id));
            param_id
        }
    }

    /// Creates a new parameter (not associated to a node)
    pub(crate) fn create_param(&mut self) -> u32 {
        let param_id = (self.params.len() + 1) as u32;
        self.params.push((ptr::null_mut(), param_id));
        param_id
    }

    /// Gets all registered nodes bound to a parameter
    pub fn param_nodes(&self) -> Vec<*mut Node> {
        self.params
            .iter()
            .map(|(i, _)| *i)
            .filter(|i| !i.is_null())
            .unique()
            .collect()
    }

    /// Gets the param id's associated to the supplied node
    pub unsafe fn param_ids(&self, node: *mut Node) -> Vec<u32> {
        self.params
            .iter()
            .filter(|(n, _)| pg_sys::equal(*n as *mut _ as *const _, node as _))
            .map(|(_, id)| *id)
            .collect()
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgx::pg_schema]
mod tests {
    use pgx::*;

    use super::*;

    #[pg_test]
    fn test_sqlil_ctx_conversion_register_alias() {
        let mut ctx = ConversionContext::new();

        let res = ctx.register_alias(1);
        assert_eq!(res, "t1");

        let res = ctx.register_alias(2);
        assert_eq!(res, "t2");

        let res = ctx.register_alias(1);
        assert_eq!(res, "t1");

        let res = ctx.register_alias(5);
        assert_eq!(res, "t3");
    }

    #[pg_test]
    fn test_sqlil_ctx_conversion_register_param() {
        let mut node1 = pg_sys::Param {
            xpr: pg_sys::Expr {
                type_: pg_sys::NodeTag_T_Const,
            },
            paramkind: 1,
            paramid: 1,
            paramtype: 1,
            paramtypmod: 1,
            paramcollid: 1,
            location: 1,
        };

        let mut node1_dup = pg_sys::Param {
            xpr: pg_sys::Expr {
                type_: pg_sys::NodeTag_T_Const,
            },
            paramkind: 1,
            paramid: 1,
            paramtype: 1,
            paramtypmod: 1,
            paramcollid: 1,
            location: 1,
        };

        let mut node2 = pg_sys::Param {
            xpr: pg_sys::Expr {
                type_: pg_sys::NodeTag_T_Const,
            },
            paramkind: 1,
            paramid: 2,
            paramtype: 1,
            paramtypmod: 1,
            paramcollid: 1,
            location: 1,
        };

        let mut ctx = ConversionContext::new();

        unsafe {
            let res = ctx.register_param(&mut node1 as *mut _ as *mut pg_sys::Node);
            assert_eq!(res, 1);

            let res = ctx.register_param(&mut node1_dup as *mut _ as *mut pg_sys::Node);
            assert_eq!(res, 1);

            let res = ctx.register_param(&mut node2 as *mut _ as *mut pg_sys::Node);
            assert_eq!(res, 2);

            let res = ctx.register_param(&mut node2 as *mut _ as *mut pg_sys::Node);
            assert_eq!(res, 2);
        }
    }
}

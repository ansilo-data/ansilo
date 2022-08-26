use std::sync::{Arc, Mutex, MutexGuard};

use ansilo_connectors_base::interface::LoggedQuery;
use ansilo_core::err::{bail, Context, Result};
use ansilo_logging::info;

/// Storage for logging remote queries
#[derive(Clone)]
pub struct RemoteQueryLog {
    /// Recorded remote queries
    queries: Option<Arc<Mutex<Vec<(String, LoggedQuery)>>>>,
}

impl RemoteQueryLog {
    pub fn new() -> Self {
        Self { queries: None }
    }

    pub fn store_in_memory() -> Self {
        Self {
            queries: Some(Arc::new(Mutex::new(vec![]))),
        }
    }

    pub fn record(&self, data_source: &str, query: LoggedQuery) -> Result<()> {
        info!("Remote query sent to {}: {:?}", data_source, query);

        if self.queries.is_some() {
            self.lock()?.push((data_source.into(), query));
        }

        Ok(())
    }

    pub fn clear_memory(&self) -> Result<()> {
        self.lock()?.clear();
        Ok(())
    }

    pub fn get_from_memory(&self) -> Result<Vec<(String, LoggedQuery)>> {
        let queries = self.lock()?;
        Ok(queries.clone())
    }

    fn lock(&self) -> Result<MutexGuard<Vec<(String, LoggedQuery)>>> {
        let queries = self
            .queries
            .as_ref()
            .context("Memory storage not enabled")?;

        Ok(match queries.lock() {
            Ok(q) => q,
            Err(err) => bail!("Failed to lock query log: {:?}", err),
        })
    }
}

impl Default for RemoteQueryLog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_remote_query_log_with_memory_disabled() {
        let log = RemoteQueryLog::new();

        log.record("abc", LoggedQuery::new_query("query")).unwrap();

        log.get_from_memory().unwrap_err();
        log.clear_memory().unwrap_err();
    }

    #[test]
    fn test_remote_query_log_with_memory_enabled() {
        let log = RemoteQueryLog::store_in_memory();

        log.record("abc", LoggedQuery::new_query("query")).unwrap();

        assert_eq!(
            log.get_from_memory().unwrap(),
            vec![("abc".to_string(), LoggedQuery::new_query("query"))]
        );

        log.clear_memory().unwrap();

        assert_eq!(log.get_from_memory().unwrap(), vec![]);
    }
}

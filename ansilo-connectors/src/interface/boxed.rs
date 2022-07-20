// pub mod boxed;

// use std::any::Any;

// use ansilo_core::{
//     data::DataType,
//     config::{self, EntityVersionConfig, NodeConfig},
//     err::{Error, Result},
//     sqlil as sql,
// };

// use super::{
//     Connection, ConnectionPool, Connector, EntitySearcher, QueryHandle, QueryOperationResult,
//     ResultSet,
// };
// pub trait BoxedConnector {
//     type BoxedConnectionConfig = BoxedConnectionConfig;
//     type BoxedEntitySourceConfig = BoxedEntitySourceConfig;
//     type BoxedConnectionPool = BoxedConnectionPool;
//     type BoxedConnection: BoxedConnection;
//     type TEntitySearcher: BoxedEntitySearcher;
//     type TEntityValidator: BoxedEntityValidator;
//     type TQueryPlanner: BoxedQueryPlanner;
//     type TQueryCompiler: BoxedQueryCompiler;
//     type TQueryHandle: BoxedQueryHandle;
//     type TQuery = BoxedQuery;
//     type TResultSet: BoxedResultSet;

//     /// Gets the type of the connector, usually the name of the target platform, eg 'postgres'
//     fn r#type(&self) -> &'static str;

//     /// Parses the supplied configuration yaml into the strongly typed Options
//     fn parse_options(&self, options: config::Value) -> Result<Self::BoxedConnectionConfig>;

//     /// Gets a connection pool instance
//     fn create_connection_pool(
//         &self,
//         options: Self::BoxedConnectionConfig,
//         nc: &NodeConfig,
//     ) -> Result<Self::BoxedConnectionPool>;

//     fn create_entity_searcher(&self) -> BoxedEntitySearcher;
//     fn create_entity_validator(&self) -> BoxedEntityValidator;
//     fn create_query_planner(&self) -> BoxedQueryPlanner;
//     fn create_query_compiler(&self) -> BoxedQueryCompiler;
// }

// pub struct BoxedConnectionConfig(Box<dyn Any>);
// pub struct BoxedEntitySourceConfig(Box<dyn Any>);
// pub struct BoxedQuery(Box<dyn Any>);
// pub struct BoxedQueryHandle(Box<dyn QueryHandle<TResultSet = BoxedResultSet>>);
// pub struct BoxedResultSet(Box<dyn ResultSet>);
// pub struct BoxedConnection(
//     Box<dyn Connection<TQuery = BoxedQuery, TQueryHandle = BoxedQueryHandle>>,
// );
// pub struct BoxedConnectionPool(Box<dyn ConnectionPool<TConnection = BoxedConnection>>);

// pub struct Boxing<T>(T);
// #[derive(Clone)]
// pub struct BoxingClonable<T: Clone>(T);

// /// Delegates to the underlying boxed connection pool
// impl ConnectionPool for BoxedConnectionPool {
//     type TConnection = BoxedConnection;

//     fn acquire(&mut self) -> Result<Self::TConnection> {
//         self.0.acquire()
//     }
// }

// impl Clone for BoxedConnectionPool {
//     fn clone(&self) -> Self {
//         Self(Box::new((*self.0).clone()))
//     }
// }

// /// Blanket impl which adapts existing connection pools to the boxed types
// impl<T: ConnectionPool> ConnectionPool for BoxingClonable<T>
// where
//     T::TConnection: 'static,
// {
//     type TConnection = BoxedConnection;

//     fn acquire(&mut self) -> Result<Self::TConnection> {
//         let con = self.0.acquire()?;
//         let con = Boxing(con);

//         Ok(BoxedConnection(Box::new(con)))
//     }
// }

// /// Delegates to the underlying boxed connection
// impl Connection for BoxedConnection {
//     type TQuery = BoxedQuery;
//     type TQueryHandle = BoxedQueryHandle;

//     fn prepare(&self, query: Self::TQuery) -> Result<Self::TQueryHandle> {
//         self.0.prepare(query)
//     }
// }

// /// Blanket impl which adapts existing Connection types the boxed types
// impl<T: Connection> Connection for Boxing<T>
// where
//     T::TQuery: 'static,
//     T::TQueryHandle: 'static,
// {
//     type TQuery = BoxedQuery;
//     type TQueryHandle = BoxedQueryHandle;

//     fn prepare(&self, query: Self::TQuery) -> Result<Self::TQueryHandle> {
//         let query = query
//             .0
//             .downcast::<T::TQuery>()
//             .map_err(|_| Error::msg("Failed to downcast query"))?;
//         let rs = Boxing(self.0.prepare(*query)?);

//         Ok(BoxedQueryHandle(Box::new(rs)))
//     }
// }

// /// Delegates to the underlying boxed query handle
// impl QueryHandle for BoxedQueryHandle {
//     type TResultSet = BoxedResultSet;

//     fn get_structure(&self) -> Result<super::QueryInputStructure> {
//         self.0.get_structure()
//     }

//     fn write(&mut self, buff: &[u8]) -> Result<usize> {
//         self.0.write(buff)
//     }

//     fn execute(&mut self) -> Result<Self::TResultSet> {
//         self.0.execute()
//     }
// }

// /// Blanket impl which adapts existing QueryHandle types the boxed types
// impl<T: QueryHandle> QueryHandle for Boxing<T>
// where
//     T::TResultSet: 'static,
// {
//     type TResultSet = BoxedResultSet;

//     fn get_structure(&self) -> Result<super::QueryInputStructure> {
//         self.0.get_structure()
//     }

//     fn write(&mut self, buff: &[u8]) -> Result<usize> {
//         self.0.write(buff)
//     }

//     fn execute(&mut self) -> Result<Self::TResultSet> {
//         Ok(BoxedResultSet(Box::new(self.0.execute()?)))
//     }
// }

// /// Delegates to the underlying boxed result set
// impl ResultSet for BoxedResultSet {
//     fn get_structure(&self) -> Result<super::RowStructure> {
//         self.0.get_structure()
//     }

//     fn read(&mut self, buff: &mut [u8]) -> Result<usize> {
//         self.0.read(buff)
//     }
// }

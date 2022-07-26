
// /// A query for retrieving rows from a data source
// #[derive(Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize)]
// pub struct Select {
//     /// The list of column expressions indexed by their aliases
//     pub cols: Vec<(String, Expr)>,
//     /// The source FROM expression
//     pub from: EntityVersionIdentifier,
//     /// The join clauses
//     pub joins: Vec<Join>,
//     /// The list of where clauses
//     pub r#where: Vec<Expr>,
//     /// The list of grouping clauses
//     pub group_bys: Vec<Expr>,
//     /// This list of ordering clauses
//     pub order_bys: Vec<Ordering>,
//     /// The number of rows to return
//     pub row_limit: Option<u64>,
//     /// The number of rows to skip
//     pub row_skip: u64,
// }

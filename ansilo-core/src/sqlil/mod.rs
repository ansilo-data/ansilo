// SQLIL === SQL Intermediate Language
// A simpler subset of the SQL syntax that can be represented in an AST.
// The purpose of this is to give the connectors a common, stable and smaller surface area that they have to support.

pub mod expr;
pub mod select;
pub mod insert;
pub mod update;
pub mod delete;
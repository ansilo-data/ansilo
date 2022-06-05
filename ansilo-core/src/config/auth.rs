use std::collections::HashMap;

use serde::{Serialize, Deserialize};

use super::*;

/// Authentication options for the node
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    /// List of node authorities, used to validate incoming auth tokens
    pub authorities: Vec<AuthorityConfig>,
    /// List of roles which grant permissions to entities
    pub roles: Vec<RoleConfig>,
    // TODO
}

/// Defines an authority, used to authenticate tokens
// TODO: possibly use strongly-typed enum
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct AuthorityConfig {
    /// The authority's name
    pub name: String,
    /// The type of the authority
    pub r#type: String,
    /// Type-specific options for the authority
    pub options: HashMap<String, String>,
    /// Rules mapping from token claims to roles
    pub auth_mappings: Vec<AuthMappingConfig>,
}

/// Defines a role which grants permissions when assumed
/// TODO: consider versioning
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct RoleConfig {
    /// The ID of the role
    pub id: String,
    /// The role's name
    pub name: String,
    /// A description of the role
    pub description: String,
    /// The permissions granted by the role
    pub permissions: Vec<PermissionConfig>,
}

/// Defines a permission which grants access 
/// TODO: Maybe it would be nice to have a more SQL-like GRANT syntax?
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct PermissionConfig {
    /// The resources which this permission applies to
    /// The format is:
    ///     `ansilo:[node]:[type]:...`
    /// Currently only entities are supported:
    ///     `ansilo:self:entity:[name]:[version]:[attributes]`
    /// For example:
    ///     `ansilo:self:entity:contacts:1.*:*`
    pub resources: Vec<ARI>,
    /// The actions allowed on this resource
    pub actions: Vec<PermissionAction>,
    /// Conditions to validate on this resource
    pub conditions: Vec<PermissionCondition>,
}

/// Actions that can be performed
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum PermissionAction {
    Select,
    Insert,
    Update,
    Delete,
}

/// Conditions to be applied on the permissions
/// TODO: strong typing?
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct PermissionCondition {
    /// The type of the condition
    pub r#type: String,
    /// The options associated to the permission
    pub options: HashMap<String, String>,
}

/// Mappings rules from incoming (authenticated) tokens to roles
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct AuthMappingConfig {
    /// Checks to make on the token 
    pub checks: Vec<TokenCheckConfig>,
    /// The inherited roles if the checks pass
    pub roles: Vec<String>
}

/// Checks to apply to the token
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct TokenCheckConfig {
    /// The claim to check on the incoming token
    pub claim: String,
    /// The operator used to check the claim
    /// TODO: use enum
    pub operator: String,
    /// The value to check against
    pub value: String
}

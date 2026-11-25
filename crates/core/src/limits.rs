//! Limits shared by several modules, defined once here and re-exported where they belong.

/// Deepest nesting of a document value, in containers (maps, lists, keyed lists).
pub const MAX_DEPTH: usize = 64;
/// Longest map key, item id, name or type, in characters.
pub const MAX_NAME: usize = 200;
/// Longest change id, in characters.
pub const MAX_CHANGE_ID: usize = 64;

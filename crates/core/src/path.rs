//! Paths into a document: each segment is a map key or the id of a keyed-list item.

use std::sync::Arc;

use crate::op::{MAX_DEPTH, MAX_NAME, OpError};
use crate::value::Plain;

pub type Path = Vec<Arc<str>>;

/// A string of 1..=`MAX_NAME` characters.
pub(crate) fn name(p: &Plain, what: &str) -> Result<Arc<str>, OpError> {
    let s = p
        .as_str()
        .ok_or_else(|| OpError::Invalid(format!("{what} must be a string")))?;
    let n = s.chars().count();
    if n == 0 || n > MAX_NAME {
        return Err(OpError::Invalid(format!(
            "{what} must be 1 to {MAX_NAME} characters"
        )));
    }
    Ok(Arc::from(s))
}

/// A list of 1..=`MAX_DEPTH` names.
pub(crate) fn from_wire(p: &Plain, what: &str) -> Result<Path, OpError> {
    let Plain::Array(items) = p else {
        return Err(OpError::Invalid(format!("{what} must be a list")));
    };
    if items.is_empty() || items.len() > MAX_DEPTH {
        return Err(OpError::Invalid(format!(
            "{what} must have 1 to {MAX_DEPTH} segments"
        )));
    }
    items.iter().map(|s| name(s, "a path segment")).collect()
}

pub(crate) fn to_wire(path: &Path) -> Plain {
    Plain::Array(path.iter().map(|s| Plain::from(&**s)).collect())
}

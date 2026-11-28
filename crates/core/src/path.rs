//! Paths into a document: each segment is a map key or the id of a keyed-list item.

use std::sync::Arc;

use crate::op::{MAX_DEPTH, MAX_NAME, OpError};
use crate::value::{LIST_KEY, Plain};

pub type Path = Vec<Arc<str>>;

/// The one rule for a path segment or an item id: 1..=`MAX_NAME` characters, other than `$list`
/// (the wire form's keyed-list key: as a map key it would make the document unreadable). Used by
/// `Op::from_wire` and, for hand-built operations, by `apply`.
pub(crate) fn check_name(s: &str, what: &str) -> Result<(), OpError> {
    let n = s.chars().count();
    if n == 0 || n > MAX_NAME {
        return Err(OpError::Invalid(format!(
            "{what} must be 1 to {MAX_NAME} characters"
        )));
    }
    if s == LIST_KEY {
        return Err(OpError::Invalid(format!("{what} cannot be {LIST_KEY}")));
    }
    Ok(())
}

/// The one rule for a path: 1..=`MAX_DEPTH` segments, each a name (`check_name`).
pub(crate) fn check(path: &[Arc<str>], what: &str) -> Result<(), OpError> {
    if path.is_empty() || path.len() > MAX_DEPTH {
        return Err(OpError::Invalid(format!(
            "{what} must have 1 to {MAX_DEPTH} segments"
        )));
    }
    path.iter()
        .try_for_each(|s| check_name(s, "a path segment"))
}

/// A string that is a name (`check_name`).
pub(crate) fn name(p: &Plain, what: &str) -> Result<Arc<str>, OpError> {
    let s = p
        .as_str()
        .ok_or_else(|| OpError::Invalid(format!("{what} must be a string")))?;
    check_name(s, what)?;
    Ok(Arc::from(s))
}

/// A list of 1..=`MAX_DEPTH` names.
pub(crate) fn from_wire(p: &Plain, what: &str) -> Result<Path, OpError> {
    let Plain::Array(items) = p else {
        return Err(OpError::Invalid(format!("{what} must be a list")));
    };
    // Reading one more than `MAX_DEPTH` is enough for `check` to refuse a path that is too long.
    let path: Path = items
        .iter()
        .take(MAX_DEPTH + 1)
        .map(|s| {
            s.as_str()
                .map(Arc::from)
                .ok_or_else(|| OpError::Invalid("a path segment must be a string".to_owned()))
        })
        .collect::<Result<_, _>>()?;
    check(&path, what)?;
    Ok(path)
}

pub(crate) fn to_wire(path: &Path) -> Plain {
    Plain::Array(path.iter().map(|s| Plain::from(&**s)).collect())
}

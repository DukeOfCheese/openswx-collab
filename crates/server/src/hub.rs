//! The open documents (rooms) and their connections.
//!
//! Placeholder: the methods other code calls exist now and do nothing; Task 9 gives the hub its rooms,
//! Task 10 the online list and Task 12 the drops and closes.

use serde::Serialize;

use crate::store::Id;

/// Someone connected to a document, as the document list shows them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Online {
    pub name: String,
    pub colour: String,
}

#[derive(Debug, Clone, Default)]
pub struct Hub {}

impl Hub {
    /// Closes every connection of an account that no longer exists or may no longer sign in.
    pub async fn drop_account(&self, _user: &Id) {}

    /// Closes the connections a user has to an organisation's documents after their role changed.
    pub async fn drop_user(&self, _user: &Id, _org: &Id) {}

    /// Closes every connection to a document, telling the clients why.
    pub async fn close_document(&self, _doc: &Id, _reason: &str) {}

    /// The first people connected to a document, and how many are connected in all.
    pub async fn online(&self, _doc: &Id) -> (Vec<Online>, usize) {
        (Vec::new(), 0)
    }

    /// Closes every room and connection; the server is stopping.
    pub async fn shutdown(&self) {}
}

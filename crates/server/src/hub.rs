//! The open documents (rooms) and their connections.
//!
//! Placeholder: the methods other code calls exist now and do nothing; Task 9 gives the hub its rooms,
//! Task 10 the online list and Task 12 the drops and closes.

use std::sync::{Arc, Mutex, PoisonError};

use serde::Serialize;

use crate::store::Id;

/// Someone connected to a document, as the document list shows them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Online {
    pub name: String,
    pub colour: String,
}

#[derive(Debug, Clone, Default)]
pub struct Hub {
    /// The drop and close calls made so far (placeholder, for tests; Task 12 replaces it with the
    /// real behaviour).
    calls: Arc<Mutex<Vec<String>>>,
}

impl Hub {
    fn record(&self, call: String) {
        let mut calls = self.calls.lock().unwrap_or_else(PoisonError::into_inner);
        if calls.len() >= 1000 {
            calls.remove(0); // (a running server keeps only the latest)
        }
        calls.push(call);
    }

    /// The drop and close calls made so far, as `drop_account <user>`, `drop_user <user> <org>` and
    /// `close_document <doc>` (placeholder for tests until the hub has connections to close).
    #[doc(hidden)]
    pub fn recorded(&self) -> Vec<String> {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Closes every connection of an account that no longer exists or may no longer sign in.
    pub async fn drop_account(&self, user: &Id) {
        self.record(format!("drop_account {user}"));
    }

    /// Closes the connections a user has to an organisation's documents after their role changed.
    pub async fn drop_user(&self, user: &Id, org: &Id) {
        self.record(format!("drop_user {user} {org}"));
    }

    /// Closes every connection to a document, telling the clients why.
    pub async fn close_document(&self, doc: &Id, _reason: &str) {
        self.record(format!("close_document {doc}"));
    }

    /// The first people connected to a document, and how many are connected in all.
    pub async fn online(&self, _doc: &Id) -> (Vec<Online>, usize) {
        (Vec::new(), 0)
    }

    /// Closes every room and connection; the server is stopping.
    pub async fn shutdown(&self) {}
}

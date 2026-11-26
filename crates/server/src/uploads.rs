//! How many blob uploads run at once ([`crate::AppState::uploads`]).
//!
//! An upload is given up on when no bytes move for `blob_idle_timeout`, but a client that sends a
//! byte now and then can keep one open for as long as it likes, and an upload to S3 holds up to
//! about 40 MiB in memory while it runs. So the server takes only so many at once
//! (`OSWX_COLLAB_MAX_BLOB_UPLOADS`, 16 unless set) and only so many from one account
//! (`OSWX_COLLAB_MAX_BLOB_UPLOADS_PER_USER`, 4 unless set). One more is refused at once, before
//! any of its body is stored:
//!
//! - over the account's own limit: **429** (it is the account's own doing: it sends again when
//!   one of its uploads has finished);
//! - over the server's limit: **503** (the server is busy with other people's: send again in a
//!   moment).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::error::ApiError;
use crate::metrics::{self, UploadLimit};
use crate::store::Id;

const ACCOUNT_BUSY: &str =
    "you have too many uploads running; send this one again when one of them has finished";
const SERVER_BUSY: &str = "the server is receiving as many uploads as it takes at once; try again \
                           in a moment";

/// The limits of the blob uploads.
#[derive(Debug)]
pub struct Uploads {
    permits: Arc<Semaphore>,
    at_once: usize,
    per_account: usize,
    /// How many uploads each account has running (no entry: none).
    accounts: Mutex<HashMap<Id, usize>>,
}

impl Uploads {
    /// At most `at_once` uploads at a time on the server and `per_account` from one account (at
    /// least one of each).
    pub fn new(at_once: usize, per_account: usize) -> Self {
        let at_once = at_once.max(1);
        Self {
            permits: Arc::new(Semaphore::new(at_once)),
            at_once,
            per_account: per_account.max(1),
            accounts: Mutex::default(),
        }
    }

    /// How many uploads are running.
    pub fn running(&self) -> usize {
        self.at_once
            .saturating_sub(self.permits.available_permits())
    }

    /// A place among the running uploads for one by `account`, kept until what this answers is
    /// dropped; 429 when the account has as many running as it may, 503 when the server has.
    pub(crate) fn place(self: &Arc<Self>, account: &Id) -> Result<Place, ApiError> {
        let mut accounts = self.accounts.lock().unwrap_or_else(PoisonError::into_inner);
        let running = accounts.get(account).copied().unwrap_or(0);
        if running >= self.per_account {
            metrics::upload_refused(UploadLimit::Account);
            return Err(ApiError::Busy(ACCOUNT_BUSY));
        }
        let Ok(permit) = self.permits.clone().try_acquire_owned() else {
            metrics::upload_refused(UploadLimit::Server);
            return Err(ApiError::Unavailable(SERVER_BUSY));
        };
        accounts.insert(account.clone(), running + 1);
        Ok(Place {
            uploads: self.clone(),
            account: account.clone(),
            _permit: permit,
        })
    }
}

/// A running upload's place ([`Uploads::place`]); dropping it gives the place back.
#[derive(Debug)]
pub(crate) struct Place {
    uploads: Arc<Uploads>,
    account: Id,
    _permit: OwnedSemaphorePermit,
}

impl Drop for Place {
    fn drop(&mut self) {
        let mut accounts = self
            .uploads
            .accounts
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match accounts.get_mut(&self.account) {
            Some(running) if *running > 1 => *running -= 1,
            _ => {
                accounts.remove(&self.account);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn so_many_per_account_and_so_many_on_the_server() {
        let uploads = Arc::new(Uploads::new(3, 2));
        let (ada, bo, cy): (Id, Id, Id) = ("ada".into(), "bo".into(), "cy".into());
        let first = uploads.place(&ada).unwrap();
        let second = uploads.place(&ada).unwrap();
        // The account's own limit: 429, and the server still has room for others.
        let refused = uploads.place(&ada).unwrap_err();
        assert_eq!(
            (refused.status().as_u16(), refused.detail()),
            (429, ACCOUNT_BUSY)
        );
        let third = uploads.place(&bo).unwrap();
        assert_eq!(uploads.running(), 3);
        // The server's limit: 503, for an account that has none running.
        let refused = uploads.place(&cy).unwrap_err();
        assert_eq!(
            (refused.status().as_u16(), refused.detail()),
            (503, SERVER_BUSY)
        );
        // (An account at its own limit hears of that one, whatever the server has.)
        assert_eq!(uploads.place(&ada).unwrap_err().status().as_u16(), 429);
        drop(first);
        assert_eq!(uploads.running(), 2);
        let again = uploads.place(&ada).unwrap();
        assert_eq!(uploads.place(&ada).unwrap_err().status().as_u16(), 429);
        drop((second, again, third));
        assert_eq!(uploads.running(), 0);
        assert!(uploads.accounts.lock().unwrap().is_empty());
        // Limits of zero would refuse everything: they count as one.
        let one = Arc::new(Uploads::new(0, 0));
        let held = one.place(&ada).unwrap();
        assert_eq!(one.place(&bo).unwrap_err().status().as_u16(), 503);
        drop(held);
    }
}

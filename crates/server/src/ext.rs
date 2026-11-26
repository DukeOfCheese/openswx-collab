//! Extension points: paid features (seat limits, quotas, audit, extra routes) attach here from
//! outside, as a [`Extensions`] implementation in [`crate::AppState::ext`].
//!
//! Every method has a default that does nothing (or allows everything), which is the open-source
//! server, [`NoExtensions`].
//!
//! The server never calls a hook directly: it goes through the functions of this module, which run
//! each call on a task of its own, wait at most [`Extensions::hook_timeout`] for it, and never while
//! a database transaction is open (so a slow hook delays only its own request):
//!
//! - The refusing hooks are asked **before the request's transaction begins**:
//!   [`check_seat`] ([`Extensions::may_add_member`]) when an invite is made and when one is
//!   accepted by someone not yet a member; [`check_quota`] ([`Extensions::quota`]) before a project
//!   is created; [`check_org_delete`] ([`Extensions::before_org_delete`]) before an organisation is
//!   deleted. A refusal answers 402 with the hook's message (or a default one). A hook that times
//!   out or panics counts as a refusal and answers 409 "try again" (logged as an error with the
//!   organisation's id). Because the answer is given before the transaction, the transaction checks
//!   that what was approved still holds (e.g. the member count has not grown) and otherwise answers
//!   409 "try again" without asking again.
//! - The notifying hooks are called **after the commit**, after the session cache and the open
//!   documents have been updated and after any email: [`notify_member_added`],
//!   [`notify_member_removed`] and [`emit`] ([`Extensions::on_event`]). A timeout or panic is logged
//!   and ignored: the request has already happened. (The Python server called the member hooks inside
//!   its transaction so they could write to it; an extension here keeps its own state.)

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::Serialize;

use crate::error::ApiError;
use crate::state::AppState;
use crate::store::Id;

/// How long the server waits for a hook unless the extension says otherwise.
pub const HOOK_TIMEOUT: Duration = Duration::from_secs(5);

/// What a hook that did not answer in time (or panicked) is told to the person.
pub const HOOK_FAILED: &str =
    "the server could not check this with its extensions in time; try again";

/// Something that happened, for an audit log ([`Extensions::on_event`]). Events carry ids and the
/// acting account, never names, email addresses, tokens or content. `by` is the account that did it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum Event {
    /// An account was created (and signed in).
    Registered { user: Id },
    /// An account signed in with its password.
    SignedIn { user: Id },
    /// An account was created by signing in through a provider (`google` or `github`) for the
    /// first time. (The session itself starts when the app exchanges its code:
    /// [`Event::SignedInWithProvider`].)
    RegisteredWithProvider { user: Id, provider: String },
    /// An account signed in through a provider: the app exchanged the browser's one-time code for a
    /// session. (Which provider is not known at that point: the code's row does not say; the
    /// account's providers are in `oauth_identities`.)
    SignedInWithProvider { user: Id },
    /// A provider identity was attached to an account: by its owner (link), or on a first sign-in
    /// through it to an account whose address was confirmed.
    ProviderLinked { user: Id, provider: String },
    /// An account's owner removed a provider from it.
    ProviderUnlinked { user: Id, provider: String },
    /// A session was ended by its owner.
    SignedOut { user: Id },
    /// An account changed its password (its other sessions ended).
    PasswordChanged { user: Id },
    /// A password reset link was sent to an account's address.
    PasswordResetRequested { user: Id },
    /// An account's password was set through a reset link (every session ended).
    PasswordResetCompleted { user: Id },
    /// An organisation was created; its creator is its owner.
    OrgCreated { org: Id, by: Id },
    /// An organisation was renamed.
    OrgRenamed { org: Id, by: Id },
    /// Someone joined an organisation (by accepting an invite).
    MemberAdded {
        org: Id,
        user: Id,
        role: &'static str,
    },
    /// Someone left an organisation or was removed from it (and from its projects).
    MemberRemoved { org: Id, user: Id, by: Id },
    /// A member's organisation role changed.
    MemberRoleChanged {
        org: Id,
        user: Id,
        role: &'static str,
        by: Id,
    },
    /// An invite was made.
    InviteCreated { org: Id, invite: Id, by: Id },
    /// An invite was used.
    InviteAccepted { org: Id, invite: Id, by: Id },
    /// An invite was withdrawn before anyone used it.
    InviteRevoked { org: Id, invite: Id, by: Id },
    /// A project was created; its creator owns it.
    ProjectCreated { org: Id, project: Id, by: Id },
    /// A project was renamed.
    ProjectRenamed { org: Id, project: Id, by: Id },
    /// A project was deleted with everything in it.
    ProjectDeleted { org: Id, project: Id, by: Id },
    /// Someone was given a role in a project, or their role changed.
    ProjectMemberSet {
        org: Id,
        project: Id,
        user: Id,
        role: &'static str,
        by: Id,
    },
    /// Someone's role in a project was taken away.
    ProjectMemberRemoved {
        org: Id,
        project: Id,
        user: Id,
        by: Id,
    },
    /// A document was created.
    DocumentCreated {
        org: Id,
        project: Id,
        document: Id,
        by: Id,
    },
    /// A document was renamed.
    DocumentRenamed {
        org: Id,
        project: Id,
        document: Id,
        by: Id,
    },
    /// A document was moved into a folder (`None`: the top of the project).
    DocumentMoved {
        org: Id,
        project: Id,
        document: Id,
        folder: Option<Id>,
        by: Id,
    },
    /// A document was deleted (the row stays, marked deleted).
    DocumentDeleted {
        org: Id,
        project: Id,
        document: Id,
        by: Id,
    },
    /// A document was put back to an earlier state (as one new change, `seq`).
    DocumentRestored {
        org: Id,
        project: Id,
        document: Id,
        seq: u64,
        by: Id,
    },
    /// A named version of a document was made.
    VersionCreated {
        org: Id,
        project: Id,
        document: Id,
        version: Id,
        by: Id,
    },
    /// A named version of a document was deleted.
    VersionDeleted {
        org: Id,
        project: Id,
        document: Id,
        version: Id,
        by: Id,
    },
    /// A folder was created.
    FolderCreated {
        org: Id,
        project: Id,
        folder: Id,
        by: Id,
    },
    /// A folder was renamed.
    FolderRenamed {
        org: Id,
        project: Id,
        folder: Id,
        by: Id,
    },
    /// A folder was moved into another (`parent: None`: the top of the project).
    FolderMoved {
        org: Id,
        project: Id,
        folder: Id,
        parent: Option<Id>,
        by: Id,
    },
    /// A folder was deleted; what was in it moved to its parent.
    FolderDeleted {
        org: Id,
        project: Id,
        folder: Id,
        by: Id,
    },
}

/// What an organisation is about to have one more of ([`Extensions::quota`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Quota {
    /// A member.
    Members,
    /// A project.
    Projects,
    /// A document.
    Documents,
    /// Stored blob bytes: this many more.
    BlobBytes(u64),
}

#[async_trait]
pub trait Extensions: Send + Sync + 'static {
    /// How long the server waits for one call of any hook below ([`HOOK_TIMEOUT`] by default).
    fn hook_timeout(&self) -> Duration {
        HOOK_TIMEOUT
    }

    /// May this organisation take one more member? `current_members` is how many it has now.
    /// Asked (before the transaction) when an invite is made and when one is accepted by someone not
    /// yet a member; `Err(reason)` refuses with 402 and the reason (or "this organisation has no
    /// free seats" if it is empty).
    async fn may_add_member(&self, _org: &Id, _current_members: usize) -> Result<(), String> {
        Ok(())
    }

    /// Someone joined the organisation (after the commit).
    async fn member_added(&self, _org: &Id, _user: &Id) {}

    /// Someone left the organisation or was removed (after the commit, once their open documents
    /// are closed).
    async fn member_removed(&self, _org: &Id, _user: &Id) {}

    /// Asked before an organisation is deleted (before the transaction); `Err(reason)` refuses with
    /// 402 (it still has a paid subscription, say).
    async fn before_org_delete(&self, _org: &Id) -> Result<(), String> {
        Ok(())
    }

    /// Extra routes, merged into the app with the default 1 MiB body limit. They must not set a
    /// fallback, and must not repeat a route the server has.
    fn routes(&self, _state: AppState) -> axum::Router {
        axum::Router::new()
    }

    /// SQL migrations for the extension's own tables, applied in order after the server's own.
    /// They must never touch the server's tables.
    fn migrations(&self) -> Vec<&'static str> {
        Vec::new()
    }

    /// Something happened (an audit sink). Called after the change is committed, last.
    async fn on_event(&self, _event: &Event) {}

    /// May this organisation have one more of `what`? Asked before the transaction; `Err(reason)`
    /// refuses with 402.
    async fn quota(&self, _org: &Id, _what: Quota) -> Result<(), String> {
        Ok(())
    }

    /// Merged into `GET /api/server`; its keys win over the server's own (e.g. `edition`).
    fn server_info(&self) -> serde_json::Value {
        serde_json::json!({})
    }
}

/// No extensions: the open-source server.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoExtensions;

impl Extensions for NoExtensions {}

/// How a hook call ended.
enum Outcome<T> {
    Answered(T),
    /// Timed out or panicked (already logged).
    Failed,
}

/// Runs one hook call on a task of its own and waits at most the extension's timeout for it. A
/// call that takes longer is cancelled. A timeout or panic is logged (with the hook's name and the
/// organisation's id, if any) and reported as [`Outcome::Failed`].
async fn run<T, F, Fut>(
    ext: &Arc<dyn Extensions>,
    hook: &'static str,
    org: &str,
    call: F,
) -> Outcome<T>
where
    T: Send + 'static,
    F: FnOnce(Arc<dyn Extensions>) -> Fut,
    Fut: Future<Output = T> + Send + 'static,
{
    let timeout = ext.hook_timeout();
    let task = tokio::spawn(call(ext.clone()));
    let abort = task.abort_handle();
    match tokio::time::timeout(timeout, task).await {
        Ok(Ok(answer)) => Outcome::Answered(answer),
        Ok(Err(_)) => {
            tracing::error!(hook, org, "an extension hook panicked");
            Outcome::Failed
        }
        Err(_) => {
            abort.abort();
            tracing::error!(
                hook,
                org,
                ?timeout,
                "an extension hook did not answer in time"
            );
            Outcome::Failed
        }
    }
}

/// A refusing hook's answer as a client sees it: 402 with the reason (or `default` for an empty
/// one); 409 [`HOOK_FAILED`] when it timed out or panicked.
async fn ask<F, Fut>(
    ext: &Arc<dyn Extensions>,
    hook: &'static str,
    org: &str,
    default: &str,
    call: F,
) -> Result<(), ApiError>
where
    F: FnOnce(Arc<dyn Extensions>) -> Fut,
    Fut: Future<Output = Result<(), String>> + Send + 'static,
{
    match run(ext, hook, org, call).await {
        Outcome::Answered(Ok(())) => Ok(()),
        Outcome::Answered(Err(why)) => Err(ApiError::PaymentRequired(if why.is_empty() {
            default.to_owned()
        } else {
            why
        })),
        Outcome::Failed => Err(ApiError::Conflict(HOOK_FAILED.to_owned())),
    }
}

/// [`Extensions::may_add_member`]: may `org`, with `current_members` members, take one more?
/// Call before the transaction.
pub async fn check_seat(
    ext: &Arc<dyn Extensions>,
    org: &Id,
    current_members: u64,
) -> Result<(), ApiError> {
    let n = usize::try_from(current_members).unwrap_or(usize::MAX);
    let org_id = org.clone();
    ask(
        ext,
        "may_add_member",
        org,
        "this organisation has no free seats",
        move |e| async move { e.may_add_member(&org_id, n).await },
    )
    .await
}

/// [`Extensions::quota`]: may `org` have one more of `what`? Call before the transaction.
pub async fn check_quota(ext: &Arc<dyn Extensions>, org: &Id, what: Quota) -> Result<(), ApiError> {
    let org_id = org.clone();
    ask(
        ext,
        "quota",
        org,
        "this organisation has reached a limit of its plan",
        move |e| async move { e.quota(&org_id, what).await },
    )
    .await
}

/// [`Extensions::before_org_delete`]: may `org` be deleted? Call before the transaction.
pub async fn check_org_delete(ext: &Arc<dyn Extensions>, org: &Id) -> Result<(), ApiError> {
    let org_id = org.clone();
    ask(
        ext,
        "before_org_delete",
        org,
        "this organisation cannot be deleted now",
        move |e| async move { e.before_org_delete(&org_id).await },
    )
    .await
}

/// [`Extensions::member_added`], after the commit.
pub async fn notify_member_added(ext: &Arc<dyn Extensions>, org: &Id, user: &Id) {
    let (org_id, user) = (org.clone(), user.clone());
    run(ext, "member_added", org, move |e| async move {
        e.member_added(&org_id, &user).await;
    })
    .await;
}

/// [`Extensions::member_removed`], after the commit and after the hub has closed the member's
/// open documents.
pub async fn notify_member_removed(ext: &Arc<dyn Extensions>, org: &Id, user: &Id) {
    let (org_id, user) = (org.clone(), user.clone());
    run(ext, "member_removed", org, move |e| async move {
        e.member_removed(&org_id, &user).await;
    })
    .await;
}

/// [`Extensions::on_event`], after the commit, last.
pub async fn emit(ext: &Arc<dyn Extensions>, event: Event) {
    run(ext, "on_event", "", move |e| async move {
        e.on_event(&event).await;
    })
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn events_carry_ids_and_a_type() {
        let e = Event::ProjectMemberSet {
            org: "o".into(),
            project: "p".into(),
            user: "u".into(),
            role: "viewer",
            by: "a".into(),
        };
        assert_eq!(
            serde_json::to_value(&e).unwrap(),
            serde_json::json!({"type": "project_member_set", "org": "o", "project": "p",
                               "user": "u", "role": "viewer", "by": "a"})
        );
    }

    #[tokio::test]
    async fn no_extensions_allows_everything() {
        let ext = NoExtensions;
        let org: Id = "o".into();
        assert_eq!(ext.may_add_member(&org, 1_000_000).await, Ok(()));
        assert_eq!(ext.quota(&org, Quota::BlobBytes(u64::MAX)).await, Ok(()));
        assert_eq!(ext.before_org_delete(&org).await, Ok(()));
        assert!(ext.migrations().is_empty());
    }

    /// Never answers, panics in the notifications, and waits 50 ms at most.
    struct Broken;

    #[async_trait]
    impl Extensions for Broken {
        fn hook_timeout(&self) -> Duration {
            Duration::from_millis(50)
        }

        async fn may_add_member(&self, _org: &Id, _n: usize) -> Result<(), String> {
            std::future::pending().await
        }

        async fn quota(&self, _org: &Id, _what: Quota) -> Result<(), String> {
            panic!("a broken quota hook")
        }

        async fn member_added(&self, _org: &Id, _user: &Id) {
            panic!("a broken notification")
        }

        async fn on_event(&self, _event: &Event) {
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn a_hook_that_hangs_or_panics_refuses_or_is_ignored() {
        let ext: Arc<dyn Extensions> = Arc::new(Broken);
        let org: Id = "o".into();
        let started = std::time::Instant::now();
        let e = check_seat(&ext, &org, 1).await.unwrap_err();
        assert_eq!((e.status().as_u16(), e.detail()), (409, HOOK_FAILED));
        let e = check_quota(&ext, &org, Quota::Projects).await.unwrap_err();
        assert_eq!((e.status().as_u16(), e.detail()), (409, HOOK_FAILED));
        notify_member_added(&ext, &org, &"u".to_owned()).await;
        emit(&ext, Event::SignedIn { user: "u".into() }).await;
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    struct Refuses;

    #[async_trait]
    impl Extensions for Refuses {
        async fn may_add_member(&self, _org: &Id, _n: usize) -> Result<(), String> {
            Err(String::new())
        }

        async fn quota(&self, _org: &Id, _what: Quota) -> Result<(), String> {
            Err("no more".into())
        }
    }

    #[tokio::test]
    async fn a_refusal_is_402_with_the_reason_or_a_default() {
        let ext: Arc<dyn Extensions> = Arc::new(Refuses);
        let org: Id = "o".into();
        let e = check_seat(&ext, &org, 1).await.unwrap_err();
        assert_eq!(
            (e.status().as_u16(), e.detail()),
            (402, "this organisation has no free seats")
        );
        let e = check_quota(&ext, &org, Quota::Projects).await.unwrap_err();
        assert_eq!((e.status().as_u16(), e.detail()), (402, "no more"));
        let ext: Arc<dyn Extensions> = Arc::new(NoExtensions);
        assert!(check_org_delete(&ext, &org).await.is_ok());
    }
}

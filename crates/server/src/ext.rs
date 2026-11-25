//! Extension points: paid features (seat limits, quotas, audit, extra routes) attach here from
//! outside, as a [`Extensions`] implementation in [`crate::AppState::ext`].
//!
//! Every method has a default that does nothing (or allows everything), which is the open-source
//! server, [`NoExtensions`]. The hooks run in the request that triggers them and are awaited; keep
//! them quick.
//!
//! The member hooks are called after the change they report is committed (the Python server called
//! them inside its transaction so they could write to it; an extension here keeps its own state).
//! The refusing hooks ([`Extensions::may_add_member`], [`Extensions::quota`],
//! [`Extensions::before_org_delete`]) are asked before anything is written; their message is shown
//! to the person with status 402.

use async_trait::async_trait;
use serde::Serialize;

use crate::state::AppState;
use crate::store::Id;

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
    /// May this organisation take one more member? `current_members` is how many it has now.
    /// Asked when an invite is made and when one is accepted by someone not yet a member;
    /// `Err(reason)` refuses with 402 and the reason (or "this organisation has no free seats" if
    /// it is empty).
    async fn may_add_member(&self, _org: &Id, _current_members: usize) -> Result<(), String> {
        Ok(())
    }

    /// Someone joined the organisation (after the commit).
    async fn member_added(&self, _org: &Id, _user: &Id) {}

    /// Someone left the organisation or was removed (after the commit).
    async fn member_removed(&self, _org: &Id, _user: &Id) {}

    /// Asked before an organisation is deleted; `Err(reason)` refuses with 402 (it still has a paid
    /// subscription, say).
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

    /// Something happened (an audit sink). Called after the change is committed.
    async fn on_event(&self, _event: &Event) {}

    /// May this organisation have one more of `what`? `Err(reason)` refuses with 402.
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
}

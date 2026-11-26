//! Organisations, their members and invites.
//!
//! - `GET /api/orgs`, `POST /api/orgs`, `PATCH /api/orgs/{org}`
//! - `GET /api/orgs/{org}/members`, `PATCH` / `DELETE /api/orgs/{org}/members/{user}`
//! - `GET` / `POST /api/orgs/{org}/invites`, `DELETE /api/orgs/{org}/invites/{invite}`
//! - `POST /api/invites/accept`
//!
//! An organisation you are not a member of answers 404 `no such organisation`, exactly as one that
//! does not exist ([`require_org`]). Members see the member list; admins manage invites and roles
//! below owner; owners rename the organisation and decide who is an owner. There is always at least
//! one owner.
//!
//! An invite is a link with a token (only its hash is stored) that makes whoever uses it a member
//! with the invite's role and gives them the roles it names in the organisation's projects. It works
//! once, until it expires, and if it names an email address only for the account with that address.
//! It is used when registering ([`crate::routes::auth`]) or by a signed-in account
//! (`POST /api/invites/accept`).

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{delete, get, patch, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::access::{CurrentUser, parse_role, recheck_org, require_org};
use crate::error::{ApiError, ApiJson, ApiPath};
use crate::ext::{Event, Extensions, check_seat, emit, notify_member_added, notify_member_removed};
use crate::mail::{invite_text, send_logged};
use crate::security::{OrgRole, ProjectRole, clean_email, new_token, token_hash};
use crate::state::AppState;
use crate::store::{Id, Invite, Org, OrgMember, ProjectMember, Tx, User, new_id};

/// The longest organisation, project or account name (the columns' size).
pub(crate) const NAME_MAX: usize = 200;
/// The longest email address accepted (the column's size).
const EMAIL_MAX: usize = 320;
/// The longest invite token accepted (real ones are 43 characters).
const TOKEN_MAX: usize = 256;
/// The most projects one invite may give a role in.
const GRANTS_MAX: usize = 100;
/// The longest project id accepted in an invite (real ones are 32 characters).
const ID_MAX: usize = 64;
/// How long an invite lasts unless it says otherwise, and the range it may say: days.
const DAYS_DEFAULT: i64 = 7;
const DAYS: std::ops::RangeInclusive<i64> = 1..=90;

const NOT_A_MEMBER: &str = "not a member";
const NO_SUCH_INVITE: &str = "no such invite";
const INVITE_INVALID: &str =
    "this invite is not valid (used, expired, or for another email address)";
const INVITE_JUST_USED: &str = "this invite has just been used";
/// The members changed between the seat check and the transaction; the client tries again.
const SEATS_CHANGED: &str = "the organisation's members changed while this invite was being \
     accepted; try again";
const NEEDS_AN_OWNER: &str = "an organisation needs at least one owner";

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/orgs", get(list_orgs).post(create_org))
        .route("/api/orgs/{org_id}", patch(rename_org))
        .route("/api/orgs/{org_id}/members", get(list_members))
        .route(
            "/api/orgs/{org_id}/members/{user_id}",
            patch(set_member_role).delete(remove_member),
        )
        .route(
            "/api/orgs/{org_id}/invites",
            get(list_invites).post(create_invite),
        )
        .route(
            "/api/orgs/{org_id}/invites/{invite_id}",
            delete(revoke_invite),
        )
        .route("/api/invites/accept", post(accept))
}

/// A name as given, without the spaces around it: 422 if it is longer than [`NAME_MAX`]
/// characters, or empty once trimmed.
pub(crate) fn clean_name(name: &str) -> Result<String, ApiError> {
    if name.contains('\0') {
        return Err(ApiError::Unprocessable(crate::error::ZERO_CHARACTER.into()));
    }
    if name.chars().count() > NAME_MAX {
        return Err(ApiError::Unprocessable(format!(
            "name must be at most {NAME_MAX} characters"
        )));
    }
    match name.trim() {
        "" => Err(ApiError::Unprocessable("name must not be empty".into())),
        name => Ok(name.to_owned()),
    }
}

/// Locks the account's row until the transaction ends, and answers 401 unless it is still an
/// account that may sign in: one deleted (or disabled) while the request was on its way gains
/// nothing.
async fn still_there(tx: &mut dyn Tx, user: &str) -> Result<(), ApiError> {
    tx.lock_user(user).await?;
    match tx.user_by_id(user).await? {
        Some(account) if account.active() => Ok(()),
        _ => Err(ApiError::Unauthorized),
    }
}

/// The signed-in account's row (401 if it vanished since the session was checked).
pub(crate) async fn load_user(state: &AppState, user: &CurrentUser) -> Result<User, ApiError> {
    state
        .store
        .user_by_id(&user.id)
        .await?
        .ok_or(ApiError::Unauthorized)
}

// ------------------------------------------------------------------ organisations

#[derive(Debug, Deserialize)]
struct NameIn {
    name: String,
}

async fn list_orgs(
    State(state): State<AppState>,
    user: CurrentUser,
) -> Result<Json<Value>, ApiError> {
    let orgs: Vec<Value> = state
        .store
        .orgs_of_user(&user.id)
        .await?
        .into_iter()
        .map(|o| json!({"id": o.id, "name": o.name, "role": o.role, "members": o.members}))
        .collect();
    Ok(Json(Value::Array(orgs)))
}

/// Creates an organisation with the caller as its owner. Only the server admin may, unless
/// registration is open to everyone.
async fn create_org(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiJson(body): ApiJson<NameIn>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let name = clean_name(&body.name)?;
    let me = load_user(&state, &user).await?;
    if !(me.is_server_admin || state.settings.open_registration) {
        return Err(ApiError::Forbidden(
            "only the server admin can create organisations on this server",
        ));
    }
    let now = state.clock.now();
    let org = Org {
        id: new_id(),
        name,
        created_at: now,
    };
    let mut tx = state.store.begin().await?;
    // The account's row is locked while it gains a membership, so that its own deletion (which
    // locks it first) either sees the membership or is seen here.
    still_there(&mut *tx, &user.id).await?;
    tx.insert_org(&org).await?;
    tx.insert_org_member(&OrgMember {
        org_id: org.id.clone(),
        user_id: user.id.clone(),
        role: OrgRole::Owner.as_str().to_owned(),
        created_at: now,
    })
    .await?;
    let event = Event::OrgCreated {
        org: org.id.clone(),
        by: user.id,
    };
    let ext = state.ext.clone();
    state
        .commit_then(tx, |_| async {}, async move { emit(&ext, event).await })
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(json!({"id": org.id, "name": org.name, "role": "owner", "members": 1})),
    ))
}

async fn rename_org(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiPath(org_id): ApiPath<String>,
    ApiJson(body): ApiJson<NameIn>,
) -> Result<Json<Value>, ApiError> {
    let name = clean_name(&body.name)?;
    require_org(state.store.as_ref(), &user.id, &org_id, OrgRole::Owner).await?;
    let mut tx = state.store.begin().await?;
    recheck_org(&mut *tx, &user.id, &org_id, OrgRole::Owner).await?;
    tx.set_org_name(&org_id, &name).await?;
    let event = Event::OrgRenamed {
        org: org_id.clone(),
        by: user.id,
    };
    let ext = state.ext.clone();
    state
        .commit_then(tx, |_| async {}, async move { emit(&ext, event).await })
        .await?;
    Ok(Json(json!({"id": org_id, "name": name})))
}

// ------------------------------------------------------------------ members

async fn list_members(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiPath(org_id): ApiPath<String>,
) -> Result<Json<Value>, ApiError> {
    require_org(state.store.as_ref(), &user.id, &org_id, OrgRole::Member).await?;
    let members: Vec<Value> = state
        .store
        .org_members(&org_id)
        .await?
        .into_iter()
        .map(|m| json!({"id": m.id, "email": m.email, "name": m.name, "role": m.role}))
        .collect();
    Ok(Json(Value::Array(members)))
}

#[derive(Debug, Deserialize)]
struct RoleIn {
    role: String,
}

/// Changes a member's role. Admins manage members and admins; only an owner makes or unmakes an
/// owner; the last owner stays one. The member's open documents are closed (reconnecting picks up
/// the new role).
async fn set_member_role(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiPath((org_id, user_id)): ApiPath<(String, String)>,
    ApiJson(body): ApiJson<RoleIn>,
) -> Result<Json<Value>, ApiError> {
    let role: OrgRole = body
        .role
        .parse()
        .map_err(|_| ApiError::Unprocessable("role must be one of owner, admin, member".into()))?;
    require_org(state.store.as_ref(), &user.id, &org_id, OrgRole::Admin).await?;
    let mut tx = state.store.begin().await?;
    let mine = recheck_org(&mut *tx, &user.id, &org_id, OrgRole::Admin).await?;
    let Some(member) = tx.org_member(&org_id, &user_id).await? else {
        return Err(ApiError::NotFound(NOT_A_MEMBER));
    };
    let old: OrgRole = parse_role(&member.role)?;
    if (role == OrgRole::Owner || old == OrgRole::Owner) && mine != OrgRole::Owner {
        return Err(ApiError::Forbidden(
            "only an owner can change who is an owner",
        ));
    }
    if old == OrgRole::Owner && role != OrgRole::Owner && tx.org_owner_count(&org_id).await? <= 1 {
        return Err(ApiError::Conflict(NEEDS_AN_OWNER.into()));
    }
    tx.set_org_member_role(&org_id, &user_id, role.as_str())
        .await?;
    let event = Event::MemberRoleChanged {
        org: org_id.clone(),
        user: user_id.clone(),
        role: role.as_str(),
        by: user.id,
    };
    let (revoker, member) = (state.clone(), user_id.clone());
    let ext = state.ext.clone();
    state
        .commit_then(
            tx,
            |_| async move {
                revoker
                    .hub
                    .drop_user(revoker.store.as_ref(), &member, &org_id)
                    .await;
            },
            async move { emit(&ext, event).await },
        )
        .await?;
    Ok(Json(json!({"user_id": user_id, "role": role.as_str()})))
}

/// Removes a member (anyone may leave; removing someone else takes an admin, and an owner takes an
/// owner) and their roles in the organisation's projects. Their open documents close at once.
async fn remove_member(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiPath((org_id, user_id)): ApiPath<(String, String)>,
) -> Result<StatusCode, ApiError> {
    require_org(state.store.as_ref(), &user.id, &org_id, OrgRole::Member).await?;
    let mut tx = state.store.begin().await?;
    let mine = recheck_org(&mut *tx, &user.id, &org_id, OrgRole::Member).await?;
    let Some(member) = tx.org_member(&org_id, &user_id).await? else {
        return Err(ApiError::NotFound(NOT_A_MEMBER));
    };
    if user_id != user.id && mine < OrgRole::Admin {
        return Err(ApiError::Forbidden("needs the organisation role admin"));
    }
    if parse_role::<OrgRole>(&member.role)? == OrgRole::Owner {
        if mine != OrgRole::Owner {
            return Err(ApiError::Forbidden("only an owner can remove an owner"));
        }
        if tx.org_owner_count(&org_id).await? <= 1 {
            return Err(ApiError::Conflict(NEEDS_AN_OWNER.into()));
        }
    }
    tx.delete_project_members_in_org(&org_id, &user_id).await?;
    tx.delete_org_member(&org_id, &user_id).await?;
    let event = Event::MemberRemoved {
        org: org_id.clone(),
        user: user_id.clone(),
        by: user.id,
    };
    let (revoker, member, org) = (state.clone(), user_id.clone(), org_id.clone());
    let ext = state.ext.clone();
    // Access goes first: their open documents close before any extension hears of it.
    state
        .commit_then(
            tx,
            |_| async move {
                revoker
                    .hub
                    .drop_user(revoker.store.as_ref(), &member, &org)
                    .await;
            },
            async move {
                notify_member_removed(&ext, &org_id, &user_id).await;
                emit(&ext, event).await;
            },
        )
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

// ------------------------------------------------------------------ invites

/// A project role an invite gives, as stored in `invites.projects`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Grant {
    project_id: String,
    /// Missing in a stored grant means viewer (the Python server's reading); a new grant defaults to
    /// editor ([`GrantIn`]).
    #[serde(default)]
    role: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GrantIn {
    project_id: String,
    #[serde(default = "editor")]
    role: String,
}

fn editor() -> String {
    ProjectRole::Editor.as_str().to_owned()
}

fn member() -> String {
    OrgRole::Member.as_str().to_owned()
}

fn days_default() -> i64 {
    DAYS_DEFAULT
}

fn yes() -> bool {
    true
}

#[derive(Debug, Deserialize)]
struct InviteIn {
    #[serde(default)]
    email: Option<String>,
    #[serde(default = "member")]
    role: String,
    #[serde(default)]
    projects: Vec<GrantIn>,
    #[serde(default = "days_default")]
    days: i64,
    /// Email the invite when the server can send email and the invite names an address.
    #[serde(default = "yes")]
    send_email: bool,
}

/// An invite as its organisation's admins see it (never its token).
fn invite_out(inv: &Invite) -> Result<Value, ApiError> {
    let projects: Value = serde_json::from_str(&inv.projects)?;
    Ok(json!({
        "id": inv.id,
        "email": inv.email,
        "role": inv.role,
        "projects": projects,
        "expires_at": inv.expires_at,
        "accepted_by": inv.accepted_by,
    }))
}

/// Makes an invite. The token is in this answer and in the email, and nowhere else.
async fn create_invite(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiPath(org_id): ApiPath<String>,
    ApiJson(body): ApiJson<InviteIn>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let role: OrgRole = match body.role.parse() {
        Ok(role @ (OrgRole::Admin | OrgRole::Member)) => role,
        _ => {
            return Err(ApiError::Unprocessable(
                "role must be admin or member".into(),
            ));
        }
    };
    if !DAYS.contains(&body.days) {
        return Err(ApiError::Unprocessable("days must be from 1 to 90".into()));
    }
    if body.projects.len() > GRANTS_MAX {
        return Err(ApiError::Unprocessable(format!(
            "an invite gives roles in at most {GRANTS_MAX} projects"
        )));
    }
    let mut grants = Vec::with_capacity(body.projects.len());
    for g in &body.projects {
        if g.project_id.chars().count() > ID_MAX {
            return Err(ApiError::Unprocessable(format!(
                "project_id must be at most {ID_MAX} characters"
            )));
        }
        let role: ProjectRole = g.role.parse().map_err(|_| {
            ApiError::Unprocessable("a project role must be one of owner, editor, viewer".into())
        })?;
        grants.push(Grant {
            project_id: g.project_id.clone(),
            role: Some(role.as_str().to_owned()),
        });
    }
    if let Some(email) = &body.email
        && email.chars().count() > EMAIL_MAX
    {
        return Err(ApiError::Unprocessable(format!(
            "email must be at most {EMAIL_MAX} characters"
        )));
    }
    require_org(state.store.as_ref(), &user.id, &org_id, OrgRole::Admin).await?;
    let email = match body.email.as_deref() {
        None | Some("") => None,
        Some(email) => Some(
            clean_email(email)
                .map_err(|_| ApiError::Unprocessable("not an email address".into()))?,
        ),
    };
    for g in &grants {
        let in_org = state
            .store
            .project_by_id(&g.project_id)
            .await?
            .is_some_and(|p| p.org_id == org_id);
        if !in_org {
            return Err(ApiError::Unprocessable(format!(
                "no project {} in this organisation",
                g.project_id
            )));
        }
    }
    check_seat(
        &state.ext,
        &org_id,
        state.store.org_member_count(&org_id).await?,
    )
    .await?;
    // Read now: nothing that can fail runs between the commit and the answer but the email.
    let inviter = load_user(&state, &user).await?;
    let org = state
        .store
        .org_by_id(&org_id)
        .await?
        .ok_or(ApiError::NotFound(crate::access::NO_SUCH_ORG))?;

    let now = state.clock.now();
    let token = new_token();
    let invite = Invite {
        id: new_id(),
        token_hash: token_hash(&token),
        org_id: org_id.clone(),
        email,
        role: role.as_str().to_owned(),
        projects: serde_json::to_string(&grants)?,
        created_by: user.id.clone(),
        created_at: now,
        expires_at: now + body.days as f64 * 86400.0,
        accepted_by: None,
    };
    let mut tx = state.store.begin().await?;
    recheck_org(&mut *tx, &user.id, &org_id, OrgRole::Admin).await?;
    tx.insert_invite(&invite).await?;

    let url = format!(
        "{}/invite/{token}",
        state.settings.public_url.trim_end_matches('/')
    );
    let mail = invite.email.clone().filter(|_| body.send_email).map(|to| {
        let days = u32::try_from(body.days).unwrap_or(DAYS_DEFAULT as u32);
        (to, invite_text(&org.name, &inviter.name, &url, days))
    });
    let event = Event::InviteCreated {
        org: org_id,
        invite: invite.id.clone(),
        by: user.id,
    };
    let after = state.clone();
    let emailed = state
        .commit_then(tx, |_| async {}, async move {
            let mut emailed = false;
            if let Some((to, (subject, text))) = mail {
                emailed =
                    send_logged(after.mailer.as_ref(), &after.settings, &to, &subject, &text).await;
            }
            emit(&after.ext, event).await;
            emailed
        })
        .await?;
    let mut out = invite_out(&invite)?; // (written by this request: it parses)
    out["token"] = json!(token);
    out["url"] = json!(url);
    out["emailed"] = json!(emailed);
    Ok((StatusCode::CREATED, Json(out)))
}

/// The organisation's invites that are unused and unexpired, oldest first.
async fn list_invites(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiPath(org_id): ApiPath<String>,
) -> Result<Json<Value>, ApiError> {
    require_org(state.store.as_ref(), &user.id, &org_id, OrgRole::Admin).await?;
    let invites = state
        .store
        .open_invites(&org_id, state.clock.now())
        .await?
        .iter()
        .map(invite_out)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(Value::Array(invites)))
}

/// Deletes an invite (used or not) so that its link no longer works.
async fn revoke_invite(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiPath((org_id, invite_id)): ApiPath<(String, String)>,
) -> Result<StatusCode, ApiError> {
    require_org(state.store.as_ref(), &user.id, &org_id, OrgRole::Admin).await?;
    let mut tx = state.store.begin().await?;
    recheck_org(&mut *tx, &user.id, &org_id, OrgRole::Admin).await?;
    if !tx.delete_invite(&org_id, &invite_id).await? {
        return Err(ApiError::NotFound(NO_SUCH_INVITE));
    }
    let event = Event::InviteRevoked {
        org: org_id,
        invite: invite_id,
        by: user.id,
    };
    let ext = state.ext.clone();
    state
        .commit_then(tx, |_| async {}, async move { emit(&ext, event).await })
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Whether `invite` can be used now by the account with this (normalised) address: unused,
/// unexpired by `now`, and naming no address or exactly this one.
fn usable(invite: &Invite, email: &str, now: f64) -> bool {
    invite.accepted_by.is_none()
        && invite.expires_at >= now
        && invite.email.as_deref().is_none_or(|e| e == email)
}

/// The usable invite for this token, or `None` ([`usable`]). Read in the caller's transaction.
pub(crate) async fn find_invite(
    tx: &mut dyn Tx,
    token: Option<&str>,
    email: &str,
    now: f64,
) -> Result<Option<Invite>, ApiError> {
    let hash = token.filter(|t| !t.is_empty()).map(token_hash);
    find_invite_by_hash(tx, hash.as_deref(), email, now).await
}

/// [`find_invite`] for the hash of the token (what a browser sign-in keeps while it is away at the
/// provider).
pub(crate) async fn find_invite_by_hash(
    tx: &mut dyn Tx,
    hash: Option<&str>,
    email: &str,
    now: f64,
) -> Result<Option<Invite>, ApiError> {
    let Some(hash) = hash else {
        return Ok(None);
    };
    Ok(tx
        .invite_by_hash(hash)
        .await?
        .filter(|invite| usable(invite, email, now)))
}

/// What the seat check before the transaction approved ([`approve_invite`]), for
/// [`accept_invite`] to hold the transaction to.
#[derive(Debug)]
pub(crate) struct Approval {
    /// The invite as it was read then.
    invite: Invite,
    /// The organisation's member count the seat hook approved one more on; `None` when the account
    /// was already a member (and the hook was not asked).
    members: Option<u64>,
    /// The organisation's name (for the answer, read before the transaction).
    org_name: String,
}

/// Before the transaction: finds the usable invite for this token and, unless `user` is already a
/// member of its organisation (a new account never is), asks the seat hook whether the organisation
/// may take one more member (402, or 409 if the hook did not answer). `None` when there is no usable
/// invite.
pub(crate) async fn approve_invite(
    state: &AppState,
    token: Option<&str>,
    email: &str,
    user: Option<&str>,
    now: f64,
) -> Result<Option<Approval>, ApiError> {
    let hash = token.filter(|t| !t.is_empty()).map(token_hash);
    approve_invite_by_hash(state, hash.as_deref(), email, user, now).await
}

/// [`approve_invite`] for the hash of the token.
pub(crate) async fn approve_invite_by_hash(
    state: &AppState,
    hash: Option<&str>,
    email: &str,
    user: Option<&str>,
    now: f64,
) -> Result<Option<Approval>, ApiError> {
    let Some(hash) = hash else {
        return Ok(None);
    };
    let Some(invite) = state
        .store
        .invite_by_hash(hash)
        .await?
        .filter(|invite| usable(invite, email, now))
    else {
        return Ok(None);
    };
    let Some(org) = state.store.org_by_id(&invite.org_id).await? else {
        return Ok(None);
    };
    let already = match user {
        Some(user) => state.store.org_member(&org.id, user).await?.is_some(),
        None => false,
    };
    let members = if already {
        None
    } else {
        let n = state.store.org_member_count(&org.id).await?;
        check_seat(&state.ext, &org.id, n).await?;
        Some(n)
    };
    Ok(Some(Approval {
        invite,
        members,
        org_name: org.name,
    }))
}

/// What accepting an invite did, to report once the transaction is committed
/// ([`Accepted::announce`]).
#[derive(Debug)]
pub(crate) struct Accepted {
    org: Id,
    invite: Id,
    user: Id,
    /// The account's role in the organisation now.
    role: OrgRole,
    /// Whether they joined (they were not a member before).
    joined: bool,
}

impl Accepted {
    /// Tells the extensions: `member_added` and the events. Run after the commit
    /// ([`AppState::commit_then`]).
    pub(crate) async fn announce(self, ext: Arc<dyn Extensions>) {
        if self.joined {
            notify_member_added(&ext, &self.org, &self.user).await;
            emit(
                &ext,
                Event::MemberAdded {
                    org: self.org.clone(),
                    user: self.user.clone(),
                    role: self.role.as_str(),
                },
            )
            .await;
        }
        emit(
            &ext,
            Event::InviteAccepted {
                org: self.org,
                invite: self.invite,
                by: self.user,
            },
        )
        .await;
    }
}

/// Makes `user` a member as the invite (from [`find_invite`]) says, in the caller's transaction;
/// does not commit; asks no hook. The invite is claimed first, so of two people using it at once
/// one gets 404 `this invite has just been used`. Someone already in the organisation keeps their
/// role. Someone new joins with the invite's role, if the seat check before the transaction
/// (`approval`, from [`approve_invite`]) was for this invite and the organisation has no more
/// members than it had then; otherwise 409 "try again" (the client asks again, and the hook is
/// asked again). Each project the invite names, if it still exists in the organisation, gives its
/// role unless they already have one there.
pub(crate) async fn accept_invite(
    tx: &mut dyn Tx,
    invite: &Invite,
    approval: Option<&Approval>,
    user: &str,
    now: f64,
) -> Result<Accepted, ApiError> {
    // The account's row is locked while it gains a membership (before the organisation's: the
    // lock order of `crate::store`), so that its own deletion either sees the membership or is
    // seen here.
    still_there(tx, user).await?;
    let org = &invite.org_id;
    // The organisation's row is locked before its members are counted: of two people joining at
    // once the second counts the first, and cannot take a seat the hook did not approve.
    tx.lock_org(org).await?;
    if !tx.claim_invite(&invite.id, user).await? {
        return Err(ApiError::NotFound(INVITE_JUST_USED));
    }
    let (role, joined) = match tx.org_member(org, user).await? {
        Some(member) => (parse_role(&member.role)?, false),
        None => {
            let approved = approval
                .filter(|a| a.invite.id == invite.id)
                .and_then(|a| a.members);
            match approved {
                Some(n) if tx.org_member_count(org).await? <= n => {}
                _ => return Err(ApiError::Conflict(SEATS_CHANGED.into())),
            }
            let role: OrgRole = parse_role(&invite.role)?;
            tx.insert_org_member(&OrgMember {
                org_id: org.clone(),
                user_id: user.to_owned(),
                role: role.as_str().to_owned(),
                created_at: now,
            })
            .await?;
            (role, true)
        }
    };
    let grants: Vec<Grant> = serde_json::from_str(&invite.projects)?;
    for grant in grants {
        let Some(project) = tx.project_by_id(&grant.project_id).await? else {
            continue;
        };
        if project.org_id != *org {
            continue;
        }
        let Ok(role) = grant
            .role
            .as_deref()
            .unwrap_or("viewer")
            .parse::<ProjectRole>()
        else {
            continue;
        };
        if tx.project_member(&project.id, user).await?.is_none() {
            tx.insert_project_member(&ProjectMember {
                project_id: project.id,
                user_id: user.to_owned(),
                role: role.as_str().to_owned(),
            })
            .await?;
        }
    }
    Ok(Accepted {
        org: org.clone(),
        invite: invite.id.clone(),
        user: user.to_owned(),
        role,
        joined,
    })
}

#[derive(Debug, Deserialize)]
struct AcceptIn {
    token: String,
}

/// A signed-in account uses an invite. Every way it can be unusable answers the same 404.
async fn accept(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiJson(body): ApiJson<AcceptIn>,
) -> Result<Json<Value>, ApiError> {
    if body.token.chars().count() > TOKEN_MAX {
        return Err(ApiError::Unprocessable(format!(
            "token must be at most {TOKEN_MAX} characters"
        )));
    }
    let me = load_user(&state, &user).await?;
    let now = state.clock.now();
    let Some(approval) =
        approve_invite(&state, Some(&body.token), &me.email, Some(&me.id), now).await?
    else {
        return Err(ApiError::NotFound(INVITE_INVALID));
    };
    let mut tx = state.store.begin().await?;
    let Some(invite) = find_invite(&mut *tx, Some(&body.token), &me.email, now).await? else {
        return Err(ApiError::NotFound(INVITE_INVALID));
    };
    let accepted = accept_invite(&mut *tx, &invite, Some(&approval), &me.id, now).await?;
    let answer = json!({
        "org": {"id": invite.org_id, "name": approval.org_name},
        "role": accepted.role.as_str(),
    });
    state
        .commit_then(tx, |_| async {}, accepted.announce(state.ext.clone()))
        .await?;
    Ok(Json(answer))
}

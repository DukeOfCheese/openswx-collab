//! Who is asking: the account behind a request's bearer token.
//!
//! Handlers that need a signed-in account take [`CurrentUser`]; those that only behave differently
//! for one take [`OptionalUser`]. Both read `Authorization: Bearer <token>` (the scheme in any case,
//! the token trimmed, as the Python server's `bearer()`), and look the session up through
//! [`crate::sessions::Sessions`], which asks the database when it has no recent answer.
//!
//! What they may do: organisation roles are `owner > admin > member`, project roles
//! `owner > editor > viewer`. Organisation owners and admins own every project of the organisation;
//! everyone else has the role they were given in a project, or cannot see it at all. A server admin
//! has no role anywhere by being one. Something you have no role in does not exist for you:
//! [`require_org`] and [`require_project`] answer 404 with the same body as for a missing one, and
//! 403 only when you can see it but your role is too low.

use axum::extract::FromRequestParts;
use axum::http::header::AUTHORIZATION;
use axum::http::request::Parts;

use crate::error::ApiError;
use crate::security::{OrgRole, ProjectRole, token_hash};
use crate::state::AppState;
use crate::store::{Document, Id, Project, ProjectWithRole, Store, Tx};

/// The signed-in account. Rejects with 401 `sign in first` when the token is missing, malformed or
/// unknown, its session has expired, or its account is disabled or deleted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentUser {
    /// The account's id.
    pub id: Id,
    /// The hash of the token the request presented (which session this is).
    pub token_hash: String,
}

/// The signed-in account, or `None` when the request has no usable session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptionalUser(pub Option<CurrentUser>);

/// The token in `Authorization: Bearer <token>`, or `""` when there is none.
pub fn bearer(parts: &Parts) -> &str {
    let Some(value) = parts
        .headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    else {
        return "";
    };
    match value.get(..7) {
        Some(scheme) if scheme.eq_ignore_ascii_case("bearer ") => value[7..].trim(),
        _ => "",
    }
}

async fn lookup(parts: &Parts, state: &AppState) -> Result<Option<CurrentUser>, ApiError> {
    let token = bearer(parts);
    if token.is_empty() {
        return Ok(None);
    }
    let hash = token_hash(token);
    let found = state
        .sessions
        .get(state.store.as_ref(), &hash, state.clock.now())
        .await?;
    Ok(found.map(|id| CurrentUser {
        id,
        token_hash: hash,
    }))
}

impl FromRequestParts<AppState> for CurrentUser {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, ApiError> {
        lookup(parts, state).await?.ok_or(ApiError::Unauthorized)
    }
}

impl FromRequestParts<AppState> for OptionalUser {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, ApiError> {
        Ok(Self(lookup(parts, state).await?))
    }
}

// ------------------------------------------------------------------ roles

/// What a caller with no role in an organisation is told: the same as for one that does not exist.
pub const NO_SUCH_ORG: &str = "no such organisation";
/// What a caller with no role in a project is told: the same as for one that does not exist.
pub const NO_SUCH_PROJECT: &str = "no such project";

/// The 403 for an organisation role that is too low.
fn org_role_needed(need: OrgRole) -> &'static str {
    match need {
        OrgRole::Member => "needs the organisation role member",
        OrgRole::Admin => "needs the organisation role admin",
        OrgRole::Owner => "needs the organisation role owner",
    }
}

/// The 403 for a project role that is too low.
fn project_role_needed(need: ProjectRole) -> &'static str {
    match need {
        ProjectRole::Viewer => "needs the project role viewer",
        ProjectRole::Editor => "needs the project role editor",
        ProjectRole::Owner => "needs the project role owner",
    }
}

/// A role read from the database; one this server does not know is an error, not a role.
pub(crate) fn parse_role<R: std::str::FromStr>(role: &str) -> anyhow::Result<R> {
    role.parse()
        .map_err(|_| anyhow::anyhow!("unknown role {role:?} in the database"))
}

/// The account's role in an organisation, or `None` if it is not a member. A server admin has no
/// role in an organisation it is not a member of.
pub async fn org_role(
    store: &dyn Store,
    user: &str,
    org: &str,
) -> Result<Option<OrgRole>, ApiError> {
    match store.org_member(org, user).await? {
        Some(m) => Ok(Some(parse_role(&m.role)?)),
        None => Ok(None),
    }
}

/// The account's role in an organisation, which must be `need` or higher: 404 `no such
/// organisation` when it has none (an organisation you are not in does not exist for you), 403
/// `needs the organisation role <need>` when it is lower.
pub async fn require_org(
    store: &dyn Store,
    user: &str,
    org: &str,
    need: OrgRole,
) -> Result<OrgRole, ApiError> {
    decide_org(org_role(store, user, org).await?, need)
}

/// 404 for no role, 403 for one below `need`.
fn decide_org(role: Option<OrgRole>, need: OrgRole) -> Result<OrgRole, ApiError> {
    let role = role.ok_or(ApiError::NotFound(NO_SUCH_ORG))?;
    if role < need {
        return Err(ApiError::Forbidden(org_role_needed(need)));
    }
    Ok(role)
}

/// [`require_org`] read again inside a transaction, so that a change made with it is refused (with
/// the same answer) if the caller's role was lowered or taken away since the first check.
pub async fn recheck_org(
    tx: &mut dyn Tx,
    user: &str,
    org: &str,
    need: OrgRole,
) -> Result<OrgRole, ApiError> {
    let role = match tx.org_member(org, user).await? {
        Some(m) => Some(parse_role(&m.role)?),
        None => None,
    };
    decide_org(role, need)
}

/// The account's role in `project`: owner for an organisation owner or admin; otherwise the role
/// it was given in the project, or `None`. Someone not in the project's organisation has none,
/// whatever their role row says.
pub async fn role_in_project(
    store: &dyn Store,
    user: &str,
    project: &Project,
) -> Result<Option<ProjectRole>, ApiError> {
    let Some(org) = org_role(store, user, &project.org_id).await? else {
        return Ok(None);
    };
    if org >= OrgRole::Admin {
        return Ok(Some(ProjectRole::Owner));
    }
    match store.project_member(&project.id, user).await? {
        Some(m) => Ok(Some(parse_role(&m.role)?)),
        None => Ok(None),
    }
}

/// The account's role in the project with this id, or `None` if it has none or there is no such
/// project.
pub async fn project_role(
    store: &dyn Store,
    user: &str,
    project: &str,
) -> Result<Option<ProjectRole>, ApiError> {
    match store.project_by_id(project).await? {
        Some(p) => role_in_project(store, user, &p).await,
        None => Ok(None),
    }
}

/// The project and the account's role in it, which must be `need` or higher: 404 `no such project`
/// when there is no such project or the account has no role in it (the same answer for both), 403
/// `needs the project role <need>` when it is lower.
pub async fn require_project(
    store: &dyn Store,
    user: &str,
    project: &str,
    need: ProjectRole,
) -> Result<(Project, ProjectRole), ApiError> {
    let found = store.project_by_id(project).await?;
    let role = match &found {
        Some(p) => role_in_project(store, user, p).await?,
        None => None,
    };
    decide_project(found, role, need)
}

/// 404 for no project or no role, 403 for one below `need`.
fn decide_project(
    project: Option<Project>,
    role: Option<ProjectRole>,
    need: ProjectRole,
) -> Result<(Project, ProjectRole), ApiError> {
    let (Some(project), Some(role)) = (project, role) else {
        return Err(ApiError::NotFound(NO_SUCH_PROJECT));
    };
    if role < need {
        return Err(ApiError::Forbidden(project_role_needed(need)));
    }
    Ok((project, role))
}

/// [`require_project`] read again inside a transaction, so that a change made with it is refused
/// (with the same answer) if the project went or the caller's role was lowered since the first
/// check.
pub async fn recheck_project(
    tx: &mut dyn Tx,
    user: &str,
    project: &str,
    need: ProjectRole,
) -> Result<(Project, ProjectRole), ApiError> {
    let found = tx.project_by_id(project).await?;
    let mut role = None;
    if let Some(p) = &found
        && let Some(m) = tx.org_member(&p.org_id, user).await?
    {
        if parse_role::<OrgRole>(&m.role)? >= OrgRole::Admin {
            role = Some(ProjectRole::Owner);
        } else if let Some(pm) = tx.project_member(&p.id, user).await? {
            role = Some(parse_role(&pm.role)?);
        }
    }
    decide_project(found, role, need)
}

/// What a caller who cannot open a document is told: the same as for one that does not exist.
pub const NO_SUCH_DOCUMENT: &str = "no such document";

/// A 404 from the project, said about the document instead (a 403 passes through).
fn as_document_error(err: ApiError) -> ApiError {
    match err {
        ApiError::NotFound(_) => ApiError::NotFound(NO_SUCH_DOCUMENT),
        other => other,
    }
}

/// The document, its project and the account's role in it, which must be `need` or higher: 404 `no
/// such document` when there is no such document, it was deleted, or the account has no role in its
/// project (the same answer for all three); 403 `needs the project role <need>` when it is lower.
pub async fn require_document(
    store: &dyn Store,
    user: &str,
    document: &str,
    need: ProjectRole,
) -> Result<(Document, Project, ProjectRole), ApiError> {
    let doc = store
        .document_by_id(document)
        .await?
        .filter(|d| d.deleted_at.is_none())
        .ok_or(ApiError::NotFound(NO_SUCH_DOCUMENT))?;
    let (project, role) = require_project(store, user, &doc.project_id, need)
        .await
        .map_err(as_document_error)?;
    Ok((doc, project, role))
}

/// [`require_document`] read again inside a transaction, so that a change made with it is refused
/// (with the same answer) if the document was deleted or the caller's role lowered since.
pub async fn recheck_document(
    tx: &mut dyn Tx,
    user: &str,
    document: &str,
    need: ProjectRole,
) -> Result<(Document, Project, ProjectRole), ApiError> {
    let doc = tx
        .document_by_id(document)
        .await?
        .filter(|d| d.deleted_at.is_none())
        .ok_or(ApiError::NotFound(NO_SUCH_DOCUMENT))?;
    let (project, role) = recheck_project(tx, user, &doc.project_id, need)
        .await
        .map_err(as_document_error)?;
    Ok((doc, project, role))
}

/// The projects of an organisation the account can see, with its role in each, by name: all of
/// them as owner for an organisation admin or owner (`org_role`), otherwise those it has a role in.
pub async fn visible_projects(
    store: &dyn Store,
    user: &str,
    org: &str,
    org_role: OrgRole,
) -> Result<Vec<ProjectWithRole>, ApiError> {
    if org_role >= OrgRole::Admin {
        return Ok(store
            .projects_in_org(org)
            .await?
            .into_iter()
            .map(|p| ProjectWithRole {
                id: p.id,
                name: p.name,
                org_id: p.org_id,
                role: ProjectRole::Owner.as_str().to_owned(),
            })
            .collect());
    }
    let rows = store.member_projects(org, user).await?;
    for row in &rows {
        parse_role::<ProjectRole>(&row.role)?;
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Request;

    fn parts(auth: Option<&str>) -> Parts {
        let mut b = Request::builder();
        if let Some(a) = auth {
            b = b.header(AUTHORIZATION, a);
        }
        b.body(()).unwrap().into_parts().0
    }

    #[test]
    fn the_bearer_scheme_is_read_in_any_case_and_the_token_trimmed() {
        assert_eq!(bearer(&parts(Some("Bearer abc"))), "abc");
        assert_eq!(bearer(&parts(Some("bEaReR   abc  "))), "abc");
        assert_eq!(bearer(&parts(Some("Basic abc"))), "");
        assert_eq!(bearer(&parts(Some("Bearer"))), "");
        assert_eq!(bearer(&parts(Some("Bearerabc"))), "");
        assert_eq!(bearer(&parts(None)), "");
        let mut latin1 = parts(None);
        latin1.headers.insert(
            AUTHORIZATION,
            axum::http::HeaderValue::from_bytes(b"Bearer \xe9t\xe9").unwrap(),
        );
        assert_eq!(bearer(&latin1), "", "not visible ASCII");
    }
}

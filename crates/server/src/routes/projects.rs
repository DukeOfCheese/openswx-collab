//! Projects and who has a role in them.
//!
//! - `GET` / `POST /api/orgs/{org}/projects`
//! - `PATCH` / `DELETE /api/projects/{project}`
//! - `GET /api/projects/{project}/members`, `PUT` / `DELETE /api/projects/{project}/members/{user}`
//!
//! Any member of an organisation may start a project, and owns it. Organisation owners and admins
//! own every project; everyone else sees the projects they have a role in. A project you have no
//! role in answers 404 `no such project`, exactly as one that does not exist ([`require_project`]).

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, patch, put};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::access::{
    CurrentUser, recheck_org, recheck_project, require_org, require_project, visible_projects,
};
use crate::error::{ApiError, ApiJson, ApiPath};
use crate::ext::{Event, Quota, check_quota, emit};
use crate::routes::orgs::clean_name;
use crate::security::{OrgRole, ProjectRole};
use crate::state::AppState;
use crate::store::{Project, ProjectMember, new_id};

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/api/orgs/{org_id}/projects",
            get(list_projects).post(create_project),
        )
        .route(
            "/api/projects/{project_id}",
            patch(rename_project).delete(delete_project),
        )
        .route("/api/projects/{project_id}/members", get(list_members))
        .route(
            "/api/projects/{project_id}/members/{user_id}",
            put(set_member).delete(remove_member),
        )
}

#[derive(Debug, Deserialize)]
struct NameIn {
    name: String,
}

/// The projects of the organisation the caller can see, with their role in each, by name.
async fn list_projects(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiPath(org_id): ApiPath<String>,
) -> Result<Json<Value>, ApiError> {
    let role = require_org(state.store.as_ref(), &user.id, &org_id, OrgRole::Member).await?;
    let projects: Vec<Value> = visible_projects(state.store.as_ref(), &user.id, &org_id, role)
        .await?
        .into_iter()
        .map(|p| json!({"id": p.id, "name": p.name, "org_id": p.org_id, "role": p.role}))
        .collect();
    Ok(Json(Value::Array(projects)))
}

/// Starts a project, owned by the caller (if the extensions' quota allows one more: 402 if not).
async fn create_project(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiPath(org_id): ApiPath<String>,
    ApiJson(body): ApiJson<NameIn>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let name = clean_name(&body.name)?;
    require_org(state.store.as_ref(), &user.id, &org_id, OrgRole::Member).await?;
    check_quota(&state.ext, &org_id, Quota::Projects).await?;
    let project = Project {
        id: new_id(),
        org_id: org_id.clone(),
        name,
        created_at: state.clock.now(),
    };
    let mut tx = state.store.begin().await?;
    recheck_org(&mut *tx, &user.id, &org_id, OrgRole::Member).await?;
    tx.insert_project(&project).await?;
    tx.insert_project_member(&ProjectMember {
        project_id: project.id.clone(),
        user_id: user.id.clone(),
        role: ProjectRole::Owner.as_str().to_owned(),
    })
    .await?;
    tx.commit().await?;
    emit(
        &state.ext,
        Event::ProjectCreated {
            org: org_id.clone(),
            project: project.id.clone(),
            by: user.id,
        },
    )
    .await;
    Ok((
        StatusCode::CREATED,
        Json(json!({"id": project.id, "name": project.name, "org_id": org_id, "role": "owner"})),
    ))
}

async fn rename_project(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiPath(project_id): ApiPath<String>,
    ApiJson(body): ApiJson<NameIn>,
) -> Result<Json<Value>, ApiError> {
    let name = clean_name(&body.name)?;
    let (project, _) = require_project(
        state.store.as_ref(),
        &user.id,
        &project_id,
        ProjectRole::Owner,
    )
    .await?;
    let mut tx = state.store.begin().await?;
    recheck_project(&mut *tx, &user.id, &project.id, ProjectRole::Owner).await?;
    tx.set_project_name(&project.id, &name).await?;
    tx.commit().await?;
    emit(
        &state.ext,
        Event::ProjectRenamed {
            org: project.org_id.clone(),
            project: project.id.clone(),
            by: user.id,
        },
    )
    .await;
    Ok(Json(
        json!({"id": project.id, "name": name, "org_id": project.org_id}),
    ))
}

/// Deletes a project and everything in it, and closes its open documents. (Snapshot files in
/// storage are left, as the Python server leaves them.)
async fn delete_project(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiPath(project_id): ApiPath<String>,
) -> Result<StatusCode, ApiError> {
    let (project, _) = require_project(
        state.store.as_ref(),
        &user.id,
        &project_id,
        ProjectRole::Owner,
    )
    .await?;
    let mut tx = state.store.begin().await?;
    recheck_project(&mut *tx, &user.id, &project.id, ProjectRole::Owner).await?;
    let documents = tx.project_document_ids(&project.id).await?;
    // As for one document: mark them deleted first, then look for users outside the project, in one
    // transaction.
    tx.mark_project_documents_deleted(&project.id, state.clock.now())
        .await?;
    // TASK 13: refuse with 409 when assemblies in other projects use parts of this one
    // (`{"message", "assemblies" the caller can open, "in_projects_you_cannot_open"}`).
    tx.delete_project(&project.id).await?;
    tx.commit().await?;
    for document in &documents {
        state
            .hub
            .close_document(document, "the project was deleted")
            .await;
    }
    emit(
        &state.ext,
        Event::ProjectDeleted {
            org: project.org_id,
            project: project.id,
            by: user.id,
        },
    )
    .await;
    Ok(StatusCode::NO_CONTENT)
}

/// The people with a role row in the project (organisation admins appear only if they have one).
async fn list_members(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiPath(project_id): ApiPath<String>,
) -> Result<Json<Value>, ApiError> {
    require_project(
        state.store.as_ref(),
        &user.id,
        &project_id,
        ProjectRole::Viewer,
    )
    .await?;
    let members: Vec<Value> = state
        .store
        .project_members(&project_id)
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

/// Gives a member of the organisation a role in the project, or changes it. Their open documents
/// in the organisation close (reconnecting picks up the new role).
async fn set_member(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiPath((project_id, user_id)): ApiPath<(String, String)>,
    ApiJson(body): ApiJson<RoleIn>,
) -> Result<Json<Value>, ApiError> {
    let role: ProjectRole = body
        .role
        .parse()
        .map_err(|_| ApiError::Unprocessable("role must be one of owner, editor, viewer".into()))?;
    let (project, _) = require_project(
        state.store.as_ref(),
        &user.id,
        &project_id,
        ProjectRole::Owner,
    )
    .await?;
    let mut tx = state.store.begin().await?;
    recheck_project(&mut *tx, &user.id, &project.id, ProjectRole::Owner).await?;
    if tx.org_member(&project.org_id, &user_id).await?.is_none() {
        return Err(ApiError::Unprocessable(
            "that person is not in the organisation; invite them first".into(),
        ));
    }
    if tx.project_member(&project.id, &user_id).await?.is_some() {
        tx.set_project_member_role(&project.id, &user_id, role.as_str())
            .await?;
    } else {
        tx.insert_project_member(&ProjectMember {
            project_id: project.id.clone(),
            user_id: user_id.clone(),
            role: role.as_str().to_owned(),
        })
        .await?;
    }
    tx.commit().await?;
    state.hub.drop_user(&user_id, &project.org_id).await;
    emit(
        &state.ext,
        Event::ProjectMemberSet {
            org: project.org_id,
            project: project.id,
            user: user_id.clone(),
            role: role.as_str(),
            by: user.id,
        },
    )
    .await;
    Ok(Json(json!({"user_id": user_id, "role": role.as_str()})))
}

/// Takes away someone's role in the project. Their open documents in the organisation close.
async fn remove_member(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiPath((project_id, user_id)): ApiPath<(String, String)>,
) -> Result<StatusCode, ApiError> {
    let (project, _) = require_project(
        state.store.as_ref(),
        &user.id,
        &project_id,
        ProjectRole::Owner,
    )
    .await?;
    let mut tx = state.store.begin().await?;
    recheck_project(&mut *tx, &user.id, &project.id, ProjectRole::Owner).await?;
    if !tx.delete_project_member(&project.id, &user_id).await? {
        return Err(ApiError::NotFound("not a member of the project"));
    }
    tx.commit().await?;
    state.hub.drop_user(&user_id, &project.org_id).await;
    emit(
        &state.ext,
        Event::ProjectMemberRemoved {
            org: project.org_id,
            project: project.id,
            user: user_id,
            by: user.id,
        },
    )
    .await;
    Ok(StatusCode::NO_CONTENT)
}

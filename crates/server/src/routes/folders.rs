//! Folders inside a project. Folders nest (at most [`MAX_FOLDER_DEPTH`] deep, a folder at the top
//! of the project being 1 deep); documents sit in one folder or at the top.
//!
//! - `GET /api/projects/{project}/folders` (viewer): every folder of the project, by name.
//! - `POST /api/projects/{project}/folders` (editor): `{name, parent_id?}`; 201 with the folder.
//! - `PATCH /api/folders/{folder}` (editor): `name` and/or `parent_id` (`null`: the top). A folder
//!   cannot be moved into itself or a folder inside it (422).
//! - `DELETE /api/folders/{folder}` (editor): 204. What was inside it (documents and folders) moves
//!   to its parent, in the same transaction; nothing else is deleted.
//!
//! A folder answers as `{id, project_id, parent_id, name, created_at}`. Names are 1 to 200
//! characters after trimming, and need not be unique. A folder that does not exist or is in a
//! project the caller has no role in answers 404 `no such folder`; naming, as a parent, a folder
//! that is not in the same project answers 422.

use std::collections::HashMap;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, patch};
use serde::Deserialize;
use serde_json::json;

use crate::access::{CurrentUser, recheck_project, require_project};
use crate::error::{ApiError, ApiJson, ApiPath};
use crate::ext::{Event, emit};
use crate::routes::documents::{NO_FOLDER_HERE, folder_in, present};
use crate::routes::orgs::clean_name;
use crate::security::ProjectRole;
use crate::state::AppState;
use crate::store::{Folder, Project, Tx, new_id};

/// How deep folders may nest.
pub const MAX_FOLDER_DEPTH: usize = 32;
const NO_SUCH_FOLDER: &str = "no such folder";
const TOO_DEEP: &str = "folders nest at most 32 deep";

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/api/projects/{project_id}/folders",
            get(list_folders).post(create_folder),
        )
        .route(
            "/api/folders/{folder_id}",
            patch(patch_folder).delete(delete_folder),
        )
}

fn folder_out(f: &Folder) -> serde_json::Value {
    json!({"id": f.id, "project_id": f.project_id, "parent_id": f.parent_id, "name": f.name,
           "created_at": f.created_at})
}

/// The folders of one project, by id, for walking up and down the tree.
struct Tree<'a> {
    parent: HashMap<&'a str, Option<&'a str>>,
}

impl<'a> Tree<'a> {
    fn new(folders: &'a [Folder]) -> Self {
        Self {
            parent: folders
                .iter()
                .map(|f| (f.id.as_str(), f.parent_id.as_deref()))
                .collect(),
        }
    }

    fn contains(&self, id: &str) -> bool {
        self.parent.contains_key(id)
    }

    /// `id` and the folders above it, nearest first (stopping, should the data hold a cycle, once
    /// every folder has been seen).
    fn ancestry<'b>(&'b self, id: &'b str) -> Vec<&'b str> {
        let mut out = Vec::new();
        let mut cur = Some(id);
        while let Some(c) = cur {
            if out.len() > self.parent.len() {
                break;
            }
            out.push(c);
            cur = self.parent.get(c).copied().flatten();
        }
        out
    }

    /// How deep `id` is: 1 at the top of the project.
    fn depth(&self, id: &str) -> usize {
        self.ancestry(id).len()
    }

    /// How many levels `id` and the folders inside it take: 1 for a folder with none inside.
    fn height(&self, id: &str) -> usize {
        self.parent
            .keys()
            .filter_map(|f| {
                let up = self.ancestry(f);
                up.iter().position(|a| *a == id).map(|i| i + 1)
            })
            .max()
            .unwrap_or(1)
    }
}

async fn list_folders(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiPath(project_id): ApiPath<String>,
) -> Result<ApiJson<Vec<serde_json::Value>>, ApiError> {
    let (project, _) = require_project(
        state.store.as_ref(),
        &user.id,
        &project_id,
        ProjectRole::Viewer,
    )
    .await?;
    let folders = state.store.folders_in_project(&project.id).await?;
    Ok(ApiJson(folders.iter().map(folder_out).collect()))
}

#[derive(Debug, Deserialize)]
struct FolderIn {
    name: String,
    parent_id: Option<String>,
}

async fn create_folder(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiPath(project_id): ApiPath<String>,
    ApiJson(body): ApiJson<FolderIn>,
) -> Result<(StatusCode, ApiJson<serde_json::Value>), ApiError> {
    let name = clean_name(&body.name)?;
    let (project, _) = require_project(
        state.store.as_ref(),
        &user.id,
        &project_id,
        ProjectRole::Editor,
    )
    .await?;
    if let Some(parent) = &body.parent_id {
        folder_in(state.store.folder_by_id(parent).await?, &project.id)?;
    }
    let folder = Folder {
        id: new_id(),
        project_id: project.id.clone(),
        parent_id: body.parent_id,
        name,
        created_at: state.clock.now(),
    };
    let turn = state.org_turn(&project.org_id).await?;
    let mut tx = state.store.begin().await?;
    recheck_project(&mut *tx, &user.id, &project.id, ProjectRole::Editor).await?;
    if let Some(parent) = &folder.parent_id {
        let folders = tx.folders_in_project(&project.id).await?;
        let tree = Tree::new(&folders);
        if !tree.contains(parent) {
            return Err(ApiError::Unprocessable(NO_FOLDER_HERE.into()));
        }
        if tree.depth(parent) + 1 > MAX_FOLDER_DEPTH {
            return Err(ApiError::Unprocessable(TOO_DEEP.into()));
        }
    }
    tx.insert_folder(&folder).await?;
    let event = Event::FolderCreated {
        org: project.org_id,
        project: project.id,
        folder: folder.id.clone(),
        by: user.id,
    };
    let ext = state.ext.clone();
    state
        .commit_then(
            (tx, turn),
            |_| async {},
            async move { emit(&ext, event).await },
        )
        .await?;
    Ok((StatusCode::CREATED, ApiJson(folder_out(&folder))))
}

/// A 404 from the project, said about the folder instead.
fn as_folder_error(err: ApiError) -> ApiError {
    match err {
        ApiError::NotFound(_) => ApiError::NotFound(NO_SUCH_FOLDER),
        other => other,
    }
}

/// The folder and its project; the caller's role there must be `need` or higher. 404 `no such
/// folder` for a missing folder and for one in a project the caller has no role in.
async fn require_folder(
    state: &AppState,
    user: &str,
    folder: &str,
    need: ProjectRole,
) -> Result<(Folder, Project), ApiError> {
    let folder = state
        .store
        .folder_by_id(folder)
        .await?
        .ok_or(ApiError::NotFound(NO_SUCH_FOLDER))?;
    let (project, _) = require_project(state.store.as_ref(), user, &folder.project_id, need)
        .await
        .map_err(as_folder_error)?;
    Ok((folder, project))
}

/// [`require_folder`] read again inside the transaction: the folder as it is now.
async fn recheck_folder(
    tx: &mut dyn Tx,
    user: &str,
    folder: &str,
    need: ProjectRole,
) -> Result<Folder, ApiError> {
    let project_id = tx
        .folder_by_id(folder)
        .await?
        .ok_or(ApiError::NotFound(NO_SUCH_FOLDER))?
        .project_id;
    recheck_project(tx, user, &project_id, need)
        .await
        .map_err(as_folder_error)?;
    // As it is now that the organisation is locked (a folder never changes project).
    tx.folder_by_id(folder)
        .await?
        .ok_or(ApiError::NotFound(NO_SUCH_FOLDER))
}

#[derive(Debug, Deserialize)]
struct FolderPatch {
    name: Option<String>,
    #[serde(default, deserialize_with = "present")]
    parent_id: Option<Option<String>>,
}

async fn patch_folder(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiPath(folder_id): ApiPath<String>,
    ApiJson(body): ApiJson<FolderPatch>,
) -> Result<ApiJson<serde_json::Value>, ApiError> {
    if body.name.is_none() && body.parent_id.is_none() {
        return Err(ApiError::Unprocessable("give name or parent_id".into()));
    }
    let name = body.name.as_deref().map(clean_name).transpose()?;
    let (folder, project) =
        require_folder(&state, &user.id, &folder_id, ProjectRole::Editor).await?;
    if let Some(Some(parent)) = &body.parent_id {
        folder_in(state.store.folder_by_id(parent).await?, &project.id)?;
    }
    let turn = state.org_turn(&project.org_id).await?;
    let mut tx = state.store.begin().await?;
    let mut folder = recheck_folder(&mut *tx, &user.id, &folder.id, ProjectRole::Editor).await?;
    if let Some(parent) = &body.parent_id {
        if let Some(parent) = parent {
            let folders = tx.folders_in_project(&project.id).await?;
            let tree = Tree::new(&folders);
            if !tree.contains(parent) {
                return Err(ApiError::Unprocessable(NO_FOLDER_HERE.into()));
            }
            if tree.ancestry(parent).contains(&folder.id.as_str()) {
                return Err(ApiError::Unprocessable(
                    "a folder cannot be moved into itself or a folder inside it".into(),
                ));
            }
            if tree.depth(parent) + tree.height(&folder.id) > MAX_FOLDER_DEPTH {
                return Err(ApiError::Unprocessable(TOO_DEEP.into()));
            }
        }
        tx.set_folder_parent(&folder.id, parent.as_deref()).await?;
        folder.parent_id.clone_from(parent);
    }
    if let Some(name) = &name {
        tx.set_folder_name(&folder.id, name).await?;
        folder.name.clone_from(name);
    }
    let mut events = Vec::new();
    if name.is_some() {
        events.push(Event::FolderRenamed {
            org: project.org_id.clone(),
            project: project.id.clone(),
            folder: folder.id.clone(),
            by: user.id.clone(),
        });
    }
    if body.parent_id.is_some() {
        events.push(Event::FolderMoved {
            org: project.org_id.clone(),
            project: project.id.clone(),
            folder: folder.id.clone(),
            parent: folder.parent_id.clone(),
            by: user.id.clone(),
        });
    }
    let ext = state.ext.clone();
    state
        .commit_then((tx, turn), |_| async {}, async move {
            for event in events {
                emit(&ext, event).await;
            }
        })
        .await?;
    Ok(ApiJson(folder_out(&folder)))
}

/// Deletes a folder; its documents and the folders inside it move to its parent.
async fn delete_folder(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiPath(folder_id): ApiPath<String>,
) -> Result<StatusCode, ApiError> {
    let (folder, project) =
        require_folder(&state, &user.id, &folder_id, ProjectRole::Editor).await?;
    let turn = state.org_turn(&project.org_id).await?;
    let mut tx = state.store.begin().await?;
    let folder = recheck_folder(&mut *tx, &user.id, &folder.id, ProjectRole::Editor).await?;
    let parent = folder.parent_id.as_deref();
    tx.move_folder_documents(&folder.id, parent).await?;
    tx.move_child_folders(&folder.id, parent).await?;
    tx.delete_folder(&folder.id).await?;
    let event = Event::FolderDeleted {
        org: project.org_id,
        project: project.id,
        folder: folder.id,
        by: user.id,
    };
    let ext = state.ext.clone();
    state
        .commit_then(
            (tx, turn),
            |_| async {},
            async move { emit(&ext, event).await },
        )
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn folder(id: &str, parent: Option<&str>) -> Folder {
        Folder {
            id: id.into(),
            project_id: "p".into(),
            parent_id: parent.map(Into::into),
            name: id.into(),
            created_at: 0.0,
        }
    }

    #[test]
    fn depth_and_height_walk_the_tree() {
        let folders = vec![
            folder("a", None),
            folder("b", Some("a")),
            folder("c", Some("b")),
            folder("d", Some("a")),
            folder("e", None),
        ];
        let tree = Tree::new(&folders);
        assert_eq!(tree.depth("a"), 1);
        assert_eq!(tree.depth("c"), 3);
        assert_eq!(tree.height("a"), 3);
        assert_eq!(tree.height("b"), 2);
        assert_eq!(tree.height("e"), 1);
        assert_eq!(tree.ancestry("c"), vec!["c", "b", "a"]);
    }

    #[test]
    fn a_cycle_in_the_data_does_not_loop() {
        let folders = vec![folder("a", Some("b")), folder("b", Some("a"))];
        let tree = Tree::new(&folders);
        assert!(tree.depth("a") <= 3);
        assert!(tree.height("a") <= 3);
    }
}

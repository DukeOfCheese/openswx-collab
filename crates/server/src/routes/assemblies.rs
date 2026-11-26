//! Assemblies: what an assembly's components use, and where a part is used (see
//! [`crate::assemblies`] for how references are checked and recorded).
//!
//! - `GET /api/documents/{assembly}/references` (viewer; 422 `only assemblies have references`
//!   for a part): per component, by component id, what it is pinned to and whether its part has
//!   moved on. A part the caller cannot open (or that no longer exists) is told as
//!   `{"component", "document": {"id", "accessible": false}, "pinned": {"seq"}}`: nothing the
//!   assembly does not already say. Otherwise `{"component", "document": {"id", "name",
//!   "project_id", "accessible": true, "deleted"}, "pinned": {"seq", "versions": [names of the
//!   part's versions at that seq]}, "head_seq", "latest_version": {"id", "name", "seq"} | null,
//!   "update_available"` (a named version newer than the pin), `"newer_edits"` (edited since the
//!   pin), `"changes_since_pinned"` (changes after the pin up to the latest version if that is
//!   newer, else up to the head)`}`.
//! - `GET /api/documents/{part}/used-in` (viewer): `[{"document": {"id", "name", "project_id"},
//!   "components": [{"component", "seq"}]}]` for the assemblies (not deleted) that use it, by name;
//!   only those the caller can open: the others are neither listed nor counted.
//!
//! Deleting a part that assemblies use, or a project whose parts assemblies of other projects use,
//! is a 409 made by `in_use`: `{"message", "assemblies": [{"id", "name"} of those the caller can
//! open], "in_projects_you_cannot_open": whether there are others}`, never naming or counting the
//! others.

use std::collections::{BTreeSet, HashMap};

use axum::Router;
use axum::extract::State;
use axum::routing::get;
use serde_json::{Value, json};

use crate::access::{CurrentUser, require_document};
use crate::assemblies::Access;
use crate::error::{ApiError, ApiJson, ApiPath};
use crate::security::ProjectRole;
use crate::state::AppState;
use crate::store::{AssemblyUse, Id, PartRow, VersionWithAuthor};

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/documents/{document_id}/references", get(references))
        .route("/api/documents/{document_id}/used-in", get(used_in))
}

/// A part as `references` shows it to someone who can open it.
struct Shown {
    part: PartRow,
    /// Newest seq first.
    versions: Vec<VersionWithAuthor>,
}

async fn references(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiPath(document_id): ApiPath<String>,
) -> Result<ApiJson<Vec<Value>>, ApiError> {
    let store = state.store.as_ref();
    let (doc, project, _) =
        require_document(store, &user.id, &document_id, ProjectRole::Viewer).await?;
    if doc.kind != "assembly" {
        return Err(ApiError::Unprocessable(
            "only assemblies have references".into(),
        ));
    }
    let rows = store.assembly_refs(&doc.id).await?;
    let ids: Vec<Id> = rows
        .iter()
        .map(|r| r.part_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let access = Access::of(store, &user.id, &project.org_id).await?;
    // A bounded number of reads whatever the number of components: the parts, then the versions
    // of those the caller can open, each in batches.
    let mut shown: HashMap<Id, Shown> = store
        .parts_by_ids(&ids)
        .await?
        .into_iter()
        .filter(|p| access.opens(&p.org_id, &p.project_id))
        .map(|part| {
            let shown = Shown {
                part,
                versions: Vec::new(),
            };
            (shown.part.id.clone(), shown)
        })
        .collect();
    let openable: Vec<Id> = shown.keys().cloned().collect();
    for v in store.versions_of(&openable).await? {
        if let Some(s) = shown.get_mut(&v.document_id) {
            s.versions.push(v);
        }
    }
    let out = rows
        .iter()
        .map(|r| {
            let Some(Shown { part, versions }) = shown.get(&r.part_id) else {
                // Not one the caller can open (or gone): nothing the assembly does not say.
                return json!({"component": r.component_id,
                              "document": {"id": r.part_id, "accessible": false},
                              "pinned": {"seq": r.seq}});
            };
            let latest = versions.first();
            let newer = latest.filter(|v| v.seq > r.seq);
            let target = newer.map_or(part.head_seq, |v| v.seq);
            json!({
                "component": r.component_id,
                "document": {"id": part.id, "name": part.name, "project_id": part.project_id,
                             "accessible": true, "deleted": part.deleted_at.is_some()},
                "pinned": {"seq": r.seq,
                           "versions": versions.iter().filter(|v| v.seq == r.seq)
                               .map(|v| v.name.as_str()).collect::<Vec<_>>()},
                "head_seq": part.head_seq,
                "latest_version": latest.map(|v| json!({"id": v.id, "name": v.name, "seq": v.seq})),
                "update_available": newer.is_some(),
                "newer_edits": part.head_seq > r.seq,
                // The journal holds every seq: the changes after the pin up to the target.
                "changes_since_pinned": (target - r.seq).max(0),
            })
        })
        .collect();
    Ok(ApiJson(out))
}

async fn used_in(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiPath(document_id): ApiPath<String>,
) -> Result<ApiJson<Vec<Value>>, ApiError> {
    let store = state.store.as_ref();
    let (doc, project, _) =
        require_document(store, &user.id, &document_id, ProjectRole::Viewer).await?;
    let access = Access::of(store, &user.id, &project.org_id).await?;
    // Every reference to the part in one read, grouped by assembly.
    let mut by_assembly: HashMap<Id, Vec<Value>> = HashMap::new();
    for r in store.refs_to_part(&doc.id).await? {
        by_assembly
            .entry(r.assembly_id)
            .or_default()
            .push(json!({"component": r.component_id, "seq": r.seq}));
    }
    let mut out = Vec::new();
    for asm in store.assemblies_using(&doc.id).await? {
        if !access.opens(&asm.org_id, &asm.project_id) {
            continue;
        }
        let components = by_assembly.remove(&asm.id).unwrap_or_default();
        out.push(json!({
            "document": {"id": asm.id, "name": asm.name, "project_id": asm.project_id},
            "components": components,
        }));
    }
    Ok(ApiJson(out))
}

/// The 409 for deleting what assemblies use: `{"message": message, "assemblies": [{"id", "name"}
/// of those the caller can open], "in_projects_you_cannot_open": whether there are others}`. The
/// others are neither named nor counted. Call it after the transaction has been rolled back (it
/// reads the caller's roles).
pub(crate) async fn in_use(
    state: &AppState,
    user: &str,
    org: &str,
    message: &str,
    users: Vec<AssemblyUse>,
) -> Result<ApiError, ApiError> {
    let access = Access::of(state.store.as_ref(), user, org).await?;
    let (shown, hidden): (Vec<AssemblyUse>, Vec<AssemblyUse>) = users
        .into_iter()
        .partition(|a| access.opens(&a.org_id, &a.project_id));
    let shown: Vec<Value> = shown
        .iter()
        .map(|a| json!({"id": a.id, "name": a.name}))
        .collect();
    Ok(ApiError::ConflictWith(json!({
        "message": message,
        "assemblies": shown,
        "in_projects_you_cannot_open": !hidden.is_empty(),
    })))
}

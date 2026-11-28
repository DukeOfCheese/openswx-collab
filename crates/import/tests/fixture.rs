//! The Python-shaped source fixture builds what the Python server's own code builds.

mod common;

use std::io::Read;

use common::{PyOp, py_source};
use serde_json::json;
use sqlx::Row;

const TABLES: [&str; 18] = [
    "alembic_version",
    "assembly_refs",
    "documents",
    "invites",
    "login_codes",
    "meta",
    "oauth_identities",
    "oauth_states",
    "ops",
    "org_members",
    "orgs",
    "password_resets",
    "project_members",
    "projects",
    "sessions",
    "snapshots",
    "users",
    "versions",
];

#[tokio::test]
async fn the_fixture_matches_the_python_models() {
    let src = py_source().await;
    let pool = src.pool().await;
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(tables, TABLES);
    let revision: String = sqlx::query_scalar("SELECT version_num FROM alembic_version")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(revision, "0003");

    let user = src.user("ada@example.com").await;
    let org = src.org("Robotics", &user).await;
    let project = src.project(&org, "Rover").await;
    let content = json!({"format": "openswx-part", "version": 3, "features": []});
    let doc = src.document(&project, "part", content.clone()).await;
    for n in 1..=2 {
        let seq = src
            .op(
                &doc,
                PyOp {
                    author: user.clone(),
                    client_id: Some(format!("{user}.app")),
                    change_id: Some(format!("c{n}")),
                    command: "edit".into(),
                    ops: json!([]),
                    inverse_ops: json!([]),
                    reverts: None,
                },
            )
            .await;
        assert_eq!(seq, n);
    }
    let head: i64 = sqlx::query_scalar("SELECT head_seq FROM documents WHERE id = $1")
        .bind(&doc)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(head, 2);

    let row = sqlx::query("SELECT key, size FROM snapshots WHERE document_id = $1 AND seq = 0")
        .bind(&doc)
        .fetch_one(&pool)
        .await
        .unwrap();
    let key: String = row.get("key");
    assert_eq!(key, format!("docs/{doc}/snapshots/0000000000.json.gz"));
    let bytes = std::fs::read(src.storage.join(&key)).unwrap();
    assert_eq!(row.get::<i64, _>("size"), bytes.len() as i64);
    let mut text = String::new();
    flate2::read::GzDecoder::new(&bytes[..])
        .read_to_string(&mut text)
        .unwrap();
    assert_eq!(text, content.to_string());
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&text).unwrap(),
        content
    );
}

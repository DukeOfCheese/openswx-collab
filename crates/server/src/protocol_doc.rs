//! `docs/protocol.md` names every WebSocket close code and every `error` code the server sends.
//!
//! The messages, fields, operations and change codes are checked in `oswx_core`
//! (`crates/core/tests/protocol_doc.rs`); the close and `error` codes are the server's, so they are
//! checked here. Both lists are read from the server's own source, so a new code fails here until the
//! document has a row for it: the close codes are the `u16` constants of [`crate::room::close`],
//! and the `error` codes the string literals given to the functions that send an `error` frame.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::room::close;

/// The functions whose first string literal argument is an `error` frame's code.
const SENDERS: [&str; 3] = ["refuse(", "send_refusal(", "malformed("];

fn read(path: &Path) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("{} cannot be read: {e}", path.display()))
        .replace("\r\n", "\n")
}

fn doc() -> String {
    read(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/protocol.md"))
}

/// Every `.rs` file under `dir` but this one.
fn sources(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            sources(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs")
            && path.file_name().is_none_or(|n| n != "protocol_doc.rs")
        {
            out.push(path);
        }
    }
}

/// The close codes: every `pub const NAME: u16 = <code>;` of the `close` module.
fn close_codes(room_mod: &str) -> BTreeSet<u16> {
    let start = room_mod
        .find("pub mod close {")
        .expect("room/mod.rs has a `close` module");
    let body = &room_mod[start..];
    let end = body.find("\n}\n").expect("the `close` module ends");
    body[..end]
        .lines()
        .filter_map(|line| {
            let rest = line.trim().strip_prefix("pub const ")?;
            let (_, value) = rest.split_once(": u16 = ")?;
            value.strip_suffix(';')?.parse().ok()
        })
        .collect()
}

/// The `error` codes: the first string literal inside each call of one of [`SENDERS`].
fn error_codes(source: &str) -> BTreeSet<String> {
    let mut codes = BTreeSet::new();
    for sender in SENDERS {
        let mut rest = source;
        while let Some(at) = rest.find(sender) {
            rest = &rest[at + sender.len()..];
            // The call's arguments: up to the parenthesis that closes it.
            let mut depth = 1;
            let end = rest
                .char_indices()
                .find(|&(_, c)| {
                    match c {
                        '(' => depth += 1,
                        ')' => depth -= 1,
                        _ => {}
                    }
                    depth == 0
                })
                .map_or(rest.len(), |(i, _)| i);
            let args = &rest[..end];
            if let Some(open) = args.find('"') {
                let literal = &args[open + 1..];
                if let Some(close) = literal.find('"') {
                    codes.insert(literal[..close].to_owned());
                }
            }
        }
    }
    codes
}

/// The part of `doc` from the line `heading` to the next heading of its level or above.
fn section<'a>(doc: &'a str, heading: &str) -> &'a str {
    let start = doc
        .find(&format!("\n{heading}\n"))
        .unwrap_or_else(|| panic!("docs/protocol.md has no section {heading:?}"))
        + 1;
    let level = heading.split(' ').next().unwrap();
    let after = start + heading.len();
    let end = (1..=level.len())
        .filter_map(|n| doc[after..].find(&format!("\n{} ", "#".repeat(n))))
        .min()
        .map_or(doc.len(), |e| after + e);
    &doc[start..end]
}

/// The first cells of `table`'s rows that are missing from `rows`.
fn missing_rows(table: &str, rows: impl IntoIterator<Item = String>) -> Vec<String> {
    rows.into_iter()
        .filter(|cell| {
            !table
                .lines()
                .any(|line| line.starts_with(&format!("| {cell} |")))
        })
        .collect()
}

fn source_codes() -> (BTreeSet<u16>, BTreeSet<String>) {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let closes = close_codes(&read(&src.join("room/mod.rs")));
    let mut files = Vec::new();
    sources(&src, &mut files);
    let errors = files
        .iter()
        .flat_map(|path| error_codes(&read(path)))
        .collect();
    (closes, errors)
}

#[test]
fn the_codes_are_read_from_the_source() {
    // (So that the reading below is not vacuous: these are known to be there.)
    let (closes, errors) = source_codes();
    for code in [
        close::GOING_AWAY,
        close::PROTOCOL_ERROR,
        close::POLICY,
        close::TOO_BIG,
        close::SERVER_ERROR,
        close::BAD_HELLO,
        close::SIGN_IN,
        close::ACCESS_CHANGED,
        close::GONE,
        close::TOO_SLOW,
        close::REPLACED,
        close::MOVED,
        close::TOO_MANY,
        close::PROTOCOL,
    ] {
        assert!(closes.contains(&code), "{code} was not read: {closes:?}");
    }
    for code in [
        "bad_hello",
        "protocol",
        "unauthorized",
        "too_many_connections",
        "not_found",
        "busy",
        "shutting_down",
        "server_error",
        "bad_message",
        "too_large",
    ] {
        assert!(errors.contains(code), "{code} was not read: {errors:?}");
    }
}

#[test]
fn the_document_names_every_close_code_and_every_error_code() {
    let doc = doc();
    let (closes, errors) = source_codes();
    let missing = missing_rows(
        section(&doc, "## Close codes"),
        closes.iter().map(u16::to_string),
    );
    assert!(
        missing.is_empty(),
        "docs/protocol.md, \"Close codes\": no row for {missing:?}"
    );
    let missing = missing_rows(
        section(&doc, "### Joining"),
        errors.iter().map(|code| format!("`{code}`")),
    );
    assert!(
        missing.is_empty(),
        "docs/protocol.md, the `error` codes under \"Joining\": no row for {missing:?}"
    );
}

#[test]
fn a_code_missing_from_the_document_is_found() {
    // The checks themselves, on a made-up source and document.
    let room_mod = "pub mod close {\n    /// x\n    pub const A: u16 = 4400;\n    pub fn f() {}\n    pub const B: u16 = 4499;\n}\npub const C: u16 = 1;\n";
    assert_eq!(close_codes(room_mod), BTreeSet::from([4400, 4499]));
    let source = "refuse(socket, \"bad_hello\", m, close::BAD_HELLO).await;\n\
                  fn refuse(socket: WebSocket, code: &str) {}\n\
                  send_refusal(\n    &outbox,\n    \"gone_away\",\n    f(x),\n);\n\
                  malformed(&room, conn, &outbox, &mut life, \"too_large\", message)";
    assert_eq!(
        error_codes(source),
        BTreeSet::from(["bad_hello".into(), "gone_away".into(), "too_large".into()])
    );
    let doc = "# T\n\n## Close codes\n\n| code | | |\n|---|---|---|\n| 4400 | bad hello | |\n\n## Next\n\n| 4499 | elsewhere | |\n";
    assert_eq!(
        missing_rows(
            section(doc, "## Close codes"),
            ["4400".to_owned(), "4499".to_owned()]
        ),
        ["4499"]
    );
}

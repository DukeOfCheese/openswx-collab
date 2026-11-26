//! Rebuild reports: which features fail to rebuild, and the change that broke them.
//!
//! Clients rebuild the document themselves and send `rebuild.report {seq, errors}` (`errors`:
//! feature id → message, at most 500 entries, limited by the wire codec). A report is ignored when
//! it comes from a connection that may only look, when `seq` is beyond the durable head, or when it
//! is not newer than the last report the room processed (from anyone). Otherwise:
//!
//! - the first report the room processes only sets the baseline;
//! - `fresh` errors (features not already in `broken`) are blamed, once a baseline exists, on the
//!   change at `seq`: `{by_seq, by_user, by_name, command, since_seq}`, `since_seq` being the first
//!   seq after the baseline; the change's `broke` column gets them merged in, written off the room's
//!   task in a small transaction of its own (a failure is logged; the report stands);
//! - an entry already in `broken` keeps its blame and takes the new message; entries not in the
//!   report are fixed and removed;
//! - when anything was fresh or fixed, everyone gets `rebuild.status {seq, broken}`.
//!
//! `broken` lives in memory only: it is in every welcome and lost when the room unloads. The
//! author, name and command of the most recent changes made durable in the room are kept for the
//! blame ([`BLAME_KEEP`]); an older change is read from the journal, off the task, the reporting
//! connection's later messages waiting.

use std::collections::BTreeMap;
use std::sync::Arc;

use oswx_core::Seq;
use oswx_core::wire::{Broken, ServerMsg};

use super::{ConnId, Internal, Room};
use crate::store::Store;

/// How many recent changes' authors and commands a room keeps for blaming.
pub const BLAME_KEEP: usize = 4096;
/// The most entries a change's `broke` column holds; later ones are not added.
pub const MAX_BROKE: usize = 500;

/// Who made a change, and what it was.
#[derive(Debug, Clone)]
pub(super) struct ChangeInfo {
    user: String,
    name: String,
    command: String,
}

/// The rebuild state of a room.
#[derive(Debug, Default)]
pub(super) struct Rebuilds {
    /// What does not rebuild, by feature id.
    pub(super) broken: BTreeMap<String, Broken>,
    /// The seq of the last report processed.
    last: Option<Seq>,
    /// Recent changes, for the blame.
    recent: BTreeMap<Seq, ChangeInfo>,
}

/// A journal read of the change a report blames, coming back.
pub(super) struct ReportLookup {
    conn: ConnId,
    seq: Seq,
    errors: BTreeMap<String, String>,
    result: anyhow::Result<Option<ChangeInfo>>,
}

impl Room {
    /// Remembers who made the change `seq` (made durable now), for the blame.
    pub(super) fn note_change(&mut self, seq: Seq, user: &str, name: &str, command: &str) {
        let recent = &mut self.rebuilds.recent;
        recent.insert(
            seq,
            ChangeInfo {
                user: user.to_owned(),
                name: name.to_owned(),
                command: command.chars().take(100).collect(),
            },
        );
        while recent.len() > BLAME_KEEP {
            recent.pop_first();
        }
    }

    /// Whether a report about state `seq` is one the room processes now.
    fn report_counts(&self, seq: Seq) -> bool {
        seq <= self.durable.seq && self.rebuilds.last.is_none_or(|last| seq > last)
    }

    pub(super) fn on_report(&mut self, conn: ConnId, seq: Seq, errors: BTreeMap<String, String>) {
        let Some(c) = self.conns.get(&conn) else {
            return;
        };
        // (Someone who cannot edit cannot blame an edit either.)
        if c.read_only.is_some() || !self.report_counts(seq) {
            return;
        }
        let blames = self.rebuilds.last.is_some()
            && seq > 0
            && errors.keys().any(|f| !self.rebuilds.broken.contains_key(f));
        if !blames || self.rebuilds.recent.contains_key(&seq) {
            let info = self.rebuilds.recent.get(&seq).cloned();
            return self.apply_report(seq, errors, info);
        }
        // The blamed change is older than what memory holds: read it.
        let Some(c) = self.conns.get_mut(&conn) else {
            return;
        };
        c.busy = true;
        let (store, doc) = (self.state.store.clone(), self.doc.clone());
        let internal = self.internal.clone();
        tokio::spawn(async move {
            let result = super::change::caught(change_info(store.as_ref(), &doc, seq)).await;
            let lookup = ReportLookup {
                conn,
                seq,
                errors,
                result,
            };
            let _ = internal.send(Internal::Report(lookup));
        });
    }

    pub(super) fn on_report_lookup(&mut self, lookup: ReportLookup) {
        let ReportLookup {
            conn,
            seq,
            errors,
            result,
        } = lookup;
        self.unpark(conn);
        if self.closing.is_some() {
            return;
        }
        if self.live_conn(conn).is_none() {
            return; // (dropped: see `Room::live_conn`)
        }
        let info = result.unwrap_or_else(|e| {
            tracing::warn!(
                "could not read change {seq} of document {} to blame a rebuild on it: {e:#}",
                self.doc
            );
            None
        });
        // (Other reports may have been processed meanwhile: it is judged as of now.)
        if self.report_counts(seq) {
            self.apply_report(seq, errors, info);
        }
    }

    /// Processes a report that counts; `info` is the change at `seq`, if known.
    fn apply_report(
        &mut self,
        seq: Seq,
        errors: BTreeMap<String, String>,
        info: Option<ChangeInfo>,
    ) {
        let r = &mut self.rebuilds;
        let baseline = r.last;
        let fresh: BTreeMap<String, String> = errors
            .iter()
            .filter(|(f, _)| !r.broken.contains_key(*f))
            .map(|(f, e)| (f.clone(), e.clone()))
            .collect();
        let fixed: Vec<String> = r
            .broken
            .keys()
            .filter(|f| !errors.contains_key(*f))
            .cloned()
            .collect();
        let cause = match (baseline, &info) {
            (Some(baseline), Some(info)) if !fresh.is_empty() => Some(Broken {
                error: String::new(),
                by_seq: Some(seq),
                by_user: Some(info.user.clone()),
                by_name: Some(info.name.clone()),
                command: Some(info.command.clone()),
                since_seq: Some(baseline + 1),
            }),
            _ => None,
        };
        for (f, e) in errors {
            let fresh_one = fresh.contains_key(&f);
            let entry = r.broken.entry(f).or_insert_with(|| match &cause {
                Some(cause) if fresh_one => cause.clone(),
                _ => Broken {
                    error: String::new(),
                    by_seq: None,
                    by_user: None,
                    by_name: None,
                    command: None,
                    since_seq: None,
                },
            });
            entry.error = e;
        }
        for f in &fixed {
            r.broken.remove(f);
        }
        r.last = Some(seq);
        if !fresh.is_empty() || !fixed.is_empty() {
            let status = self.encode(&ServerMsg::RebuildStatus {
                seq,
                broken: self.rebuilds.broken.clone(),
            });
            self.broadcast(&status, &[]);
        }
        if cause.is_some() {
            let (store, doc) = (self.state.store.clone(), self.doc.clone());
            tokio::spawn(async move {
                if let Err(e) = record_broke(store, &doc, seq, fresh).await {
                    tracing::warn!(
                        "could not record what change {seq} of document {doc} broke: {e:#}"
                    );
                }
            });
        }
    }
}

/// Who made the change `seq` of `doc`, and its command, from the journal.
async fn change_info(store: &dyn Store, doc: &str, seq: Seq) -> anyhow::Result<Option<ChangeInfo>> {
    let Some(row) = store.change_by_seq(doc, i64::try_from(seq)?).await? else {
        return Ok(None);
    };
    let name = store
        .user_by_id(&row.author_id)
        .await?
        .map(|u| u.name)
        .unwrap_or_default();
    Ok(Some(ChangeInfo {
        user: row.author_id,
        name,
        command: row.command,
    }))
}

/// Merges `fresh` (feature id → error) into the `broke` column of the change `seq`, in a
/// transaction of its own that locks the row first. At most [`MAX_BROKE`] entries are kept.
async fn record_broke(
    store: Arc<dyn Store>,
    doc: &str,
    seq: Seq,
    fresh: BTreeMap<String, String>,
) -> anyhow::Result<()> {
    let seq = i64::try_from(seq)?;
    // (The server's own write, as the journal's are: never on a request's connection.)
    let mut tx = store.begin_journal().await?;
    let Some(current) = tx.change_broke(doc, seq).await? else {
        return Ok(()); // (no such change)
    };
    let mut broke = current
        .and_then(|j| serde_json::from_str::<serde_json::Value>(&j).ok())
        .and_then(|v| match v {
            serde_json::Value::Object(m) => Some(m),
            _ => None,
        })
        .unwrap_or_default();
    for (f, e) in fresh {
        if broke.len() >= MAX_BROKE && !broke.contains_key(&f) {
            continue;
        }
        broke.insert(f, serde_json::Value::String(e));
    }
    let json = serde_json::to_string(&broke)?;
    tx.set_change_broke(doc, seq, &json).await?;
    tx.commit().await
}

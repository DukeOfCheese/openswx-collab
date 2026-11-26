//! Document snapshots: the whole document and its conflict index at one point of its journal.
//!
//! A snapshot is a zstd-compressed MessagePack map
//!
//! ```text
//! {"v": 1, "seq": <u64>, "doc": <Value wire form>, "index": <Index wire form>}
//! ```
//!
//! stored as a whole value ([`crate::blobs::Blobs::put`]) under [`snapshot_key`], with a
//! `snapshots` row naming it. [`decode`] refuses anything else (another `v`, a missing or extra
//! field, a wrong type, bytes after the end, a document or index the core would not read back) with
//! a [`SnapshotError`], never a panic, and stops decompressing past [`MAX_DECOMPRESSED`].
//!
//! Encoding and decoding a large document takes a while: call [`encode`] and [`decode`] on the
//! blocking pool (`tokio::task::spawn_blocking`), as [`load_head_doc`] does.

use std::io::{self, BufRead, Read};
use std::sync::Arc;

use anyhow::Context;
use bytes::Bytes;
use oswx_core::{ClientId, Head, Index, OpError, Plain, Rules, Seq, Value, apply};

use crate::blobs::Blobs;
use crate::record::{RecordError, decode_ops, decode_paths, decode_reads};
use crate::store::{ChangeRow, SnapshotRow, Store};

/// The snapshot format this server writes and reads.
pub const FORMAT: u64 = 1;
/// The most a snapshot may decompress to; a bigger one is refused while it is being read.
pub const MAX_DECOMPRESSED: u64 = 512 << 20;
/// The zstd level snapshots are written with.
const LEVEL: i32 = 3;
/// How far the MessagePack reader may recurse: the core's bound on open containers
/// (`oswx_core::wire::MAX_NESTING`, which covers a document of `MAX_DEPTH` levels and its index),
/// counted the way rmpv counts (two levels per container and up to three for a leaf), as the core's
/// own frame reader does.
const PARSE_DEPTH: usize = 2 * oswx_core::wire::MAX_NESTING + 4;

/// Why a snapshot could not be read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SnapshotError {
    /// The bytes are not one zstd frame, or the frame is damaged.
    #[error("the snapshot is not valid zstd data")]
    Compression,
    /// It decompresses to more than [`MAX_DECOMPRESSED`].
    #[error("the snapshot decompresses to more than {MAX_DECOMPRESSED} bytes")]
    TooLarge,
    /// Bytes follow the zstd frame or the MessagePack value.
    #[error("the snapshot has bytes after its end")]
    TrailingBytes,
    /// The MessagePack is malformed, or is not the snapshot map.
    #[error("the snapshot is not a snapshot map")]
    Shape,
    /// A snapshot format this server does not know.
    #[error("the snapshot has format {0}, which this server does not read")]
    Version(u64),
    /// The document is not a document the core reads back.
    #[error("the snapshot's document is malformed")]
    Document,
    /// The index is not an index the core reads back.
    #[error("the snapshot's index is malformed")]
    Index,
}

/// The storage key of a document's snapshot at `seq`, written under lease `epoch`:
/// `docs/<doc>/snapshots/<epoch, 10 digits>-<seq, 10 digits>.mpk.zst`.
pub fn snapshot_key(doc: &str, epoch: u64, seq: Seq) -> String {
    format!("docs/{doc}/snapshots/{epoch:010}-{seq:010}.mpk.zst")
}

/// The snapshot of `doc` with `index` at `seq`.
pub fn encode(doc: &Value, index: &Index, seq: Seq) -> Bytes {
    let map = Plain::Map(vec![
        (Plain::from("v"), Plain::from(FORMAT)),
        (Plain::from("seq"), Plain::from(seq)),
        (Plain::from("doc"), doc.to_wire()),
        (Plain::from("index"), index.to_wire()),
    ]);
    let mut packed = Vec::new();
    rmpv::encode::write_value(&mut packed, &map).expect("writing to a Vec cannot fail");
    let compressed = zstd::stream::encode_all(packed.as_slice(), LEVEL)
        .expect("compressing a slice cannot fail");
    Bytes::from(compressed)
}

/// The document, index and seq a snapshot holds.
pub fn decode(bytes: &[u8]) -> Result<(Value, Index, Seq), SnapshotError> {
    let packed = decompress(bytes, MAX_DECOMPRESSED)?;
    let mut rest = packed.as_slice();
    let plain = rmpv::decode::read_value_with_max_depth(&mut rest, PARSE_DEPTH)
        .map_err(|_| SnapshotError::Shape)?;
    if !rest.is_empty() {
        return Err(SnapshotError::TrailingBytes);
    }
    let Plain::Map(pairs) = plain else {
        return Err(SnapshotError::Shape);
    };
    let mut fields: [Option<Plain>; 4] = [None, None, None, None];
    for (k, v) in pairs {
        let slot = match k.as_str() {
            Some("v") => 0,
            Some("seq") => 1,
            Some("doc") => 2,
            Some("index") => 3,
            _ => return Err(SnapshotError::Shape),
        };
        if fields[slot].replace(v).is_some() {
            return Err(SnapshotError::Shape);
        }
    }
    let [Some(v), Some(seq), Some(doc), Some(index)] = fields else {
        return Err(SnapshotError::Shape);
    };
    match v.as_u64() {
        Some(FORMAT) => {}
        Some(other) => return Err(SnapshotError::Version(other)),
        None => return Err(SnapshotError::Shape),
    }
    let seq = seq.as_u64().ok_or(SnapshotError::Shape)?;
    let doc = Value::from_wire(&doc).map_err(|_| SnapshotError::Document)?;
    if !matches!(doc, Value::Map(_)) {
        return Err(SnapshotError::Document);
    }
    let index = Index::from_wire(&index).ok_or(SnapshotError::Index)?;
    Ok((doc, index, seq))
}

/// One zstd frame, decompressed, refused once it passes `limit` bytes (reading stops there) and if
/// anything follows it.
fn decompress(bytes: &[u8], limit: u64) -> Result<Vec<u8>, SnapshotError> {
    let decoder = zstd::stream::read::Decoder::with_buffer(bytes)
        .map_err(|_| SnapshotError::Compression)?
        .single_frame();
    let mut out = Vec::new();
    let mut limited = decoder.take(limit + 1);
    limited
        .read_to_end(&mut out)
        .map_err(|_| SnapshotError::Compression)?;
    if out.len() as u64 > limit {
        return Err(SnapshotError::TooLarge);
    }
    let mut rest = limited.into_inner().finish();
    if !rest.fill_buf().map_err(io_error)?.is_empty() {
        return Err(SnapshotError::TrailingBytes);
    }
    Ok(out)
}

fn io_error(_: io::Error) -> SnapshotError {
    SnapshotError::Compression
}

/// The document as it is now ([`load_head`]): the document, its index and the seq it is at.
pub async fn load_head_doc(
    store: &dyn Store,
    blobs: &dyn Blobs,
    doc_id: &str,
) -> anyhow::Result<(Value, Index, Seq)> {
    let (head, _) = load_head(store, blobs, doc_id).await?;
    Ok((head.doc, head.index, head.seq))
}

/// The client id under which the conflict index records a change the server made (a journal row
/// with no `client_id`, such as a restore). A real client is `<user id>.<client id>`, which never
/// starts with `~`.
///
/// The room must decide a change it makes itself (`oswx_core::decide_server`) under this client id, and
/// journal it with `client_id: None`, so that a reload rebuilds the same index the room had.
pub use oswx_core::SERVER_CLIENT;

/// How many journal rows loading reads at a time.
const REPLAY_PAGE: i64 = 500;

/// Why a document could not be loaded. Each is an error, never a panic, and never a silently
/// shorter document.
#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error("there is no such document")]
    NoDocument,
    /// `state_at` was asked for a seq below 0 or above the head.
    #[error("the document has no state {0}")]
    NoState(i64),
    #[error("the document has no snapshot at or before seq {0}")]
    NoSnapshot(i64),
    #[error("the snapshot {0} is missing from storage")]
    SnapshotMissing(String),
    #[error("the snapshot {key} cannot be read: {error}")]
    Snapshot { key: String, error: SnapshotError },
    #[error("the snapshot {key} holds seq {found}, its row says {row}")]
    SnapshotSeq { key: String, found: Seq, row: i64 },
    /// The journal has no row with this seq, though the document's head is at or above it.
    #[error("the journal has no change {0}")]
    Gap(Seq),
    #[error("the journal's change {seq} is damaged: {error}")]
    Damaged { seq: Seq, error: RecordError },
    #[error("the journal's change {seq} does not apply: {error}")]
    DoesNotApply { seq: Seq, error: OpError },
    #[error(transparent)]
    Store(#[from] anyhow::Error),
}

/// A document's head, as the room had it in memory when it wrote its last change, and the epoch of
/// the snapshot it started from.
///
/// It reads the snapshot with the greatest seq at or below `documents.head_seq` (ties: the greatest
/// epoch), decodes it on the blocking pool, then replays every journal row after it in order: the
/// stored operations are applied with [`oswx_core::apply()`] (`Rules::default()`, no id prefix:
/// these changes were judged when they were accepted) and the stored writes, removals and reads are
/// recorded into the index under the stored `client_id` ([`SERVER_CLIENT`] for none). A document
/// created and never acquired has a snapshot at epoch 0.
///
/// The epoch returned is the snapshot's, which says where loading started; it is **not** an epoch
/// to write under. A room calls [`crate::lease::acquire`] before it loads and puts the epoch that
/// gives in every record (and passes it to [`write_snapshot`]).
pub async fn load_head(
    store: &dyn Store,
    blobs: &dyn Blobs,
    doc: &str,
) -> Result<(Head, u64), LoadError> {
    let document = store
        .document_by_id(doc)
        .await?
        .ok_or(LoadError::NoDocument)?;
    let (head, epoch) = replay_to(store, blobs, doc, document.head_seq, true).await?;
    let epoch = u64::try_from(epoch).map_err(anyhow::Error::from)?;
    Ok((head, epoch))
}

/// The document as it was at `seq`: the nearest snapshot at or before it, and the journal replayed
/// up to it. A seq below 0 or above the head is [`LoadError::NoState`].
pub async fn state_at(
    store: &dyn Store,
    blobs: &dyn Blobs,
    doc: &str,
    seq: i64,
) -> Result<Value, LoadError> {
    let document = store
        .document_by_id(doc)
        .await?
        .ok_or(LoadError::NoDocument)?;
    if seq < 0 || seq > document.head_seq {
        return Err(LoadError::NoState(seq));
    }
    Ok(replay_to(store, blobs, doc, seq, false).await?.0.doc)
}

/// The nearest snapshot at or before `target`, with the journal replayed up to `target` (the index
/// too when `with_index`); and the snapshot's epoch.
async fn replay_to(
    store: &dyn Store,
    blobs: &dyn Blobs,
    doc: &str,
    target: i64,
    with_index: bool,
) -> Result<(Head, i64), LoadError> {
    let row = store
        .snapshot_at_or_before(doc, target)
        .await?
        .ok_or(LoadError::NoSnapshot(target))?;
    let bytes = blobs
        .get(&row.key)
        .await?
        .ok_or_else(|| LoadError::SnapshotMissing(row.key.clone()))?;
    let decoded = tokio::task::spawn_blocking(move || decode(&bytes))
        .await
        .context("decoding a snapshot")?;
    let (doc_value, index, seq) = decoded.map_err(|error| LoadError::Snapshot {
        key: row.key.clone(),
        error,
    })?;
    if i64::try_from(seq).ok() != Some(row.seq) {
        return Err(LoadError::SnapshotSeq {
            key: row.key,
            found: seq,
            row: row.seq,
        });
    }
    let mut head = Head {
        seq,
        doc: doc_value,
        index,
    };
    let mut at = row.seq;
    while at < target {
        let rows = store.changes_between(doc, at, target, REPLAY_PAGE).await?;
        if rows.is_empty() {
            return Err(LoadError::Gap(head.seq + 1));
        }
        head = tokio::task::spawn_blocking(move || replay(head, rows, with_index))
            .await
            .context("replaying the journal")??;
        at = i64::try_from(head.seq).map_err(anyhow::Error::from)?;
    }
    Ok((head, row.epoch))
}

/// `head` with `rows` applied, in order; each must be the next seq.
fn replay(mut head: Head, rows: Vec<ChangeRow>, with_index: bool) -> Result<Head, LoadError> {
    for row in rows {
        let seq = head.seq + 1;
        if i64::try_from(seq).ok() != Some(row.seq) {
            return Err(LoadError::Gap(seq));
        }
        let damaged = |error| LoadError::Damaged { seq, error };
        let ops = decode_ops(&row.ops).map_err(damaged)?;
        let doc = apply(&head.doc, &ops, &Rules::default(), None)
            .map_err(|error| LoadError::DoesNotApply { seq, error })?
            .doc;
        let index = if with_index {
            let (writes, removed) = decode_paths(&row.paths).map_err(damaged)?;
            let reads = decode_reads(&row.reads).map_err(damaged)?;
            let client: ClientId = Arc::from(row.client_id.as_deref().unwrap_or(SERVER_CLIENT));
            head.index.record(seq, &client, &writes, &removed, &reads)
        } else {
            head.index
        };
        head = Head { seq, doc, index };
    }
    Ok(head)
}

/// Why [`write_snapshot`] did not record a snapshot.
#[derive(Debug, thiserror::Error)]
pub enum WriteSnapshotError {
    #[error("there is no such document")]
    NoDocument,
    /// The head is ahead of the journal (a tentative version, not yet durable): a snapshot of it
    /// could hold changes that are never written.
    #[error("the snapshot's seq {seq} is ahead of the document's head seq {head_seq}")]
    AheadOfJournal { seq: Seq, head_seq: i64 },
    #[error(transparent)]
    Store(#[from] anyhow::Error),
}

/// Writes a snapshot of `head`, a version the room has published, under lease `epoch`: encoded on
/// the blocking pool, stored under [`snapshot_key`], then its `snapshots` row inserted. Writing one
/// that already has its row does nothing. Older snapshots are never deleted.
///
/// A head whose seq is above `documents.head_seq` is refused ([`WriteSnapshotError::AheadOfJournal`],
/// checked again inside the transaction that inserts the row): only durable versions are
/// snapshotted.
pub async fn write_snapshot(
    store: &dyn Store,
    blobs: &dyn Blobs,
    doc: &str,
    epoch: u64,
    head: &Head,
    now: f64,
) -> Result<(), WriteSnapshotError> {
    let epoch_i = i64::try_from(epoch).map_err(anyhow::Error::from)?;
    let seq_i = i64::try_from(head.seq).map_err(anyhow::Error::from)?;
    let ahead = |head_seq: i64| WriteSnapshotError::AheadOfJournal {
        seq: head.seq,
        head_seq,
    };
    // Before any work: nothing is stored for a head the journal has not reached.
    let document = store
        .document_by_id(doc)
        .await?
        .ok_or(WriteSnapshotError::NoDocument)?;
    if seq_i > document.head_seq {
        return Err(ahead(document.head_seq));
    }
    if store.snapshot_row(doc, epoch_i, seq_i).await?.is_some() {
        return Ok(());
    }
    let key = snapshot_key(doc, epoch, head.seq);
    let (value, index, seq) = (head.doc.clone(), head.index.clone(), head.seq);
    let bytes = tokio::task::spawn_blocking(move || encode(&value, &index, seq))
        .await
        .map_err(anyhow::Error::from)?;
    let row = SnapshotRow {
        document_id: doc.to_owned(),
        epoch: epoch_i,
        seq: seq_i,
        key: key.clone(),
        size: i64::try_from(bytes.len()).map_err(anyhow::Error::from)?,
        created_at: now,
    };
    blobs.put(&key, bytes).await?;
    let mut tx = store.begin_journal().await?;
    let head_seq = tx
        .document_by_id(doc)
        .await?
        .ok_or(WriteSnapshotError::NoDocument)?
        .head_seq;
    if seq_i > head_seq {
        return Err(ahead(head_seq)); // dropping `tx` rolls it back
    }
    if tx.snapshot_row(doc, epoch_i, seq_i).await?.is_none() {
        tx.insert_snapshot(&row).await?;
    }
    tx.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use oswx_core::Write;
    use serde_json::json;

    use super::*;
    use crate::blobs::check_key;
    use crate::content::plain_from_json;

    fn sample() -> (Value, Index) {
        let doc = Value::from_plain(&plain_from_json(&json!({
            "version": 3,
            "features": [
                {"id": "f1", "name": "Sketch1", "params": {"sketch": {"entities": [
                    {"id": "e1", "kind": "line"}, {"id": "e2", "kind": "arc"}]}}},
                {"id": "f2", "name": "Boss-Extrude1", "params": {"depth": 10.5}},
            ],
            "plain": [1, 2, [3, {"x": null}]],
        })))
        .unwrap();
        let path = |segs: &[&str]| {
            segs.iter()
                .map(|s| Arc::<str>::from(*s))
                .collect::<Vec<_>>()
        };
        let writes: BTreeSet<Write> = [
            Write::At(path(&["features", "f2", "params", "depth"])),
            Write::Position(path(&["features"]), Arc::from("f1")),
        ]
        .into_iter()
        .collect();
        let removed: BTreeSet<Vec<Arc<str>>> = [path(&["features", "f3"])].into_iter().collect();
        let reads: BTreeSet<Vec<Arc<str>>> = [path(&["features", "f1"])].into_iter().collect();
        let index = Index::default()
            .record(
                4,
                &Arc::from("u.c"),
                &writes,
                &BTreeSet::new(),
                &BTreeSet::new(),
            )
            .record(7, &Arc::from("v.d"), &BTreeSet::new(), &removed, &reads);
        (doc, index)
    }

    #[test]
    fn a_snapshot_round_trips() {
        let (doc, index) = sample();
        let bytes = encode(&doc, &index, 7);
        let (doc2, index2, seq) = decode(&bytes).unwrap();
        assert_eq!(doc2, doc);
        assert_eq!(index2.to_wire(), index.to_wire());
        assert_ne!(
            index.to_wire(),
            Index::default().to_wire(),
            "the index is not empty"
        );
        assert_eq!(seq, 7);
    }

    fn refused(bytes: &[u8]) -> Option<SnapshotError> {
        decode(bytes).err()
    }

    #[test]
    fn the_deepest_document_and_index_the_core_allows_round_trip() {
        // A root map, 31 keyed lists of maps, and a map at the bottom: 64 levels.
        let mut item = json!({"id": "i", "leaf": {"x": 1}});
        for _ in 1..31 {
            item = json!({"id": "i", "c": [item]});
        }
        let doc = Value::from_plain(&plain_from_json(&json!({ "c": [item] }))).unwrap();
        let deep: Vec<Arc<str>> = (0..64).map(|i| Arc::from(format!("s{i}"))).collect();
        let writes: BTreeSet<Write> = [Write::At(deep)].into_iter().collect();
        let index = Index::default().record(
            1,
            &Arc::from("u.c"),
            &writes,
            &BTreeSet::new(),
            &BTreeSet::new(),
        );
        let (doc2, index2, _) = decode(&encode(&doc, &index, 1)).unwrap();
        assert_eq!(doc2, doc);
        assert_eq!(index2.to_wire(), index.to_wire());
    }

    #[test]
    fn nesting_beyond_the_cores_bound_is_refused_by_the_reader() {
        let mut deep = Plain::Nil;
        for _ in 0..400 {
            deep = Plain::Array(vec![deep]);
        }
        let mut f = good_fields();
        f[2].1 = deep;
        assert_eq!(refused(&pack(f)), Some(SnapshotError::Shape));
    }

    fn pack(map: Vec<(&str, Plain)>) -> Vec<u8> {
        let map = Plain::Map(map.into_iter().map(|(k, v)| (Plain::from(k), v)).collect());
        let mut out = Vec::new();
        rmpv::encode::write_value(&mut out, &map).unwrap();
        zstd::stream::encode_all(out.as_slice(), 3).unwrap()
    }

    fn good_fields() -> Vec<(&'static str, Plain)> {
        let (doc, index) = sample();
        vec![
            ("v", Plain::from(1)),
            ("seq", Plain::from(3)),
            ("doc", doc.to_wire()),
            ("index", index.to_wire()),
        ]
    }

    #[test]
    fn a_damaged_snapshot_is_an_error() {
        let (doc, index) = sample();
        let bytes = encode(&doc, &index, 7).to_vec();
        for i in 0..bytes.len() {
            let mut flipped = bytes.clone();
            flipped[i] ^= 0x5a;
            // Any answer but a panic; most flips are refused.
            let _ = decode(&flipped);
        }
        let mut flipped = bytes.clone();
        flipped[0] ^= 0xff;
        assert_eq!(refused(&flipped), Some(SnapshotError::Compression));
        for cut in [0, 1, bytes.len() / 2, bytes.len() - 1] {
            assert!(decode(&bytes[..cut]).is_err(), "cut at {cut}");
        }
        let mut longer = bytes.clone();
        longer.push(0);
        assert_eq!(refused(&longer), Some(SnapshotError::TrailingBytes));
        let mut two = bytes.clone();
        two.extend_from_slice(&bytes);
        assert_eq!(refused(&two), Some(SnapshotError::TrailingBytes));
        assert_eq!(refused(b""), Some(SnapshotError::Compression));
    }

    #[test]
    fn anything_but_the_snapshot_map_is_refused() {
        assert!(decode(&pack(good_fields())).is_ok());
        let with = |key: &str, v: Plain| {
            let mut f = good_fields();
            f.iter_mut().find(|(k, _)| *k == key).unwrap().1 = v;
            refused(&pack(f))
        };
        assert_eq!(with("v", Plain::from(2)), Some(SnapshotError::Version(2)));
        assert_eq!(with("v", Plain::from("1")), Some(SnapshotError::Shape));
        assert_eq!(with("seq", Plain::from(-1)), Some(SnapshotError::Shape));
        assert_eq!(with("seq", Plain::from("3")), Some(SnapshotError::Shape));
        assert_eq!(with("doc", Plain::from(1)), Some(SnapshotError::Document));
        assert_eq!(
            with(
                "doc",
                Plain::Map(vec![(Plain::from("$list"), Plain::from(1))])
            ),
            Some(SnapshotError::Document)
        );
        assert_eq!(with("index", Plain::from(1)), Some(SnapshotError::Index));
        let mut missing = good_fields();
        missing.pop();
        assert_eq!(refused(&pack(missing)), Some(SnapshotError::Shape));
        let mut extra = good_fields();
        extra.push(("more", Plain::Nil));
        assert_eq!(refused(&pack(extra)), Some(SnapshotError::Shape));
        let mut repeated = good_fields();
        repeated.push(("v", Plain::from(1)));
        assert_eq!(refused(&pack(repeated)), Some(SnapshotError::Shape));
        let not_a_map = zstd::stream::encode_all(&[0x01u8][..], 3).unwrap();
        assert_eq!(refused(&not_a_map), Some(SnapshotError::Shape));
        let mut trailing = Vec::new();
        rmpv::encode::write_value(&mut trailing, &Plain::Map(vec![])).unwrap();
        trailing.push(0xc0);
        let trailing = zstd::stream::encode_all(trailing.as_slice(), 3).unwrap();
        assert_eq!(refused(&trailing), Some(SnapshotError::TrailingBytes));
    }

    #[test]
    fn a_snapshot_that_decompresses_too_far_is_refused_while_reading() {
        // 64 MiB of zeros compress to a few kilobytes; reading stops one byte past the limit.
        let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 1).unwrap();
        let chunk = vec![0u8; 1 << 20];
        for _ in 0..64 {
            io::Write::write_all(&mut encoder, &chunk).unwrap();
        }
        let bomb = encoder.finish().unwrap();
        assert!(bomb.len() < 1 << 20);
        assert_eq!(
            decompress(&bomb, 1 << 20).err(),
            Some(SnapshotError::TooLarge)
        );
        assert_eq!(decompress(&bomb, 64 << 20).map(|v| v.len()), Ok(64 << 20));
    }

    #[test]
    fn snapshot_keys_sort_by_epoch_then_seq_and_are_valid_storage_keys() {
        let doc = "0123456789abcdef0123456789abcdef";
        let key = snapshot_key(doc, 0, 0);
        assert_eq!(
            key,
            format!("docs/{doc}/snapshots/0000000000-0000000000.mpk.zst")
        );
        check_key(&key).unwrap();
        check_key(&snapshot_key(doc, u64::from(u32::MAX), u64::MAX)).unwrap();
        assert!(snapshot_key(doc, 1, 2) < snapshot_key(doc, 1, 10));
    }
}

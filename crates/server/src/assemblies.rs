//! Assemblies: documents whose components use parts by reference.
//!
//! A cloud assembly carries no copies of its parts. Each component says which part document it
//! uses and at which state of that part's history:
//!
//! ```json
//! {"id": "c1", "x": [0, 0, 0, 0, 0, 0], "ref": {"document": "<part id>", "seq": 42, "version": "Rev A"}}
//! ```
//!
//! `seq` is what counts (a state of a document never changes); `version` is the label the person
//! picked, kept for display and never checked against the part's versions (as the Python server:
//! a version is a label on a seq, and deleting or renaming one must not break assemblies). So
//! editing a part never changes an assembly; its users learn that something newer exists (`GET
//! .../references`, and `reference.update` when the part gets a named version) and move a
//! component to it by changing its `ref` like any other edit.
//!
//! # What is checked
//!
//! Choosing a part, or another state of one, needs that part: when a change gives a component a
//! `(document, seq)` it did not have (a new component, or another pin), the part must exist, not be
//! deleted, be a `part`, be in the assembly's organisation, be one the acting user can open (any
//! role in its project), and have that state. Every failure says the same
//! ([`no_part`]), so that a part one cannot open is told apart from nothing. Moving, renaming or
//! removing a component, or relabelling it (same document and seq), needs nothing of the part; nor
//! does a revert or restore that brings back a component as it was (it may even point at a part
//! deleted since), nor an undo that brings it back exactly as the undone change's kept inverse
//! holds it (a client sends its undo's operations itself). Such a change that re-pins a component
//! that stays is checked like any other.
//!
//! The checks read the store, so the room runs them off its task ([`check_choices`]); the parts
//! chosen travel with the journal record ([`crate::record::Extra::AssemblyRefs`]), which locks each
//! of them and refuses the record if one was deleted meanwhile.
//!
//! The `assembly_refs` table keeps, per assembly, `(component, part, seq)` for every component. It
//! is rewritten (whole) with the record of a change that alters that set, and only then.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use oswx_core::{Op, Seq, Value, Write};

use crate::access::parse_role;
use crate::record::{AssemblyRef, ChosenPart};
use crate::security::OrgRole;
use crate::store::{Id, PartRow, Store};

/// The most distinct parts one change (or one new assembly) may newly choose.
pub const MAX_CHOSEN_PARTS: usize = 1024;
/// The longest `ref.version` label.
pub const MAX_VERSION_LABEL: usize = 200;
/// The longest `ref.document` (a document id).
const MAX_PART_ID: usize = 32;
/// What a part (not an assembly) is told when a change touches `components` or `mates`.
pub const ONLY_ASSEMBLIES: &str = "only assemblies have components and mates";
/// The lists only an assembly has.
const ASSEMBLY_LISTS: [&str; 2] = ["components", "mates"];

/// What every failed reference check says: the same for a part that does not exist, one in
/// another organisation, one the user cannot open, one deleted, a document that is not a part,
/// and a state the part does not have.
pub fn no_part(component: &str) -> String {
    format!("component {component}: there is no part you can use at that state")
}

fn needs_ref(component: &str) -> String {
    format!("component {component} needs ref: {{document: <part document id>, seq: <number>}}")
}

/// What a component points at: a part and a state of it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Pin {
    pub part: Arc<str>,
    pub seq: i64,
}

/// The pin of the component `id`, or why it has none (a message for the client, about the
/// component's shape only).
pub fn pin_of(id: &str, component: &Value) -> Result<Pin, String> {
    let Value::Map(c) = component else {
        return Err(needs_ref(id));
    };
    if c.contains_key("part") {
        return Err(format!(
            "component {id}: a cloud assembly refers to its parts ('ref'); it does not carry copies"
        ));
    }
    let Some(Value::Map(r)) = c.get("ref") else {
        return Err(needs_ref(id));
    };
    let (Some(Value::Str(part)), Some(Value::Int(seq))) = (r.get("document"), r.get("seq")) else {
        return Err(needs_ref(id));
    };
    if part.is_empty() || *seq < 0 {
        return Err(needs_ref(id));
    }
    match r.get("version") {
        None | Some(Value::Null) => {}
        Some(Value::Str(v)) if v.chars().count() <= MAX_VERSION_LABEL => {}
        Some(_) => {
            return Err(format!(
                "component {id}: ref.version is a version name of at most {MAX_VERSION_LABEL} \
                 characters"
            ));
        }
    }
    if part.chars().count() > MAX_PART_ID {
        // (No document has such an id.)
        return Err(no_part(id));
    }
    Ok(Pin {
        part: part.clone(),
        seq: *seq,
    })
}

/// The components of a document, by id (none if it has no keyed `components` list).
fn components(doc: &Value) -> Vec<(&Arc<str>, &Value)> {
    match doc.at(&["components"]) {
        Some(Value::Keyed(k)) => k.iter().map(|(id, _, v)| (id, v)).collect(),
        _ => Vec::new(),
    }
}

/// Every component's pin, by component id; a component without a valid one (which the checks
/// never let in) is left out.
pub fn pins(doc: &Value) -> BTreeMap<Arc<str>, Pin> {
    components(doc)
        .into_iter()
        .filter_map(|(id, c)| pin_of(id, c).ok().map(|p| (id.clone(), p)))
        .collect()
}

/// Every component's pin, by component id, for a new assembly (whose every component is chosen):
/// `Err` for a component without a valid one, or more than [`MAX_CHOSEN_PARTS`] distinct parts.
pub fn chosen_in(doc: &Value) -> Result<Vec<(Arc<str>, Pin)>, String> {
    let mut out = Vec::new();
    let mut parts = HashSet::new();
    for (id, c) in components(doc) {
        let pin = pin_of(id, c)?;
        parts.insert(pin.part.clone());
        if parts.len() > MAX_CHOSEN_PARTS {
            return Err(format!(
                "an assembly may use parts from at most {MAX_CHOSEN_PARTS} documents"
            ));
        }
        out.push((id.clone(), pin));
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

/// The `assembly_refs` rows of a whole assembly, by component id.
pub fn rows(doc: &Value) -> Vec<AssemblyRef> {
    pins(doc)
        .into_iter()
        .map(|(id, p)| AssemblyRef {
            component_id: id.to_string(),
            part_id: p.part.to_string(),
            seq: Seq::try_from(p.seq).unwrap_or_default(),
        })
        .collect()
}

/// Whether a change writes anything in `components` or `mates` (a part must not).
pub fn touches_assembly_lists(writes: &BTreeSet<Write>) -> bool {
    writes.iter().any(|w| {
        let first = match w {
            Write::At(p) | Write::Position(p, _) => p.first(),
        };
        first.is_some_and(|s| ASSEMBLY_LISTS.contains(&&**s))
    })
}

/// What a change does to an assembly's references.
#[derive(Debug, Clone, Default)]
pub struct RefChange {
    /// The components given a pin they did not have, which must be checked
    /// ([`check_choices`]), by component id.
    pub chosen: Vec<(Arc<str>, Pin)>,
    /// The assembly's rows after the change, when it alters them (`None`: unchanged).
    pub rows: Option<Vec<AssemblyRef>>,
}

impl RefChange {
    /// Whether there is anything to check or write.
    pub fn is_empty(&self) -> bool {
        self.chosen.is_empty() && self.rows.is_none()
    }

    /// The distinct parts chosen, each with the first component choosing it.
    pub fn chosen_parts(&self) -> Vec<ChosenPart> {
        chosen_parts(&self.chosen)
    }
}

/// The distinct parts of `chosen`, each with the first component choosing it.
pub fn chosen_parts(chosen: &[(Arc<str>, Pin)]) -> Vec<ChosenPart> {
    let mut seen = HashSet::new();
    chosen
        .iter()
        .filter(|(_, p)| seen.insert(p.part.clone()))
        .map(|(id, p)| ChosenPart {
            part_id: p.part.to_string(),
            component_id: id.to_string(),
        })
        .collect()
}

/// The pins of the components an operation list inserts into `components`, by component id: what
/// the kept inverse of an undone change would put back.
pub fn inserted_pins(ops: &[Op]) -> HashMap<Arc<str>, Pin> {
    ops.iter()
        .filter_map(|op| match op {
            Op::Insert { list, item, .. } if list.len() == 1 && &*list[0] == "components" => {
                let Value::Map(m) = item else {
                    return None;
                };
                let Some(Value::Str(id)) = m.get("id") else {
                    return None;
                };
                pin_of(id, item).ok().map(|pin| (id.clone(), pin))
            }
            _ => None,
        })
        .collect()
}

/// What a change that turned `before` into `after`, writing `writes`, does to the assembly's
/// references. `restored(id, pin)`: a component the change adds with this pin comes back as it was
/// (an undo, a revert, a restore putting back what was there) and needs no check. Looks only at
/// the components the change writes (all of them only when it replaces the whole list). `Err`: a
/// component it writes has no valid pin, or the change chooses more than [`MAX_CHOSEN_PARTS`]
/// parts.
pub fn ref_change(
    before: &Value,
    after: &Value,
    writes: &BTreeSet<Write>,
    restored: impl Fn(&str, &Pin) -> bool,
) -> Result<RefChange, String> {
    let mut whole = false;
    let mut touched: BTreeSet<Arc<str>> = BTreeSet::new();
    for w in writes {
        if let Write::At(p) = w
            && p.first().is_some_and(|s| &**s == "components")
        {
            match p.get(1) {
                Some(id) => {
                    touched.insert(id.clone());
                }
                None => whole = true,
            }
        }
    }
    if whole {
        let ok = match after.at(&["components"]) {
            None | Some(Value::Keyed(_)) => true,
            Some(Value::List(items)) => items.is_empty(),
            Some(_) => false,
        };
        if !ok {
            return Err("components must be a list of objects with distinct ids".into());
        }
        touched.extend(components(before).into_iter().map(|(id, _)| id.clone()));
        touched.extend(components(after).into_iter().map(|(id, _)| id.clone()));
    }
    let mut out = RefChange::default();
    let mut altered = false;
    let mut parts = HashSet::new();
    for id in touched {
        let old = before
            .at(&["components", &id])
            .and_then(|c| pin_of(&id, c).ok());
        let new = match after.at(&["components", &id]) {
            Some(c) => Some(pin_of(&id, c)?),
            None => None,
        };
        if old == new {
            continue;
        }
        altered = true;
        if let Some(pin) = new
            && (old.is_some() || !restored(&id, &pin))
        {
            parts.insert(pin.part.clone());
            if parts.len() > MAX_CHOSEN_PARTS {
                return Err(format!(
                    "a change may choose parts from at most {MAX_CHOSEN_PARTS} documents"
                ));
            }
            out.chosen.push((id, pin));
        }
    }
    if altered {
        out.rows = Some(rows(after));
    }
    Ok(out)
}

/// Which projects of one organisation a user can open.
#[derive(Debug, Clone)]
pub struct Access {
    org: Id,
    /// An organisation owner or admin: every project.
    all: bool,
    projects: HashSet<Id>,
}

impl Access {
    /// What `user` can open in `org` (two reads at most).
    pub async fn of(store: &dyn Store, user: &str, org: &str) -> anyhow::Result<Access> {
        let mut access = Access {
            org: org.to_owned(),
            all: false,
            projects: HashSet::new(),
        };
        if let Some(m) = store.org_member(org, user).await? {
            if parse_role::<OrgRole>(&m.role)? >= OrgRole::Admin {
                access.all = true;
            } else {
                access.projects = store
                    .member_projects(org, user)
                    .await?
                    .into_iter()
                    .map(|p| p.id)
                    .collect();
            }
        }
        Ok(access)
    }

    /// Whether the user can open the project `project` of the organisation `org`.
    pub fn opens(&self, org: &str, project: &str) -> bool {
        org == self.org && (self.all || self.projects.contains(project))
    }
}

/// Checks the parts an assembly of the organisation `org` newly chooses for `user` (see the module
/// documentation). Answers the first failure's message ([`no_part`]) for the first component, in
/// the order given, whose choice fails. Reads the parts in batches, then the user's roles. The
/// outer `Err` is a store failure.
pub async fn check_choices(
    store: &dyn Store,
    org: &str,
    user: &str,
    chosen: &[(Arc<str>, Pin)],
) -> anyhow::Result<Result<(), String>> {
    let ids: Vec<Id> = chosen
        .iter()
        .map(|(_, p)| p.part.to_string())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    if ids.is_empty() {
        return Ok(Ok(()));
    }
    let found: HashMap<Id, PartRow> = store
        .parts_by_ids(&ids)
        .await?
        .into_iter()
        .map(|p| (p.id.clone(), p))
        .collect();
    let access = Access::of(store, user, org).await?;
    for (id, pin) in chosen {
        let usable = found.get(&*pin.part).is_some_and(|p| {
            p.deleted_at.is_none()
                && p.kind == "part"
                && access.opens(&p.org_id, &p.project_id)
                && pin.seq <= p.head_seq
        });
        if !usable {
            return Ok(Err(no_part(id)));
        }
    }
    Ok(Ok(()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::content::plain_from_json;
    use serde_json::json;

    fn value(j: serde_json::Value) -> Value {
        Value::from_plain(&plain_from_json(&j)).unwrap()
    }

    #[test]
    fn a_pin_needs_a_document_and_a_state() {
        let ok = value(json!({"id": "c1", "ref": {"document": "p", "seq": 3, "version": "A"}}));
        assert_eq!(
            pin_of("c1", &ok),
            Ok(Pin {
                part: Arc::from("p"),
                seq: 3
            })
        );
        for bad in [
            json!({"id": "c1"}),
            json!({"id": "c1", "ref": "p"}),
            json!({"id": "c1", "ref": {"document": 5, "seq": 1}}),
            json!({"id": "c1", "ref": {"document": "", "seq": 1}}),
            json!({"id": "c1", "ref": {"document": "p", "seq": -1}}),
            json!({"id": "c1", "ref": {"document": "p", "seq": 1.0}}),
            json!({"id": "c1", "ref": {"document": "p", "seq": true}}),
            json!({"id": "c1", "ref": {"document": "p", "seq": "1"}}),
        ] {
            assert_eq!(
                pin_of("c1", &value(bad.clone())),
                Err(needs_ref("c1")),
                "{bad}"
            );
        }
        let copy = value(json!({"id": "c1", "part": {}, "ref": {"document": "p", "seq": 1}}));
        assert!(
            pin_of("c1", &copy)
                .unwrap_err()
                .contains("does not carry copies")
        );
        let label = value(json!({"id": "c1", "ref": {"document": "p", "seq": 1, "version": 3}}));
        assert!(pin_of("c1", &label).unwrap_err().contains("version name"));
        let long = value(json!({"id": "c1", "ref": {"document": "a".repeat(33), "seq": 1}}));
        assert_eq!(pin_of("c1", &long), Err(no_part("c1")));
    }

    #[test]
    fn only_written_components_are_compared() {
        let before = value(json!({"components": [
            {"id": "c1", "ref": {"document": "p", "seq": 0}},
            {"id": "c2", "ref": {"document": "p", "seq": 0}}]}));
        let after = value(json!({"components": [
            {"id": "c1", "ref": {"document": "p", "seq": 0, "version": "x"}, "x": 1},
            {"id": "c2", "ref": {"document": "q", "seq": 2}},
            {"id": "c3", "ref": {"document": "r", "seq": 0}}]}));
        let w = |p: &[&str]| Write::At(p.iter().map(|s| Arc::from(*s)).collect());
        let writes: BTreeSet<Write> = [
            w(&["components", "c1", "x"]),
            w(&["components", "c2", "ref"]),
            w(&["components", "c3"]),
        ]
        .into();
        let rc = ref_change(&before, &after, &writes, |_, _| false).unwrap();
        let chosen: Vec<&str> = rc.chosen.iter().map(|(id, _)| &**id).collect();
        assert_eq!(chosen, ["c2", "c3"]);
        assert_eq!(rc.rows.as_ref().unwrap().len(), 3);
        // Brought back by history: the new component needs no check; the re-pin does.
        let rc = ref_change(&before, &after, &writes, |_, _| true).unwrap();
        let chosen: Vec<&str> = rc.chosen.iter().map(|(id, _)| &**id).collect();
        assert_eq!(chosen, ["c2"]);
        // A relabel alone changes nothing.
        let only = [w(&["components", "c1", "x"])].into();
        assert!(
            ref_change(&before, &after, &only, |_, _| false)
                .unwrap()
                .is_empty()
        );
    }
}

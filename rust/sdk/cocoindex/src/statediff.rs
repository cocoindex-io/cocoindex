//! Small diff helpers for managed connector targets.
//!
//! These helpers model resources that can be owned either by CocoIndex or by
//! the user, and decide whether reconciliation should insert, update, replace,
//! delete, or skip a target.
//!
//! The [`diff_composite`] helper extends [`diff`] to two-level state: a single
//! `main` record plus a set of keyed `sub` records (e.g. a SQL table's primary
//! key / virtual-table signature as `main`, and its non-PK columns as `sub`),
//! enabling column-level / attachment-level diffs.

use std::collections::HashMap;
use std::hash::Hash;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ManagedBy {
    #[default]
    System,
    User,
}

impl ManagedBy {
    pub fn is_system(self) -> bool {
        matches!(self, Self::System)
    }

    pub fn is_user(self) -> bool {
        matches!(self, Self::User)
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManagedTargetOptions {
    pub managed_by: ManagedBy,
}

impl ManagedTargetOptions {
    pub fn system_managed() -> Self {
        Self {
            managed_by: ManagedBy::System,
        }
    }

    pub fn user_managed() -> Self {
        Self {
            managed_by: ManagedBy::User,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MutualTrackingRecord<T> {
    pub tracking_record: T,
    pub managed_by: ManagedBy,
}

impl<T> MutualTrackingRecord<T> {
    pub fn new(tracking_record: T, managed_by: ManagedBy) -> Self {
        Self {
            tracking_record,
            managed_by,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrackingRecordTransition<T> {
    pub desired: Option<T>,
    pub prev: Vec<T>,
    pub prev_may_be_missing: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DiffAction {
    Insert,
    Upsert,
    Replace,
    Delete,
}

pub fn resolve_system_transition<T: Clone>(
    desired: Option<MutualTrackingRecord<T>>,
    prev: Vec<MutualTrackingRecord<T>>,
    prev_may_be_missing: bool,
) -> Option<TrackingRecordTransition<T>> {
    match desired {
        Some(desired) if desired.managed_by.is_user() => None,
        None if prev.is_empty() || prev.iter().any(|p| p.managed_by.is_user()) => None,
        None => Some(TrackingRecordTransition {
            desired: None,
            prev: prev
                .into_iter()
                .filter(|p| p.managed_by.is_system())
                .map(|p| p.tracking_record)
                .collect(),
            prev_may_be_missing,
        }),
        Some(desired) => Some(TrackingRecordTransition {
            desired: Some(desired.tracking_record),
            prev: prev
                .into_iter()
                .filter(|p| p.managed_by.is_system())
                .map(|p| p.tracking_record)
                .collect(),
            prev_may_be_missing,
        }),
    }
}

pub fn diff<T: PartialEq>(t: Option<&TrackingRecordTransition<T>>) -> Option<DiffAction> {
    let t = t?;
    match &t.desired {
        None if t.prev.is_empty() => None,
        None => Some(DiffAction::Delete),
        Some(desired) if t.prev.iter().any(|p| p != desired) => Some(DiffAction::Replace),
        Some(_) if !t.prev_may_be_missing => None,
        Some(_) if t.prev.is_empty() => Some(DiffAction::Insert),
        Some(_) => Some(DiffAction::Upsert),
    }
}

/// A tracking record with a `main` component and a set of keyed `sub`
/// components.
///
/// This is useful when a single identity produces:
///
/// - a **main** record (single state), and
/// - multiple **sub** records keyed by some hashable `K`.
///
/// [`diff_composite`] computes the main action plus grouped sub-state
/// transitions. A concrete example is a SQL table target: the primary-key
/// columns + virtual-table signature are the `main` record, and each non-PK
/// column is a `sub` record keyed by its name — letting the connector tell a
/// full table drop/recreate (`main` changed) apart from an incremental
/// `ALTER TABLE ADD COLUMN` (`main` unchanged, one `sub` inserted).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompositeTrackingRecord<M, K, S>
where
    K: Eq + Hash,
{
    pub main: M,
    pub sub: HashMap<K, S>,
}

impl<M, K, S> CompositeTrackingRecord<M, K, S>
where
    K: Eq + Hash,
{
    pub fn new(main: M, sub: HashMap<K, S>) -> Self {
        Self { main, sub }
    }
}

/// Internal mutable accumulator used by [`diff_composite`]. Mirrors the
/// `_GroupedStates` dataclass on the Python side.
struct GroupedStates<S> {
    desired: Option<S>,
    prev: Vec<S>,
}

impl<S> Default for GroupedStates<S> {
    fn default() -> Self {
        Self {
            desired: None,
            prev: Vec::new(),
        }
    }
}

/// Compute a diff for a composite state and group its sub-state transitions.
///
/// Returns a pair of:
/// - the **main** diff action (via [`diff`] on the `.main` field), and
/// - a mapping from each observed or desired sub-key to a
///   [`TrackingRecordTransition`] for that key — feed each into [`diff`] to get
///   its action.
///
/// If the main action is `Replace` or `Delete`, sub-state observations are
/// treated as potentially missing (a main-level rewrite can imply sub-state
/// churn), so `prev_may_be_missing` is forced on for every sub transition.
/// A sub-key not seen in *every* observed `prev` is likewise marked
/// potentially-missing.
pub fn diff_composite<M, K, S>(
    t: Option<&TrackingRecordTransition<CompositeTrackingRecord<M, K, S>>>,
) -> (Option<DiffAction>, HashMap<K, TrackingRecordTransition<S>>)
where
    M: PartialEq,
    K: Eq + Hash + Clone,
    S: Clone,
{
    let Some(t) = t else {
        return (None, HashMap::new());
    };

    let Some(desired) = &t.desired else {
        // Desired is non-existence: delete the whole composite (sub-states go
        // with it), or no-op if nothing was ever observed.
        if t.prev.is_empty() {
            return (None, HashMap::new());
        }
        return (Some(DiffAction::Delete), HashMap::new());
    };

    // Diff the main records. Borrow rather than clone — `diff` only compares.
    let main_transition = TrackingRecordTransition {
        desired: Some(&desired.main),
        prev: t.prev.iter().map(|p| &p.main).collect(),
        prev_may_be_missing: t.prev_may_be_missing,
    };
    let main_action = diff(Some(&main_transition));

    let sub_prev_may_be_missing = t.prev_may_be_missing
        || matches!(
            main_action,
            Some(DiffAction::Replace) | Some(DiffAction::Delete)
        );

    let prev_count = t.prev.len();
    let mut grouped: HashMap<K, GroupedStates<S>> = HashMap::new();
    for p in &t.prev {
        for (sub_key, sub_state) in &p.sub {
            grouped
                .entry(sub_key.clone())
                .or_default()
                .prev
                .push(sub_state.clone());
        }
    }
    for (sub_key, desired_state) in &desired.sub {
        grouped.entry(sub_key.clone()).or_default().desired = Some(desired_state.clone());
    }

    let groups = grouped
        .into_iter()
        .map(|(k, g)| {
            let prev_may_be_missing = sub_prev_may_be_missing || g.prev.len() < prev_count;
            (
                k,
                TrackingRecordTransition {
                    desired: g.desired,
                    prev: g.prev,
                    prev_may_be_missing,
                },
            )
        })
        .collect();

    (main_action, groups)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(value: &str, managed_by: ManagedBy) -> MutualTrackingRecord<String> {
        MutualTrackingRecord::new(value.to_string(), managed_by)
    }

    #[test]
    fn user_managed_desired_suppresses_system_diff() {
        let resolved = resolve_system_transition(
            Some(rec("schema-v2", ManagedBy::User)),
            vec![rec("schema-v1", ManagedBy::System)],
            false,
        );
        assert_eq!(resolved, None);
    }

    #[test]
    fn user_managed_previous_suppresses_delete() {
        let resolved = resolve_system_transition(None, vec![rec("schema", ManagedBy::User)], false);
        assert_eq!(resolved, None);
    }

    #[test]
    fn system_transition_filters_user_previous_records() {
        let resolved = resolve_system_transition(
            Some(rec("schema-v2", ManagedBy::System)),
            vec![
                rec("schema-user", ManagedBy::User),
                rec("schema-v1", ManagedBy::System),
            ],
            false,
        )
        .unwrap();
        assert_eq!(resolved.prev, vec!["schema-v1".to_string()]);
        assert_eq!(diff(Some(&resolved)), Some(DiffAction::Replace));
    }

    #[test]
    fn diff_distinguishes_insert_upsert_replace_delete() {
        assert_eq!(
            diff(Some(&TrackingRecordTransition {
                desired: Some("x"),
                prev: Vec::<&str>::new(),
                prev_may_be_missing: true,
            })),
            Some(DiffAction::Insert)
        );
        assert_eq!(
            diff(Some(&TrackingRecordTransition {
                desired: Some("x"),
                prev: vec!["x"],
                prev_may_be_missing: true,
            })),
            Some(DiffAction::Upsert)
        );
        assert_eq!(
            diff(Some(&TrackingRecordTransition {
                desired: Some("x"),
                prev: vec!["y"],
                prev_may_be_missing: false,
            })),
            Some(DiffAction::Replace)
        );
        assert_eq!(
            diff(Some(&TrackingRecordTransition {
                desired: None::<&str>,
                prev: vec!["x"],
                prev_may_be_missing: false,
            })),
            Some(DiffAction::Delete)
        );
    }

    // ----- composite layer ---------------------------------------------------
    //
    // These mirror, at the pure-logic level, the scenarios that the Python
    // SQLite connector exercises end-to-end in `test_sqlite_target.py`
    // (create table, no-change, add column, drop column, change column type,
    // drop table, full table replace). `main` stands in for the PK/virtual-
    // table signature; each `sub` keyed by column name stands in for a non-PK
    // column's type signature.

    type Composite = CompositeTrackingRecord<String, String, String>;

    fn comp(main: &str, subs: &[(&str, &str)]) -> Composite {
        CompositeTrackingRecord::new(
            main.to_string(),
            subs.iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }

    fn transition(
        desired: Option<Composite>,
        prev: Vec<Composite>,
        prev_may_be_missing: bool,
    ) -> TrackingRecordTransition<Composite> {
        TrackingRecordTransition {
            desired,
            prev,
            prev_may_be_missing,
        }
    }

    /// The action `diff` would take for the sub-key `key` in `groups`.
    /// `None` for a key absent from the map (never observed nor desired).
    fn sub_action(
        groups: &HashMap<String, TrackingRecordTransition<String>>,
        key: &str,
    ) -> Option<DiffAction> {
        diff(groups.get(key))
    }

    #[test]
    fn composite_none_and_empty_inputs_are_noops() {
        let (main, groups) = diff_composite::<String, String, String>(None);
        assert_eq!(main, None);
        assert!(groups.is_empty());

        // Desired non-existence with nothing ever observed: still a no-op.
        let t = transition(None, vec![], false);
        let (main, groups) = diff_composite(Some(&t));
        assert_eq!(main, None);
        assert!(groups.is_empty());
    }

    #[test]
    fn composite_delete_when_desired_absent_and_prev_present() {
        // Drop table: desired gone, something was there. Sub-states ride along
        // with the main delete, so no per-sub transitions are emitted.
        let t = transition(None, vec![comp("pk", &[("col:name", "text")])], false);
        let (main, groups) = diff_composite(Some(&t));
        assert_eq!(main, Some(DiffAction::Delete));
        assert!(groups.is_empty());
    }

    #[test]
    fn composite_create_inserts_main_and_each_sub() {
        // First run: no prev, prev_may_be_missing=true (nothing scanned yet).
        let desired = comp("pk", &[("col:name", "text"), ("col:value", "int")]);
        let t = transition(Some(desired), vec![], true);
        let (main, groups) = diff_composite(Some(&t));
        assert_eq!(main, Some(DiffAction::Insert));
        assert_eq!(sub_action(&groups, "col:name"), Some(DiffAction::Insert));
        assert_eq!(sub_action(&groups, "col:value"), Some(DiffAction::Insert));
    }

    #[test]
    fn composite_no_change_emits_no_actions() {
        // Re-run with identical, fully-observed state: everything converged.
        let state = comp("pk", &[("col:name", "text"), ("col:value", "int")]);
        let t = transition(Some(state.clone()), vec![state], false);
        let (main, groups) = diff_composite(Some(&t));
        assert_eq!(main, None);
        assert_eq!(sub_action(&groups, "col:name"), None);
        assert_eq!(sub_action(&groups, "col:value"), None);
    }

    #[test]
    fn composite_add_column_inserts_only_new_sub() {
        // ALTER TABLE ADD COLUMN: main unchanged, one new sub appears.
        let prev = comp("pk", &[("col:name", "text")]);
        let desired = comp("pk", &[("col:name", "text"), ("col:extra", "text")]);
        let t = transition(Some(desired), vec![prev], false);
        let (main, groups) = diff_composite(Some(&t));
        // Table itself is untouched.
        assert_eq!(main, None);
        // Existing column converged; new column inserted (its prev is empty and
        // it was missing from the observed snapshot, so prev_may_be_missing).
        assert_eq!(sub_action(&groups, "col:name"), None);
        assert_eq!(sub_action(&groups, "col:extra"), Some(DiffAction::Insert));
    }

    #[test]
    fn composite_drop_column_deletes_only_missing_sub() {
        let prev = comp("pk", &[("col:name", "text"), ("col:value", "int")]);
        let desired = comp("pk", &[("col:name", "text")]);
        let t = transition(Some(desired), vec![prev], false);
        let (main, groups) = diff_composite(Some(&t));
        assert_eq!(main, None);
        assert_eq!(sub_action(&groups, "col:name"), None);
        // `col:value` is observed but no longer desired → delete that column.
        assert_eq!(sub_action(&groups, "col:value"), Some(DiffAction::Delete));
    }

    #[test]
    fn composite_change_column_type_replaces_sub() {
        let prev = comp("pk", &[("col:value", "int")]);
        let desired = comp("pk", &[("col:value", "text")]);
        let t = transition(Some(desired), vec![prev], false);
        let (main, groups) = diff_composite(Some(&t));
        assert_eq!(main, None);
        assert_eq!(sub_action(&groups, "col:value"), Some(DiffAction::Replace));
    }

    #[test]
    fn composite_main_replace_forces_sub_rewrite() {
        // Primary key signature changed → table is dropped & recreated. Even an
        // otherwise-identical column must be rewritten, so its sub transition is
        // marked prev_may_be_missing (upsert), not a no-op.
        let prev = comp("pk-v1", &[("col:name", "text")]);
        let desired = comp("pk-v2", &[("col:name", "text")]);
        let t = transition(Some(desired), vec![prev], false);
        let (main, groups) = diff_composite(Some(&t));
        assert_eq!(main, Some(DiffAction::Replace));
        assert_eq!(sub_action(&groups, "col:name"), Some(DiffAction::Upsert));
    }

    #[test]
    fn composite_groups_prev_across_multiple_observations() {
        // Ambiguous history: two possible previous records disagree on a
        // column's type. Both observations are grouped under the sub-key, and
        // `diff` sees the disagreement as a replace.
        let prev_a = comp("pk", &[("col:value", "int")]);
        let prev_b = comp("pk", &[("col:value", "bigint")]);
        let desired = comp("pk", &[("col:value", "int")]);
        let t = transition(Some(desired), vec![prev_a, prev_b], false);
        let (main, groups) = diff_composite(Some(&t));
        assert_eq!(main, None);
        // prev = ["int", "bigint"]; one differs from desired "int" → replace.
        let group = groups.get("col:value").unwrap();
        assert_eq!(group.prev.len(), 2);
        assert_eq!(diff(Some(group)), Some(DiffAction::Replace));
    }

    #[test]
    fn composite_partial_observation_marks_sub_missing() {
        // Two prev observations; a column appears in only one of them. It is
        // grouped with a single prev entry but flagged prev_may_be_missing
        // (len(prev) < number of observations), so a matching desired still
        // upserts to guarantee convergence.
        let prev_a = comp("pk", &[("col:name", "text"), ("col:value", "int")]);
        let prev_b = comp("pk", &[("col:name", "text")]);
        let desired = comp("pk", &[("col:name", "text"), ("col:value", "int")]);
        let t = transition(Some(desired), vec![prev_a, prev_b], false);
        let (main, groups) = diff_composite(Some(&t));
        assert_eq!(main, None);
        // `col:name` seen in both observations and matches → converged.
        assert_eq!(sub_action(&groups, "col:name"), None);
        // `col:value` seen in only one observation → upsert to be safe.
        let group = groups.get("col:value").unwrap();
        assert_eq!(group.prev.len(), 1);
        assert!(group.prev_may_be_missing);
        assert_eq!(sub_action(&groups, "col:value"), Some(DiffAction::Upsert));
    }
}

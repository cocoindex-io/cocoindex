//! Small diff helpers for managed connector targets.
//!
//! These mirror the Python `cocoindex.connectorkits.target/statediff` contract
//! for resources that can be owned either by CocoIndex or by the user.

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
}

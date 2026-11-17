use std::collections::btree_map;

use crate::prelude::*;

use crate::state::stable_path::{StableKey, StablePathRef};

#[derive(Default)]
pub struct ChildStablePathSet {
    pub children: BTreeMap<StableKey, StablePathSet>,
}

impl ChildStablePathSet {
    pub fn add_child(&mut self, path: StablePathRef, info: StablePathSet) -> Result<()> {
        let Some((last, dir)) = path.split_last() else {
            client_bail!("Path is empty");
        };
        let mut current = self;
        for key in dir {
            match current
                .children
                .entry(key.clone())
                .or_insert_with(StablePathSet::directory)
            {
                StablePathSet::Directory(dir) => {
                    current = dir;
                }
                StablePathSet::Component | StablePathSet::Skipped => {
                    client_bail!("{key} is not a directory in path {path}");
                }
            }
        }
        match current.children.entry(last.clone()) {
            btree_map::Entry::Occupied(entry) => match entry.get() {
                StablePathSet::Component | StablePathSet::Skipped => already_declared_bail!(
                    "Child path {path} is mounted more than once in one run of its parent \
                     component. Each mount needs its own path: keep item keys unique within \
                     one mount_each() call, and give each call its own subpath"
                ),
                StablePathSet::Directory(_) => already_declared_bail!(
                    "Child path {path} is mounted as a component, but other children of the \
                     same parent component run are already mounted under it"
                ),
            },
            btree_map::Entry::Vacant(entry) => {
                entry.insert(info);
                Ok(())
            }
        }
    }
}

pub enum StablePathSet {
    Directory(ChildStablePathSet),
    Component,
    /// The run mounted a component here and its selector left it out. The
    /// mount counts as declared, as a mount the run executes does, but says
    /// nothing about what the store holds at this path.
    Skipped,
}

impl StablePathSet {
    pub fn directory() -> Self {
        Self::Directory(ChildStablePathSet::default())
    }

    /// Something at or below this node is a component the run executes.
    pub(crate) fn has_executed_component(&self) -> bool {
        match self {
            Self::Directory(dir) => dir
                .children
                .values()
                .any(StablePathSet::has_executed_component),
            Self::Component => true,
            Self::Skipped => false,
        }
    }
}

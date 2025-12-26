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
                .or_insert_with(|| StablePathSet::directory())
            {
                StablePathSet::Directory(dir) => {
                    current = dir;
                }
                StablePathSet::Component => {
                    client_bail!("{key} is not a directory in path {path}");
                }
            }
        }
        match current.children.entry(last.clone()) {
            btree_map::Entry::Occupied(_) => {
                client_bail!("Path {path} already exists");
            }
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
}

impl StablePathSet {
    pub fn directory() -> Self {
        Self::Directory(ChildStablePathSet::default())
    }
}

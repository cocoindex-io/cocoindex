use std::collections::btree_map;

use crate::prelude::*;

use crate::state::state_path::{StateKey, StatePathRef};

#[derive(Default)]
pub struct ChildStatePathSet {
    pub children: BTreeMap<StateKey, StatePathSet>,
}

impl ChildStatePathSet {
    pub fn add_child(&mut self, path: StatePathRef, info: StatePathSet) -> Result<()> {
        let Some((last, dir)) = path.split_last() else {
            bail!("Path is empty");
        };
        let mut current = self;
        for key in dir {
            match current
                .children
                .entry(key.clone())
                .or_insert_with(|| StatePathSet::directory())
            {
                StatePathSet::Directory(dir) => {
                    current = dir;
                }
                StatePathSet::Component => {
                    bail!("{key} is not a directory in path {path}");
                }
            }
        }
        match current.children.entry(last.clone()) {
            btree_map::Entry::Occupied(_) => {
                bail!("Path {path} already exists");
            }
            btree_map::Entry::Vacant(entry) => {
                entry.insert(info);
                Ok(())
            }
        }
    }
}

pub enum StatePathSet {
    Directory(ChildStatePathSet),
    Component,
}

impl StatePathSet {
    pub fn directory() -> Self {
        Self::Directory(ChildStatePathSet::default())
    }
}

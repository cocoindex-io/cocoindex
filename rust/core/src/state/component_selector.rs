//! Which components one update runs: sliced execution.

use std::collections::HashMap;

use crate::prelude::*;
use crate::state::stable_path::{StableKey, StablePath};

/// The components an update runs, as a set of component paths. Selecting a path
/// selects its whole subtree, so one path names one component or everything
/// below a prefix alike.
///
/// A tree with a `HashMap` per level, so that classifying a path costs one
/// lookup per path part however many paths are selected (a page of an
/// observability UI selects tens of them).
#[derive(Debug, Clone)]
pub struct ComponentSelector {
    root: Arc<SelectorNode>,
    /// This view belongs to a run inside a selected subtree: every path below
    /// it is selected. The update as a whole may still leave components out.
    within_selected: bool,
}

#[derive(Debug, Default)]
struct SelectorNode {
    selected: bool,
    children: HashMap<StableKey, SelectorNode>,
}

/// How a selector relates to one path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComponentSelection {
    /// The path, or a prefix of it, is selected: it runs as without a selector.
    Selected,
    /// A selected path lies strictly below: it runs to reach that path, and
    /// the selector still applies to what it mounts.
    AncestorOfSelected,
    /// Nothing about the path is selected.
    Unselected,
}

/// What mounting a component comes to under the selector of the mounting run.
pub(crate) enum MountDecision {
    /// The component is unselected: no run, and nothing about it changes.
    Skip,
    /// The component runs, under this selector (see
    /// [`ComponentSelector::scoped_to`]).
    Run(Option<ComponentSelector>),
}

impl ComponentSelector {
    pub fn new(paths: impl IntoIterator<Item = StablePath>) -> Self {
        let mut root = SelectorNode::default();
        for path in paths {
            let mut node = &mut root;
            for key in path.iter() {
                if node.selected {
                    break;
                }
                node = node.children.entry(key.clone()).or_default();
            }
            // Selecting a path covers what was selected below it.
            node.selected = true;
            node.children = HashMap::new();
        }
        Self {
            root: Arc::new(root),
            within_selected: false,
        }
    }

    pub fn classify(&self, path: &StablePath) -> ComponentSelection {
        if self.within_selected {
            return ComponentSelection::Selected;
        }
        let mut node = &*self.root;
        for key in path.iter() {
            if node.selected {
                return ComponentSelection::Selected;
            }
            let Some(child) = node.children.get(key) else {
                return ComponentSelection::Unselected;
            };
            node = child;
        }
        if node.selected {
            ComponentSelection::Selected
        } else {
            ComponentSelection::AncestorOfSelected
        }
    }

    /// The path is selected, directly or through a prefix.
    pub fn covers(&self, path: &StablePath) -> bool {
        self.classify(path) == ComponentSelection::Selected
    }

    /// The update covers `path`, wherever this view's run is. Unlike
    /// [`Self::covers`], a view inside a selected subtree answers for paths
    /// outside it too.
    pub(crate) fn update_covers(&self, path: &StablePath) -> bool {
        Self {
            root: self.root.clone(),
            within_selected: false,
        }
        .covers(path)
    }

    /// A run under this view can leave out something it mounts. False inside a
    /// selected subtree, from where on the run is an ordinary one. That never
    /// comes back on the way down, so the ancestors of a run with this true
    /// have it true too.
    pub(crate) fn can_leave_out_children(&self) -> bool {
        !self.within_selected
    }

    /// The whole app is selected, so the update is a plain one.
    pub(crate) fn selects_everything(&self) -> bool {
        self.root.selected
    }

    /// The selector a run of the component at `path` is under, given the
    /// selector of the run that started it.
    pub(crate) fn scoped_to(selector: Option<&Self>, path: &StablePath) -> Option<Self> {
        selector.map(|selector| Self {
            root: selector.root.clone(),
            within_selected: selector.covers(path),
        })
    }

    /// See [`MountDecision`]. A child the parent waits for a value from
    /// (`use_mount`, the root) always runs, so it takes [`Self::scoped_to`].
    pub(crate) fn for_mount(selector: Option<&Self>, path: &StablePath) -> MountDecision {
        if selector
            .is_some_and(|selector| selector.classify(path) == ComponentSelection::Unselected)
        {
            return MountDecision::Skip;
        }
        MountDecision::Run(Self::scoped_to(selector, path))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(parts: &[StableKey]) -> StablePath {
        parts.iter().fold(StablePath::root(), |path, part| {
            path.concat_part(part.clone())
        })
    }

    fn s(value: &str) -> StableKey {
        StableKey::Str(Arc::from(value))
    }

    #[test]
    fn exact_path_selects_itself_and_its_subtree_and_runs_its_ancestors() {
        let selector = ComponentSelector::new([path(&[s("process"), s("a")])]);
        assert_eq!(
            selector.classify(&StablePath::root()),
            ComponentSelection::AncestorOfSelected
        );
        assert_eq!(
            selector.classify(&path(&[s("process")])),
            ComponentSelection::AncestorOfSelected
        );
        assert_eq!(
            selector.classify(&path(&[s("process"), s("a")])),
            ComponentSelection::Selected
        );
        assert_eq!(
            selector.classify(&path(&[s("process"), s("a"), s("chunk")])),
            ComponentSelection::Selected
        );
        assert_eq!(
            selector.classify(&path(&[s("process"), s("b")])),
            ComponentSelection::Unselected
        );
        assert_eq!(
            selector.classify(&path(&[s("other")])),
            ComponentSelection::Unselected
        );
    }

    #[test]
    fn a_prefix_covers_paths_selected_below_it_in_either_order() {
        let prefix = path(&[s("process")]);
        let below = path(&[s("process"), s("a")]);
        for paths in [
            vec![prefix.clone(), below.clone()],
            vec![below.clone(), prefix.clone()],
        ] {
            let selector = ComponentSelector::new(paths);
            assert!(selector.covers(&prefix));
            assert!(selector.covers(&below));
            assert!(selector.covers(&path(&[s("process"), s("b")])));
            assert!(!selector.covers(&path(&[s("other")])));
        }
    }

    #[test]
    fn the_root_path_selects_everything_and_no_path_selects_nothing() {
        let everything = ComponentSelector::new([StablePath::root()]);
        assert!(everything.covers(&StablePath::root()));
        assert!(everything.covers(&path(&[s("any"), s("thing")])));

        let nothing = ComponentSelector::new([]);
        assert_eq!(
            nothing.classify(&StablePath::root()),
            ComponentSelection::AncestorOfSelected
        );
        assert_eq!(
            nothing.classify(&path(&[s("any")])),
            ComponentSelection::Unselected
        );
    }

    #[test]
    fn keys_match_by_type_not_by_how_they_print() {
        let tuple = StableKey::Array(Arc::from(vec![s("a"), StableKey::Int(1)]));
        let selector = ComponentSelector::new([
            path(&[StableKey::Int(1)]),
            path(&[StableKey::Bool(true)]),
            path(std::slice::from_ref(&tuple)),
            path(&[StableKey::Symbol(Arc::from("process"))]),
        ]);
        assert!(selector.covers(&path(&[StableKey::Int(1)])));
        assert!(selector.covers(&path(&[StableKey::Bool(true)])));
        assert!(selector.covers(&path(&[tuple])));
        assert!(selector.covers(&path(&[StableKey::Symbol(Arc::from("process"))])));
        assert!(!selector.covers(&path(&[s("1")])));
        assert!(!selector.covers(&path(&[s("true")])));
        assert!(!selector.covers(&path(&[s("process")])));
    }

    #[test]
    fn scoping_inside_a_selected_subtree_covers_everything_below() {
        let selector = ComponentSelector::new([path(&[s("process"), s("a")])]);
        let scoped =
            |target: &StablePath| ComponentSelector::scoped_to(Some(&selector), target).unwrap();
        assert!(scoped(&path(&[s("process")])).can_leave_out_children());
        assert!(scoped(&path(&[s("other")])).can_leave_out_children());
        let inside = scoped(&path(&[s("process"), s("a")]));
        assert!(!inside.can_leave_out_children());
        assert!(inside.covers(&path(&[s("process"), s("a"), s("x")])));
        assert!(!inside.selects_everything());
        assert!(ComponentSelector::scoped_to(None, &path(&[s("process")])).is_none());
    }
}

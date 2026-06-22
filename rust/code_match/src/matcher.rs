//! The matcher: flatten a candidate node into a leaf frontier + node spans, then
//! match the flat pattern against it with a memoized DP. Metavariables snap to
//! node boundaries.
//!
//! Bindings are threaded *forward* (insert on bind, restore on backtrack) so
//! repeated metavar names enforce equality (`$A ... $A` must capture equal
//! text). The fail-memo on `(pi, li)` is only sound when names are unique, so it
//! is disabled for patterns that repeat a name.
//!
//! `Match`/`Capture` borrow the source string (`'s`): their `text` is a slice of
//! it, so callers can't accidentally pass the wrong source.

use std::collections::{HashMap, HashSet};
use std::ops::Range;

use cocoindex_utils::error::Result;
use regex::Regex;
use tree_sitter::{Node, Parser, Tree};

use crate::config::LangConfig;
use crate::lexer::{Cardinality, PatternItem, lex};

/// A captured metavariable: the matched source text and its byte range.
#[derive(Debug, Clone)]
pub struct Capture<'s> {
    pub text: &'s str,
    pub range: Range<usize>,
    /// true for a `$$$` (sibling-run) capture, false for a single-node `$X`
    pub multi: bool,
}

#[derive(Debug, Clone)]
pub struct Match<'s> {
    /// tree-sitter node kind of the matched node.
    pub kind: &'static str,
    pub range: Range<usize>,
    pub text: &'s str,
    pub captures: HashMap<String, Capture<'s>>,
}

impl<'s> Match<'s> {
    pub fn capture(&self, name: &str) -> Option<&Capture<'s>> {
        self.captures.get(name)
    }
    /// Convenience: the captured text for `name`, if bound.
    pub fn capture_text(&self, name: &str) -> Option<&'s str> {
        self.captures.get(name).map(|c| c.text)
    }
}

#[derive(Clone)]
struct Leaf {
    text: String,
    anon: bool,
    start_byte: usize,
    end_byte: usize,
}

#[derive(Clone)]
struct Span {
    start_leaf: usize,
    end_leaf: usize, // inclusive
    start_byte: usize,
    end_byte: usize,
    /// tree-sitter node kind (`Node::kind()` is `&'static str`).
    node_kind: &'static str,
    /// `[start_leaf, end_leaf]` of each direct child that produced leaves, in
    /// order. Used only for candidates (partial child-aligned matching); empty
    /// for leaf nodes. (Carried on every span for simplicity; see §matches.)
    child_bounds: Vec<(usize, usize)>,
}

struct Indexed {
    leaves: Vec<Leaf>,
    /// spans (named nodes) grouped by first leaf, sorted by end_leaf desc (greedy-largest-first)
    spans_by_start: Vec<Vec<Span>>,
    /// every named node, used as a match candidate
    candidates: Vec<Span>,
    /// Per leaf, the ids of nodes for which it is a direct-child **start** /
    /// **end**. A same-level (`*`) run `[li, next)` is a sibling slice iff some
    /// node owns `li` as a child-start *and* `next-1` as a child-end (children
    /// tile a node contiguously in leaf space). Used by `match_multi`.
    child_start_owners: Vec<Vec<u32>>,
    child_end_owners: Vec<Vec<u32>>,
}

impl Indexed {
    /// Is `[li, next)` a contiguous run of one node's direct children?
    fn same_level(&self, li: usize, next: usize) -> bool {
        if next <= li {
            return true; // empty run
        }
        let starts = &self.child_start_owners[li];
        let ends = &self.child_end_owners[next - 1];
        starts.iter().any(|n| ends.contains(n))
    }

    /// Is `[li, next)` *exactly one* direct child of some parent — a single
    /// same-level sibling node, not the whole subtree and not a multi-sibling run?
    /// (`same_level` is true for the whole of a node's children too — e.g. a root
    /// `module` spanning all its statements — which `\{{ \}}` must *not* treat as a
    /// single node.) True iff some parent `M` owns `li` as a child-start and
    /// `next-1` as a child-end with **no** child-end of `M` strictly inside.
    fn single_child(&self, li: usize, next: usize) -> bool {
        if next <= li {
            return false;
        }
        let last = next - 1;
        self.child_start_owners[li].iter().any(|&m| {
            self.child_end_owners[last].contains(&m)
                && !(li..last).any(|e| self.child_end_owners[e].contains(&m))
        })
    }
}

/// Compiled, language-bound pattern.
pub struct Pattern {
    items: Vec<PatternItem>,
    cfg: LangConfig,
    /// Whether the `(pi, li)` fail-memo is sound *globally*. It isn't when
    /// forward-threaded bindings can change whether a `(pi, li)` matches: a repeated
    /// metavar name, or a containment (`\{{ \}}`), whose INNER is matched against many
    /// descendants with **different `stops`**.
    use_memo: bool,
    /// No repeated metavar name → bindings never change whether a `(pi, li)` matches.
    /// Lets the containment use a *fresh per-descendant* memo for INNER (the descendant's
    /// `stops` are fixed, so it's sound there) even when `use_memo` is off for the whole
    /// pattern — so INNER's leading-tolerance starts collapse to one pass, like the
    /// top-level. (Repeated names — the "backreference" case — keep it off.)
    no_dups: bool,
}

impl Pattern {
    /// Compile a pattern for `cfg`. Fails with a `client` error on a malformed
    /// metavar matcher (e.g. an unparseable regex) or unbalanced `\{{` / `\}}`.
    pub fn compile(pattern: &str, cfg: &LangConfig) -> Result<Pattern> {
        let items = lex(pattern, cfg)?;
        // A bounded sub-region (`\{{ \}}` or `\{ \}`) matches its inner pattern against
        // *several* candidates with different `hi`/`stops`, so the start-independent
        // `(pi, li)` fail-memo isn't globally sound — disable it (as for containment).
        let has_brackets = items.iter().any(|it| {
            matches!(
                it,
                PatternItem::ContainsOpen { .. } | PatternItem::WholeOpen { .. }
            )
        });
        let no_dups = !detect_dup_names(&items);
        let use_memo = no_dups && !has_brackets;
        Ok(Pattern {
            items,
            cfg: cfg.clone(),
            use_memo,
            no_dups,
        })
    }

    /// Build a [`Prefilter`](crate::Prefilter) — the pattern's required literal
    /// content, for cheaply rejecting sources that can't match before parsing.
    /// `min_len` drops terms shorter than this many chars (default sensibly 3).
    pub fn prefilter(&self, min_len: usize) -> crate::Prefilter {
        crate::Prefilter::build(&self.items, &self.cfg, min_len)
    }

    /// Match `source`, skipping the parse entirely when `prefilter` rejects it —
    /// the point of prefiltering (the parse dominates cost at scale). `prefilter`
    /// should be one this pattern produced; a mismatched one stays *sound* (it can
    /// only over-accept) but may parse needlessly.
    pub fn matches_prefiltered<'s>(
        &self,
        source: &'s str,
        prefilter: &crate::Prefilter,
    ) -> Vec<Match<'s>> {
        if prefilter.might_match(source) {
            self.matches(source)
        } else {
            Vec::new()
        }
    }

    pub fn matches<'s>(&self, source: &'s str) -> Vec<Match<'s>> {
        let mut parser = Parser::new();
        parser
            .set_language(&self.cfg.language)
            .expect("load language");
        let tree = parser.parse(source, None).expect("parse source");
        self.matches_in_tree(&tree, source)
    }

    /// For every containment in the pattern, find the descendants its INNER matches —
    /// once, up front — keyed by INNER item position (`open + 1`), as `(start_leaf,
    /// candidate_index)` sorted by `start_leaf`. `match_contains` then binary-searches
    /// the bracketed node's extent for a hit instead of re-scanning every candidate on
    /// every outer position, turning containment from O(N²) (O(N³) when nested) into
    /// O(N·log N).
    ///
    /// Built per containment, gated by `containment_inner_cacheable`: only an INNER
    /// whose captured names never appear outside its bracket is cached (its matches are
    /// then independent of outer bindings, so the empty-`bound` precompute is valid).
    /// Others — a backref into the outside — stay uncached and `match_contains` scans.
    ///
    /// INNER ranges are processed innermost-first (largest `open` index first) so a
    /// nested containment's entry is already present when the enclosing INNER is
    /// resolved against each candidate.
    fn build_contains_cache(&self, idx: &Indexed, source: &str) -> HashMap<usize, Vec<(u32, u32)>> {
        let mut cache: HashMap<usize, Vec<(u32, u32)>> = HashMap::new();
        let mut opens: Vec<(usize, usize)> = self
            .items
            .iter()
            .enumerate()
            .filter_map(|(pi, it)| match it {
                PatternItem::ContainsOpen { close } => Some((pi, *close)),
                _ => None,
            })
            .collect();
        opens.sort_by(|a, b| b.0.cmp(&a.0)); // innermost (largest open) first
        for (open, close) in opens {
            // Cache this containment only when its INNER is self-contained — see
            // `containment_inner_cacheable`. A nested INNER whose name is bound by an
            // *enclosing* INNER (`\{{ \X(\*) \{{ return \X \}} \}}`) stays uncached and
            // falls back to the scan; the enclosing, self-contained INNER still caches.
            if !self.containment_inner_cacheable(open, close) {
                continue;
            }
            let inner = open + 1;
            let mut hits: Vec<(u32, u32)> = Vec::new();
            for (ci, cand) in idx.candidates.iter().enumerate() {
                let mut ctx = Ctx {
                    items: &self.items,
                    idx,
                    source,
                    use_memo: self.use_memo,
                    no_dups: self.no_dups,
                    bound: HashMap::new(),
                    fail: HashSet::new(),
                    stops: HashSet::new(),
                    matched_end: 0,
                    tolerant_end: None,
                    contains_cache: &cache,
                    delimiters: &self.cfg.trailing_delimiters,
                };
                if ctx.inner_matches_candidate(inner, close, cand) {
                    hits.push((cand.start_leaf as u32, ci as u32));
                }
            }
            hits.sort_by_key(|&(s, _)| s);
            cache.insert(inner, hits);
        }
        cache
    }

    /// Whether `\{{ INNER \}}` bracketed by `items[open]`..`items[close]` can use the
    /// precomputed cache: yes iff no name INNER captures also appears *outside* the
    /// bracket. Then INNER's matches don't depend on any outer binding (so the
    /// empty-`bound` precompute is valid) and its captures aren't referenced after the
    /// containment (so existence, not a specific descendant, suffices). A name repeated
    /// *within* INNER is fine — that backref resolves inside the INNER match. A name
    /// shared with the outside (`def foo(\P): \{{ return \P \}}`, or a nested INNER's
    /// `\X` bound by its enclosing INNER) is not, and keeps that level on the scan.
    fn containment_inner_cacheable(&self, open: usize, close: usize) -> bool {
        let name_at = |i: usize| match &self.items[i] {
            PatternItem::Meta { name: Some(n), .. } => Some(n.as_str()),
            _ => None,
        };
        let inner_names: HashSet<&str> = (open + 1..close).filter_map(name_at).collect();
        if inner_names.is_empty() {
            return true;
        }
        !(0..open)
            .chain(close + 1..self.items.len())
            .filter_map(name_at)
            .any(|n| inner_names.contains(n))
    }

    /// Match against an already-parsed `tree`, so one compiled AST can be reused
    /// across calls (e.g. shared with the chunk splitter instead of re-parsing).
    /// The tree MUST come from the same grammar as this pattern's
    /// [`LangConfig::language`]; tree-sitter is pinned to one version
    /// workspace-wide, so a tree the splitter parsed for the same language is
    /// compatible.
    pub fn matches_in_tree<'s>(&self, tree: &Tree, source: &'s str) -> Vec<Match<'s>> {
        let idx = index_tree(tree.root_node(), source.as_bytes());
        let n_items = self.items.len();
        // Resolve every containment's INNER against all descendants once, up front, so
        // the per-candidate DP answers a containment by binary search instead of a
        // fresh O(N) descendant scan (see `build_contains_cache`).
        let contains_cache = self.build_contains_cache(&idx, source);

        let mut out = Vec::new();
        for cand in &idx.candidates {
            // For each candidate we run the DP **once per start position** with the
            // fail-memo shared across starts and *trailing tolerance* in the base
            // case (the match may stop at any child-end boundary). That folds the old
            // O(children²) (i, j) scan into O(children) DP entries with one shared
            // memo → O(N·k). The whole-node case is just the entry that starts at the
            // candidate's first leaf and consumes everything.
            let kids = &cand.child_bounds;
            let hi = cand.end_leaf + 1;

            // Valid stop boundaries (child-end-exclusive). `li == hi` (whole-node /
            // a leaf candidate) is always allowed by the base case, so a childless
            // candidate needs no entry here.
            let stops: HashSet<usize> = kids.iter().map(|&(_, e)| e + 1).collect();

            let mut ctx = Ctx {
                items: &self.items,
                idx: &idx,
                source,
                use_memo: self.use_memo,
                no_dups: self.no_dups,
                bound: HashMap::new(),
                fail: HashSet::new(),
                stops,
                matched_end: 0,
                tolerant_end: None,
                contains_cache: &contains_cache,
                delimiters: &self.cfg.trailing_delimiters,
            };

            // Start positions: each child-start; a leaf candidate has none, so use
            // the candidate's start (whole-node only).
            let starts: Vec<usize> = if kids.is_empty() {
                vec![cand.start_leaf]
            } else {
                kids.iter().map(|&(s, _)| s).collect()
            };
            // child-start / child-end leaf → child index, to classify a fragment.
            let start_idx: HashMap<usize, usize> =
                kids.iter().enumerate().map(|(c, &(s, _))| (s, c)).collect();
            let end_idx: HashMap<usize, usize> =
                kids.iter().enumerate().map(|(c, &(_, e))| (e, c)).collect();

            // Collect the candidate's matches **leftmost-longest, non-overlapping**:
            // after a match `[a, b)` resume at `b` (skip overlapped starts). Because
            // `stops` is start-independent the fail-memo is shared across the whole
            // loop, so finding *every* occurrence is still O(N·k) — no worse than
            // finding the first; the old `break` was only an early exit. The DP is
            // greedy (longest stop first), so each match is the longest from its start.
            let mut next_start = 0; // earliest non-overlapping start (a leaf index)
            for a in starts {
                if a < next_start {
                    continue;
                }
                ctx.bound.clear();
                if !ctx.dp(0, n_items, a, hi) {
                    continue;
                }
                let b = ctx.matched_end; // child-end-exclusive, or `hi`
                // Whole-node coverage spans the entire candidate → the whole node, no
                // ≥2 gate. Otherwise a fragment: it must span **≥2** direct children,
                // except a single child that is one anonymous leaf (a keyword/punct
                // like `if` or an operator like `=>` — never a candidate on its own;
                // see §4). A single *named* child defers to its own candidate, so it
                // isn't reported here.
                let range = if a == cand.start_leaf && b == hi {
                    Some((cand.start_byte, cand.end_byte))
                } else if b > a {
                    let ci = start_idx[&a];
                    let cj = end_idx[&(b - 1)];
                    let ok = cj > ci || {
                        let (s, e) = kids[ci];
                        s == e && idx.leaves[s].anon
                    };
                    ok.then(|| (idx.leaves[a].start_byte, idx.leaves[b - 1].end_byte))
                } else {
                    // `b == a`: the pattern matched **empty** (e.g. an all-`\*` / `\?`
                    // pattern landing on a child boundary). A zero-width match isn't a
                    // fragment — drop it (and don't index `leaves[b - 1]`, which would
                    // be the leaf *before* `a` → an inverted byte range).
                    None
                };
                if let Some((start_byte, end_byte)) = range {
                    out.push(Match {
                        kind: cand.node_kind,
                        range: start_byte..end_byte,
                        text: &source[start_byte..end_byte],
                        captures: std::mem::take(&mut ctx.bound),
                    });
                    next_start = b; // non-overlapping: resume past this match
                }
            }
        }
        out
    }
}

fn detect_dup_names(items: &[PatternItem]) -> bool {
    let mut seen = HashSet::new();
    for it in items {
        if let PatternItem::Meta { name: Some(n), .. } = it
            && !seen.insert(n)
        {
            return true;
        }
    }
    false
}

fn index_tree(root: Node, src: &[u8]) -> Indexed {
    let mut c = Collector {
        src,
        leaves: Vec::new(),
        spans: Vec::new(),
        cs_pairs: Vec::new(),
        ce_pairs: Vec::new(),
        next_id: 0,
    };
    c.collect(root);

    let n = c.leaves.len();
    let mut spans_by_start: Vec<Vec<Span>> = vec![Vec::new(); n];
    for sp in &c.spans {
        spans_by_start[sp.start_leaf].push(sp.clone());
    }
    for v in spans_by_start.iter_mut() {
        // greedy-largest-first: by leaf extent, then by byte width — the latter
        // breaks ties when a node and a child share the same single leaf but the
        // node's byte range is wider (e.g. a raw string whose delimiters aren't
        // leaf children), so a metavar binds the outer node.
        v.sort_by_key(|s| {
            (
                std::cmp::Reverse(s.end_leaf),
                std::cmp::Reverse(s.end_byte - s.start_byte),
            )
        });
    }

    let mut child_start_owners: Vec<Vec<u32>> = vec![Vec::new(); n];
    let mut child_end_owners: Vec<Vec<u32>> = vec![Vec::new(); n];
    for (leaf, id) in c.cs_pairs {
        child_start_owners[leaf].push(id);
    }
    for (leaf, id) in c.ce_pairs {
        child_end_owners[leaf].push(id);
    }

    // Match candidates: drop leaf-equivalent wrappers — a named node spanning
    // exactly the same leaves as a descendant (e.g. a Python `block` whose only
    // child is a `return_statement`) would otherwise report the same match with
    // only a different `kind`. Such a wrapper has a single leaf-producing child,
    // so it can only match whole-node (never a child-run), making its match a
    // pure duplicate of the inner one. Candidates are in post-order (innermost
    // first), so keep the first occurrence of each leaf span. `spans_by_start`
    // keeps *all* spans — metavar binding still needs every nesting level.
    let mut seen_spans: HashSet<(usize, usize)> = HashSet::new();
    let candidates: Vec<Span> = c
        .spans
        .into_iter()
        .filter(|sp| seen_spans.insert((sp.start_leaf, sp.end_leaf)))
        .collect();

    Indexed {
        leaves: c.leaves,
        spans_by_start,
        candidates,
        child_start_owners,
        child_end_owners,
    }
}

/// Flattens the tree into the leaf frontier + node spans + child-boundary
/// ownership in a single pass.
struct Collector<'a> {
    src: &'a [u8],
    leaves: Vec<Leaf>,
    spans: Vec<Span>,
    cs_pairs: Vec<(usize, u32)>, // (child_start_leaf, parent_id)
    ce_pairs: Vec<(usize, u32)>, // (child_end_leaf, parent_id)
    next_id: u32,
}

impl Collector<'_> {
    fn collect(&mut self, node: Node) {
        // treat comments as insignificant
        if node.kind().contains("comment") {
            return;
        }
        let my_id = self.next_id; // pre-order id; this node is the parent of its children
        self.next_id += 1;
        let first = self.leaves.len();
        let mut child_bounds: Vec<(usize, usize)> = Vec::new();

        if node.child_count() == 0 {
            let text = node.utf8_text(self.src).unwrap_or("").to_string();
            // Keep tree-sitter's own leaf intact — a compound operator like `=>`
            // stays one anonymous leaf. The *pattern* side splits compounds into
            // single chars (see `config::detect_splittable`); the matcher's
            // `match_token_run` reconciles the two by letting a pattern char-run
            // match one source leaf's text.
            self.leaves.push(Leaf {
                text,
                anon: !node.is_named(),
                start_byte: node.start_byte(),
                end_byte: node.end_byte(),
            });
        } else {
            let mut cursor = node.walk();
            let children: Vec<Node> = node.children(&mut cursor).collect();
            for ch in children {
                let before = self.leaves.len();
                self.collect(ch);
                // record this child's leaf span (skip children that yielded none,
                // e.g. comments) for partial matching + same-level ownership
                if self.leaves.len() > before {
                    let last = self.leaves.len() - 1;
                    child_bounds.push((before, last));
                    self.cs_pairs.push((before, my_id));
                    self.ce_pairs.push((last, my_id));
                }
            }
        }

        if node.is_named() && self.leaves.len() > first {
            self.spans.push(Span {
                start_leaf: first,
                end_leaf: self.leaves.len() - 1,
                start_byte: node.start_byte(),
                end_byte: node.end_byte(),
                node_kind: node.kind(),
                child_bounds,
            });
        }
    }
}

struct Ctx<'a, 's> {
    items: &'a [PatternItem],
    idx: &'a Indexed,
    source: &'s str,
    use_memo: bool,
    /// No repeated names (see `Pattern::no_dups`) — lets `inner_matches_candidate`
    /// turn on a fresh per-descendant memo while the global `use_memo` stays off.
    no_dups: bool,
    bound: HashMap<String, Capture<'s>>,
    fail: HashSet<(usize, usize)>,
    /// Valid end positions for the **whole pattern** when matching against the
    /// current candidate: the child-end-exclusive boundaries `{ child.end + 1 }`
    /// (which include the candidate's own end). When the pattern is fully consumed
    /// (`pi == items.len()`) the match may stop at any of these — that's the
    /// trailing leading/trailing-tolerance, folded into one DP per candidate so the
    /// fail-memo is shared across every start position (→ O(N·k), not O(children²)).
    /// `stops` is fixed per candidate, so it doesn't depend on where matching began
    /// — which is what keeps the shared memo sound. Empty for a leaf candidate
    /// (whole-node only, via `li == hi`).
    stops: HashSet<usize>,
    /// Set to the stop position when the top-level base case succeeds, so the caller
    /// learns where the (possibly partial) match ended.
    matched_end: usize,
    /// While matching a containment's INNER against a descendant candidate, the INNER
    /// end (`close`). The base case then applies the same whole-node/fragment trailing
    /// tolerance (via `stops`) to INNER as to the whole pattern — so a bare keyword
    /// `\{{ throw \}}` matches its `throw_statement`, just like a top-level `throw`.
    /// `None` otherwise (every other sub-pattern end must land exactly on `hi`).
    tolerant_end: Option<usize>,
    /// Precomputed containment index: INNER item position (`open + 1`) → the
    /// descendants it matches, as `(start_leaf, candidate_index)` sorted by
    /// `start_leaf`. Built once per `matches_in_tree` (see `build_contains_cache`),
    /// so a containment is answered by binary-searching the bracketed node's extent
    /// instead of re-scanning every candidate per outer position — O(N·log N), not
    /// O(N²). Empty when the pattern repeats a name (`!no_dups`): INNER then depends
    /// on outer bindings, so `match_contains` falls back to the per-call scan.
    contains_cache: &'a HashMap<usize, Vec<(u32, u32)>>,
    /// Delimiters (`;`, `,`) trailing tolerance may skip inside the last child
    /// (see `LangConfig::trailing_delimiters`).
    delimiters: &'a HashSet<String>,
}

impl<'s> Ctx<'_, 's> {
    /// Match `items[pi..end]` against leaves `[li, hi)`. On success, `bound` holds
    /// the captures. `end` is the exclusive item bound: `items.len()` at the top
    /// level, or a containment's `close` index for an INNER sub-match — so
    /// the same `dp` engine matches a bracketed sub-pattern without it running
    /// past its `\}}`. (`end` is a structural function of `pi` — each item sits in
    /// exactly one bracket level — so the `(pi, li)` fail-memo stays well-keyed.)
    fn dp(&mut self, pi: usize, end: usize, li: usize, hi: usize) -> bool {
        if pi == end {
            // Top-level (the whole pattern consumed): leading/trailing tolerance —
            // the match may stop at any of the candidate's child-end boundaries, not
            // only at the very end. `stops` is candidate-fixed (start-independent),
            // so caching the states that lead here stays sound across start
            // positions. A containment-inner sub-pattern (`end < items.len()`) must
            // still land exactly on `hi`.
            if end == self.items.len() || self.tolerant_end == Some(end) {
                // Whole pattern, or a containment INNER matched against a candidate
                // (`tolerant_end`): the match may stop at any of the candidate's
                // child-end boundaries (`stops`), not only at the very end.
                if li == hi || self.stops.contains(&li) {
                    self.matched_end = li;
                    return true;
                }
                // Trailing tolerance *into* the last child: if the tail landed inside a
                // child with only statement **delimiters** (`;`, `,`) between it and the
                // next child boundary, consume them to that boundary. This is what lets
                // `if (\X) return \Y` match `if (c) return foo;` — `\Y` binds `foo` and
                // the `;` is free, the same tolerance `return \Y` gets at the top level.
                // Applies equally to a containment INNER (this branch covers both, via
                // `tolerant_end`): the integral-fragment rule for whole match *and* INNER
                // exists to avoid precedence violations (extracting `a+b` from `a+b*c`),
                // which an insignificant trailing delimiter can't cause — so skipping it
                // is sound there too. Delimiters only, never closers, so `f(\X` still
                // won't match `f(a)`.
                if let Some(&s) = self.stops.iter().filter(|&&s| s > li).min()
                    && (li..s).all(|l| {
                        let leaf = &self.idx.leaves[l];
                        leaf.anon && self.delimiters.contains(&leaf.text)
                    })
                {
                    self.matched_end = s;
                    return true;
                }
                return false;
            }
            // Any other sub-pattern end must land exactly on `hi`.
            return li == hi;
        }
        if self.use_memo && self.fail.contains(&(pi, li)) {
            return false;
        }

        // Copy the shared (`Copy`) reference out of `self` so matching on it
        // doesn't borrow `self`, leaving `self` free for the `&mut self` calls.
        let items = self.items;
        let ok = match &items[pi] {
            PatternItem::Token(_) => self.match_token_run(pi, end, li, hi),
            PatternItem::Str(s) => self.match_literal(pi, end, li, hi, s),
            PatternItem::Meta { name, card, regex } => match card {
                Cardinality::One => {
                    self.match_single(pi, end, li, hi, name.as_deref(), regex.as_ref())
                }
                // A regex on a run constrains every node in it (literal per-node,
                // §0). `OneOrMore` is `Many` with a non-empty run.
                Cardinality::Many => {
                    self.match_multi(pi, end, li, hi, name.as_deref(), false, regex.as_ref())
                }
                Cardinality::OneOrMore => {
                    self.match_multi(pi, end, li, hi, name.as_deref(), true, regex.as_ref())
                }
                Cardinality::Optional => {
                    self.match_optional(pi, end, li, hi, name.as_deref(), regex.as_ref())
                }
            },
            PatternItem::ContainsOpen { close } => self.match_contains(pi, *close, end, li, hi),
            PatternItem::WholeOpen { close } => self.match_whole(pi, *close, end, li, hi),
            // Never landed on: the outer DP jumps over `[open+1, close]` to
            // `close+1`, and an INNER/P sub-DP stops at `pi == end == close`.
            PatternItem::ContainsClose | PatternItem::WholeClose => false,
        };

        if !ok && self.use_memo {
            self.fail.insert((pi, li));
        }
        ok
    }

    /// Match a run of consecutive literal `Token`s against a single source leaf.
    ///
    /// A keyword/operator pattern token normally matches one source leaf one-to-one
    /// (`if` ⟹ `if`, `>` ⟹ a generic-close `>`). But a compound operator is split
    /// on the pattern side only (`=>` → `=` `>`; `config::detect_splittable`), while
    /// the source keeps tree-sitter's single `=>` leaf — so we accumulate consecutive
    /// pattern tokens until their concatenation equals the source leaf's text, then
    /// consume that one leaf. Stopping at the first exact equality keeps the
    /// one-to-one path (`>` then `>` over two source `>` leaves) intact; the
    /// multi-token path only engages when a single source leaf is longer (`=` `>` ⟹
    /// `=>`, or `>` `>` ⟹ a `>>` shift). A non-`Token` item or a divergent prefix
    /// ends the run.
    fn match_token_run(&mut self, pi: usize, end: usize, li: usize, hi: usize) -> bool {
        if li >= hi {
            return false;
        }
        let items = self.items;
        let target = &self.idx.leaves[li].text; // `idx` is a Copy ref → not a `self` borrow
        let mut acc = String::new();
        let mut j = pi;
        while j < end {
            let PatternItem::Token(t) = &items[j] else {
                break;
            };
            acc.push_str(t);
            if acc.len() > target.len() || !target.starts_with(&acc) {
                return false;
            }
            j += 1;
            if acc == *target {
                return self.dp(j, end, li + 1, hi);
            }
        }
        false
    }

    fn match_literal(&mut self, pi: usize, end: usize, li: usize, hi: usize, s: &str) -> bool {
        if li >= hi {
            return false;
        }
        // A string/char literal is one node whose text includes its quotes;
        // match any span with equal text (language-agnostic).
        let idx = self.idx;
        let source = self.source;
        for sp in &idx.spans_by_start[li] {
            if sp.end_leaf < hi
                && &source[sp.start_byte..sp.end_byte] == s
                && self.dp(pi + 1, end, sp.end_leaf + 1, hi)
            {
                return true;
            }
        }
        false
    }

    fn match_single(
        &mut self,
        pi: usize,
        end: usize,
        li: usize,
        hi: usize,
        name: Option<&str>,
        regex: Option<&Regex>,
    ) -> bool {
        if li >= hi {
            return false;
        }
        let idx = self.idx;
        // greedy: spans are sorted largest-first. The regex (if any) filters the
        // candidates *inside* this loop, so every nesting level that satisfies it
        // stays a live, backtrackable candidate — not just the greedy-largest.
        for sp in &idx.spans_by_start[li] {
            if sp.end_leaf >= hi {
                continue;
            }
            if !regex_ok(regex, &self.source[sp.start_byte..sp.end_byte]) {
                continue;
            }
            let cap = self.make_capture(sp.start_byte, sp.end_byte, false);
            match self.bind(name, cap) {
                BindResult::Inconsistent => continue,
                bind => {
                    if self.dp(pi + 1, end, sp.end_leaf + 1, hi) {
                        return true;
                    }
                    self.unbind(name, bind);
                }
            }
        }
        // A single-node term also matches an **anonymous leaf** (keyword/operator/
        // punctuation): it is a node like any other — a literal matches it, a `\*` run
        // spans it, and a literal alternation `\( if | while \)` matches it — so `\X`,
        // `.`, `\_`, and `\/re/` must too. (Named leaves are already covered by the spans
        // above, so this only adds the anon ones.) Tried *last*, so the greedy
        // largest-first preference for named subtrees is unchanged — the bare leaf is a
        // backtrack fallback (`\/if|while/` matches the `if` keyword; `.` still binds the
        // enclosing `if_statement` first unless the pattern forces the leaf).
        let leaf = &idx.leaves[li];
        if leaf.anon && regex_ok(regex, &self.source[leaf.start_byte..leaf.end_byte]) {
            let cap = self.make_capture(leaf.start_byte, leaf.end_byte, false);
            match self.bind(name, cap) {
                BindResult::Inconsistent => {}
                bind => {
                    if self.dp(pi + 1, end, li + 1, hi) {
                        return true;
                    }
                    self.unbind(name, bind);
                }
            }
        }
        false
    }

    /// `\(X*\)` — match a run of sibling subtrees, restricted to a contiguous run
    /// of *one parent's* direct children (same-level), so a `*` run can't
    /// silently leak out of the subtree the pattern entered. A cross-level skip
    /// is written as multiple `*`, one per grammar level.
    fn match_multi(
        &mut self,
        pi: usize,
        end: usize,
        li: usize,
        hi: usize,
        name: Option<&str>,
        nonempty: bool,
        regex: Option<&Regex>,
    ) -> bool {
        let idx = self.idx;
        // a regex run extends only over nodes each matching `re` (literal per-node).
        let re_filter = regex.map(|re| (self.source, re));
        let reach = reachable(li, hi, idx, re_filter); // descending => greedy longest first
        for next in reach {
            // `\+` (one-or-more) requires at least one node; `\*` allows the empty run
            if nonempty && next == li {
                continue;
            }
            // a same-level `*`/`+` run must be a single parent's sibling slice
            if !idx.same_level(li, next) {
                continue;
            }
            let (sb, eb) = if next > li {
                (idx.leaves[li].start_byte, idx.leaves[next - 1].end_byte)
            } else {
                let b = idx.leaves.get(li).map(|l| l.start_byte).unwrap_or(0);
                (b, b)
            };
            let cap = self.make_capture(sb, eb, true);
            match self.bind(name, cap) {
                BindResult::Inconsistent => continue,
                bind => {
                    if self.dp(pi + 1, end, next, hi) {
                        return true;
                    }
                    self.unbind(name, bind);
                }
            }
        }
        false
    }

    /// `\{ P \}` — whole-node boundary ("is", §5). `P` (`items[pi+1..close]`) must match
    /// a node starting at `li` **in full** — no leading or trailing tolerance — then the
    /// outer match continues from that node's end. So `\{ if (\X) { \Y } \}` matches an
    /// `if` with no `else` (P has to span every child). Tried largest-first and **bounded
    /// to `spans_by_start[li]`** (the nodes anchored at this leaf), so it stays O(spans·k)
    /// — no descendant scan, none of the old containment pathology. P's captures thread
    /// forward; a `bound` snapshot undoes them on a failed attempt.
    fn match_whole(&mut self, pi: usize, close: usize, end: usize, li: usize, hi: usize) -> bool {
        let inner = pi + 1; // first item of P
        let cont = close + 1; // first outer item after `\}`
        let idx = self.idx;
        let Some(spans) = idx.spans_by_start.get(li) else {
            return false;
        };
        for sp in spans {
            let next = sp.end_leaf + 1;
            if next > hi {
                continue;
            }
            let snapshot = self.bound.clone();
            // Whole-node: clear `tolerant_end` so the base case requires the match to land
            // exactly on `next` (the node's end), not at any earlier child boundary.
            let saved_tol = self.tolerant_end.take();
            let p_ok = self.dp(inner, close, li, next);
            self.tolerant_end = saved_tol;
            if p_ok && self.dp(cont, end, next, hi) {
                return true;
            }
            self.bound = snapshot; // undo P's bindings from a failed attempt
        }
        false
    }

    /// `\{{ INNER \}}` — containment (DESIGN §12). The region is a **single node**
    /// starting at `li` (a clean direct-child of the surrounding parent): require
    /// `INNER` (`items[pi+1..close]`) to match *some descendant node* inside that one
    /// node (any depth), then continue the outer match at `items[close+1..end]` from
    /// just past it. "This one node contains INNER" — the `has` intuition; the node
    /// is tried largest-first (`spans_by_start` is sorted that way).
    fn match_contains(
        &mut self,
        pi: usize,
        close: usize,
        end: usize,
        li: usize,
        hi: usize,
    ) -> bool {
        let inner = pi + 1; // first INNER item
        let cont = close + 1; // first outer item after `\}}`
        let idx = self.idx;
        // The precomputed INNER hits (present iff `no_dups`); a Copy reference so the
        // `&mut self` calls below don't conflict with this borrow.
        let cached = self.contains_cache.get(&inner);
        // `li` can be the end-exclusive position (`== leaves.len()`, e.g. a preceding
        // `\*` consumed everything up to the candidate end) — there's no node starting
        // there to bracket, so nothing contains INNER.
        let Some(spans) = idx.spans_by_start.get(li) else {
            return false;
        };
        for sp in spans {
            let next = sp.end_leaf + 1;
            // the region must be exactly one direct-child node (not a multi-child
            // span like a root `module` covering all its statements)
            if next > hi || !idx.single_child(li, next) {
                continue;
            }
            let matched = match cached {
                Some(hits) => {
                    self.contains_region_cached(inner, close, cont, end, li, next, hi, hits)
                }
                None => self.contains_then_continue(inner, close, cont, end, li, next, hi),
            };
            if matched {
                return true;
            }
        }
        false
    }

    /// Cached containment for a fixed bracketed region `[reg_lo, reg_hi)` (one node):
    /// the INNER-matching descendants are precomputed (`hits`, sorted by `start_leaf`),
    /// so we binary-search for one inside the extent rather than re-scanning. A node
    /// starting inside the extent is, by tree nesting, fully inside it — so a start in
    /// `[reg_lo, reg_hi)` is enough. Existence is sufficient because under `no_dups` the
    /// continuation can't depend on INNER's (never-reused) captures, so if it succeeds
    /// for one matching descendant it succeeds for all; we re-bind INNER on the leftmost
    /// hit only to expose its report-only captures.
    #[allow(clippy::too_many_arguments)]
    fn contains_region_cached(
        &mut self,
        inner: usize,
        inner_end: usize,
        cont: usize,
        cont_end: usize,
        reg_lo: usize,
        reg_hi: usize,
        hi: usize,
        hits: &[(u32, u32)],
    ) -> bool {
        let idx = self.idx;
        let rep = hits.partition_point(|&(s, _)| (s as usize) < reg_lo);
        if let Some(&(s, ci)) = hits.get(rep)
            && (s as usize) < reg_hi
        {
            let snapshot = self.bound.clone();
            let cand = &idx.candidates[ci as usize];
            if self.inner_matches_candidate(inner, inner_end, cand)
                && self.dp(cont, cont_end, reg_hi, hi)
            {
                return true;
            }
            self.bound = snapshot; // undo INNER bindings from a failed continuation
        }
        // INNER matches zero nodes (all-optional INNER, e.g. `\{{ \? \}}`): match the
        // empty leaf range, then continue. Not a candidate, so not in `hits`.
        let snapshot = self.bound.clone();
        if self.dp(inner, inner_end, reg_lo, reg_lo) && self.dp(cont, cont_end, reg_hi, hi) {
            return true;
        }
        self.bound = snapshot;
        false
    }

    /// For a fixed region `[reg_lo, reg_hi)`: try to match `INNER` (`items[inner..
    /// inner_end]`) against some descendant inside it, and on a hit continue the
    /// outer match (`items[cont..cont_end]`) from `reg_hi`. INNER bindings thread
    /// forward (visible after the containment); a `bound` snapshot/restore around each
    /// attempt undoes them when that attempt doesn't pan out.
    #[allow(clippy::too_many_arguments)]
    fn contains_then_continue(
        &mut self,
        inner: usize,
        inner_end: usize,
        cont: usize,
        cont_end: usize,
        reg_lo: usize,
        reg_hi: usize,
        hi: usize,
    ) -> bool {
        let idx = self.idx;
        // Descendant candidates fully inside the region, any depth. `candidates`
        // is post-order (innermost first) and leaf-dedup'd — enough for existence.
        // (MVP scans all candidates filtered by region; a region-indexed lookup is
        // a perf follow-up, in line with §10 deferring indexing.)
        for cand in &idx.candidates {
            if cand.start_leaf < reg_lo || cand.end_leaf >= reg_hi {
                continue;
            }
            // Snapshot is the *pre-containment* bindings (captures inside INNER haven't
            // happened yet) — usually empty or tiny, so the clone is cheap.
            let snapshot = self.bound.clone();
            if self.inner_matches_candidate(inner, inner_end, cand)
                && self.dp(cont, cont_end, reg_hi, hi)
            {
                return true;
            }
            self.bound = snapshot; // undo INNER bindings from a failed attempt
        }
        // INNER matches zero nodes (all-optional INNER, e.g. `\{{ \? \}}`): match
        // the empty leaf range, then continue.
        let snapshot = self.bound.clone();
        if self.dp(inner, inner_end, reg_lo, reg_lo) && self.dp(cont, cont_end, reg_hi, hi) {
            return true;
        }
        self.bound = snapshot;
        false
    }

    /// Does INNER (`items[lo..hi_items]`) match the descendant candidate `cand` as a
    /// **whole node or a valid (child-aligned) fragment** — the *same* whole-node /
    /// leading+trailing tolerance a top-level match gives, so a bare keyword
    /// `\{{ throw \}}` matches its `throw_statement`, and `\{{ fn clone(self) \}}`
    /// matches a `pub fn clone(self){}`. `cand`'s child-end boundaries become the
    /// trailing-tolerance `stops` and `tolerant_end` flags the INNER end so the base
    /// case honors them; the leading-tolerance starts are `cand`'s child-starts. (Every
    /// start/stop is a child boundary, so any match is child-aligned by construction —
    /// no ≥2-vs-anon classification needed; a single-named-child fragment is redundantly
    /// covered by that child's own candidate.)
    ///
    /// Cost: a **fresh per-descendant fail-memo** (sound because `cand`'s `stops` are
    /// fixed; on only when `no_dups`) makes the leading-tolerance starts share work —
    /// O(candidate leaves · INNER) total, the same as the old single whole-node DP. So
    /// no complexity increase, *except* the repeated-name ("backreference") case, where
    /// the memo is unsound and off → the starts cost an extra O(children) factor.
    ///
    /// On a hit INNER's bindings stay (threaded to the continuation); on a miss the DP
    /// self-unwinds (and the caller's snapshot wraps INNER + cont anyway).
    fn inner_matches_candidate(&mut self, lo: usize, hi_items: usize, cand: &Span) -> bool {
        let cand_hi = cand.end_leaf + 1;
        let stops: HashSet<usize> = cand
            .child_bounds
            .iter()
            .map(|&(_, e)| e + 1)
            .chain(std::iter::once(cand_hi))
            .collect();
        // leading-tolerance starts: the candidate's start + each child start.
        let starts: Vec<usize> = std::iter::once(cand.start_leaf)
            .chain(cand.child_bounds.iter().map(|&(s, _)| s))
            .collect();
        let saved_stops = std::mem::replace(&mut self.stops, stops);
        let saved_tol = self.tolerant_end.replace(hi_items);
        // A fresh memo for *this* descendant (its `stops` are fixed → sound); taken/
        // restored so nesting is fine. Off when the pattern repeats a name.
        let saved_fail = std::mem::take(&mut self.fail);
        let saved_use_memo = std::mem::replace(&mut self.use_memo, self.no_dups);
        let mut ok = false;
        for a in starts {
            let snap = self.bound.clone();
            if self.dp(lo, hi_items, a, cand_hi) {
                ok = true;
                break;
            }
            self.bound = snap; // undo bindings before trying the next start
        }
        self.use_memo = saved_use_memo;
        self.fail = saved_fail;
        self.stops = saved_stops;
        self.tolerant_end = saved_tol;
        ok
    }

    /// `\(X?\)` — match zero or one node. Greedy: try one node first, then none
    /// (binding an empty capture at `li`). A regex constrains the node *when
    /// present*; absence is always allowed (the `?` keeps its meaning).
    fn match_optional(
        &mut self,
        pi: usize,
        end: usize,
        li: usize,
        hi: usize,
        name: Option<&str>,
        regex: Option<&Regex>,
    ) -> bool {
        let idx = self.idx;
        // one node (greedy, largest span first)
        if li < hi {
            for sp in &idx.spans_by_start[li] {
                if sp.end_leaf >= hi {
                    continue;
                }
                if !regex_ok(regex, &self.source[sp.start_byte..sp.end_byte]) {
                    continue;
                }
                let cap = self.make_capture(sp.start_byte, sp.end_byte, false);
                match self.bind(name, cap) {
                    BindResult::Inconsistent => continue,
                    bind => {
                        if self.dp(pi + 1, end, sp.end_leaf + 1, hi) {
                            return true;
                        }
                        self.unbind(name, bind);
                    }
                }
            }
        }
        // zero nodes: empty capture, do not advance the leaf cursor
        let b = if li < idx.leaves.len() {
            idx.leaves[li].start_byte
        } else if li > 0 {
            idx.leaves[li - 1].end_byte
        } else {
            0
        };
        let cap = self.make_capture(b, b, false);
        match self.bind(name, cap) {
            BindResult::Inconsistent => false,
            bind => {
                if self.dp(pi + 1, end, li, hi) {
                    return true;
                }
                self.unbind(name, bind);
                false
            }
        }
    }

    fn make_capture(&self, start_byte: usize, end_byte: usize, multi: bool) -> Capture<'s> {
        Capture {
            text: &self.source[start_byte..end_byte],
            range: start_byte..end_byte,
            multi,
        }
    }

    /// Try to bind `name` to `cap`. Returns whether we inserted (so backtracking
    /// can restore), or `Inconsistent` if an existing binding of the same name
    /// has different text.
    fn bind(&mut self, name: Option<&str>, cap: Capture<'s>) -> BindResult {
        let Some(n) = name else {
            return BindResult::NotInserted;
        };
        match self.bound.get(n) {
            Some(existing) if existing.text != cap.text => BindResult::Inconsistent,
            Some(_) => BindResult::NotInserted,
            None => {
                self.bound.insert(n.to_string(), cap);
                BindResult::Inserted
            }
        }
    }

    fn unbind(&mut self, name: Option<&str>, bind: BindResult) {
        if let (Some(n), BindResult::Inserted) = (name, bind) {
            self.bound.remove(n);
        }
    }
}

enum BindResult {
    Inserted,
    NotInserted,
    Inconsistent,
}

/// A metavar's regex constraint (if any) against a candidate's source text. The
/// regex is whole-node anchored at compile time (`^(?:re)$`, see `lexer::lex_regex`),
/// so `is_match` here means "the *whole* text matches": `/get/` ≡ exactly `get`,
/// `/get.*/` ≡ starts with `get`.
fn regex_ok(regex: Option<&Regex>, text: &str) -> bool {
    regex.is_none_or(|re| re.is_match(text))
}

/// Positions reachable from `li` (within `[li, hi]`) by consuming whole units:
/// a complete named subtree span, or a single anonymous leaf. Returns them
/// descending so the multi-metavar binds greedily (longest first).
fn reachable(li: usize, hi: usize, idx: &Indexed, re_filter: Option<(&str, &Regex)>) -> Vec<usize> {
    let n = hi - li;
    let mut reach = vec![false; n + 1];
    reach[0] = true;
    for off in 0..n {
        if !reach[off] {
            continue;
        }
        let p = li + off;
        for sp in &idx.spans_by_start[p] {
            // `\(/re/*\)` only extends a run over nodes whose whole text matches
            // `re` (literal per-node — a non-matching node ends the run).
            if sp.end_leaf < hi
                && re_filter.is_none_or(|(src, re)| re.is_match(&src[sp.start_byte..sp.end_byte]))
            {
                reach[sp.end_leaf + 1 - li] = true;
            }
        }
        if idx.leaves[p].anon && re_filter.is_none_or(|(_, re)| re.is_match(&idx.leaves[p].text)) {
            reach[p + 1 - li] = true;
        }
    }
    let mut res: Vec<usize> = (0..=n).filter(|&o| reach[o]).map(|o| li + o).collect();
    res.sort_by(|a, b| b.cmp(a));
    res
}

#[cfg(test)]
mod tests {
    use super::Pattern;
    use tree_sitter::Parser;

    #[test]
    fn by_name_resolves_aliases() {
        assert!(crate::lang::by_name("python").is_some());
        assert!(crate::lang::by_name("PY").is_some());
        assert!(crate::lang::by_name("c++").is_some());
        assert!(crate::lang::by_name("nope").is_none());
    }

    /// One parse, many patterns — and `matches_in_tree` agrees with `matches`.
    #[test]
    fn matches_in_tree_reuses_parse() {
        let cfg = crate::lang::by_name("python").unwrap();
        let src = "def foo(a, b):\n    return a + b\n";
        let mut parser = Parser::new();
        parser.set_language(&cfg.language).unwrap();
        let tree = parser.parse(src, None).unwrap();

        let p1 = Pattern::compile(r"def \NAME(\(A*\)):", &cfg).unwrap();
        let m1 = p1.matches_in_tree(&tree, src);
        assert_eq!(m1.iter().find_map(|m| m.capture_text("NAME")), Some("foo"));

        let p2 = Pattern::compile(r"return \X + \Y", &cfg).unwrap();
        let m2 = p2.matches_in_tree(&tree, src);
        assert_eq!(m2.iter().find_map(|m| m.capture_text("X")), Some("a"));

        // identical to the self-parsing entry point
        assert_eq!(p1.matches_in_tree(&tree, src).len(), p1.matches(src).len());
    }

    /// A leaf-equivalent wrapper (a `block` whose only child is the matched
    /// `return_statement`) must not produce a duplicate match.
    #[test]
    fn dedupes_leaf_equivalent_wrappers() {
        let cfg = crate::lang::by_name("python").unwrap();
        let ms =
            crate::lang::testutil::matches(cfg, r"return \X", "def foo(a, b):\n    return a + b\n");
        let kinds: Vec<&str> = ms.iter().map(|m| m.kind).collect();
        assert_eq!(ms.len(), 1, "expected one match, got kinds {kinds:?}");
        assert_eq!(ms[0].kind, "return_statement"); // the inner node, not `block`
        assert_eq!(ms[0].capture_text("X"), Some("a + b"));
    }
}

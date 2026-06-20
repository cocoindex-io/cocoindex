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
}

/// Compiled, language-bound pattern.
pub struct Pattern {
    items: Vec<PatternItem>,
    cfg: LangConfig,
    /// Whether the `(pi, li)` fail-memo is sound. It isn't when forward-threaded
    /// bindings can change whether a `(pi, li)` matches: a repeated metavar name,
    /// or a containment group (`\{{ \}}`), whose INNER binds names per descendant.
    use_memo: bool,
}

impl Pattern {
    /// Compile a pattern for `cfg`. Fails with a `client` error on a malformed
    /// metavar matcher (e.g. an unparseable regex) or unbalanced `\{{` / `\}}`.
    pub fn compile(pattern: &str, cfg: &LangConfig) -> Result<Pattern> {
        let items = lex(pattern, cfg)?;
        let has_contains = items
            .iter()
            .any(|it| matches!(it, PatternItem::ContainsOpen { .. }));
        let use_memo = !detect_dup_names(&items) && !has_contains;
        Ok(Pattern {
            items,
            cfg: cfg.clone(),
            use_memo,
        })
    }

    /// Build a [`Prefilter`](crate::Prefilter) — the pattern's required literal
    /// content, for cheaply rejecting sources that can't match before parsing.
    /// `min_len` drops terms shorter than this many chars (default sensibly 3).
    pub fn prefilter(&self, min_len: usize) -> crate::Prefilter {
        crate::Prefilter::build(&self.items, &self.cfg, min_len)
    }

    pub fn matches<'s>(&self, source: &'s str) -> Vec<Match<'s>> {
        let mut parser = Parser::new();
        parser
            .set_language(&self.cfg.language)
            .expect("load language");
        let tree = parser.parse(source, None).expect("parse source");
        self.matches_in_tree(&tree, source)
    }

    /// Match against an already-parsed `tree`, so one compiled AST can be reused
    /// across calls (e.g. shared with the chunk splitter instead of re-parsing).
    /// The tree MUST come from the same grammar as this pattern's
    /// [`LangConfig::language`]; tree-sitter is pinned to one version
    /// workspace-wide, so a tree the splitter parsed for the same language is
    /// compatible.
    pub fn matches_in_tree<'s>(&self, tree: &Tree, source: &'s str) -> Vec<Match<'s>> {
        let idx = index_tree(tree.root_node(), source.as_bytes(), &self.cfg);

        // Run the DP over leaves `[start, hi)` with a fresh binding context;
        // return the captures on an exact match.
        let run = |start: usize, hi: usize| -> Option<HashMap<String, Capture<'s>>> {
            let mut ctx = Ctx {
                items: &self.items,
                idx: &idx,
                source,
                use_memo: self.use_memo,
                bound: HashMap::new(),
                fail: HashSet::new(),
            };
            ctx.dp(0, self.items.len(), start, hi).then_some(ctx.bound)
        };

        let mut out = Vec::new();
        for cand in &idx.candidates {
            // 1) Whole-node coverage (the pattern spans the entire candidate).
            let captures = run(cand.start_leaf, cand.end_leaf + 1).or_else(|| {
                // 2) Leading/trailing tolerance: the pattern may instead cover a
                // contiguous run of the candidate's *direct children* that spans
                // **≥2** children (`j > i`). Leading/trailing siblings — e.g. a
                // Rust `pub` and the fn body around `fn clone(self)` — are free
                // context. The ≥2 rule means a single-child run isn't matched
                // here; that child is the integral match on its own iteration
                // (so `\A = \B` matches the assignment, not the `expr;` around it).
                let kids = &cand.child_bounds;
                (0..kids.len()).find_map(|i| {
                    // Cheap prune: if the pattern starts with a literal token, the
                    // run must start at a child whose first leaf is that token —
                    // skip hopeless starts without allocating a match context.
                    // (Keeps the O(children²) partial scan from churning on the
                    // common no-match candidates during a search.)
                    if let Some(PatternItem::Token(t)) = self.items.first()
                        && &idx.leaves[kids[i].0].text != t
                    {
                        return None;
                    }
                    (i + 1..kids.len()).find_map(|j| {
                        let (a, hi) = (kids[i].0, kids[j].1 + 1);
                        // skip the whole-node run — already tried in (1)
                        if a == cand.start_leaf && hi == cand.end_leaf + 1 {
                            return None;
                        }
                        run(a, hi)
                    })
                })
            });
            if let Some(captures) = captures {
                out.push(Match {
                    kind: cand.node_kind,
                    range: cand.start_byte..cand.end_byte,
                    text: &source[cand.start_byte..cand.end_byte],
                    captures,
                });
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

fn index_tree(root: Node, src: &[u8], cfg: &LangConfig) -> Indexed {
    let mut c = Collector {
        src,
        cfg,
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
    cfg: &'a LangConfig,
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
            if self.cfg.is_splittable(&text) {
                let mut b = node.start_byte();
                for ch in text.chars() {
                    let cl = ch.len_utf8();
                    self.leaves.push(Leaf {
                        text: ch.to_string(),
                        anon: true,
                        start_byte: b,
                        end_byte: b + cl,
                    });
                    b += cl;
                }
            } else {
                self.leaves.push(Leaf {
                    text,
                    anon: !node.is_named(),
                    start_byte: node.start_byte(),
                    end_byte: node.end_byte(),
                });
            }
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
    bound: HashMap<String, Capture<'s>>,
    fail: HashSet<(usize, usize)>,
}

impl<'s> Ctx<'_, 's> {
    /// Match `items[pi..end]` against leaves `[li, hi)`. On success, `bound` holds
    /// the captures. `end` is the exclusive item bound: `items.len()` at the top
    /// level, or a containment group's `close` index for an INNER sub-match — so
    /// the same `dp` engine matches a bracketed sub-pattern without it running
    /// past its `\}}`. (`end` is a structural function of `pi` — each item sits in
    /// exactly one bracket level — so the `(pi, li)` fail-memo stays well-keyed.)
    fn dp(&mut self, pi: usize, end: usize, li: usize, hi: usize) -> bool {
        if pi == end {
            return li == hi;
        }
        if self.use_memo && self.fail.contains(&(pi, li)) {
            return false;
        }

        // Copy the shared (`Copy`) reference out of `self` so matching on it
        // doesn't borrow `self`, leaving `self` free for the `&mut self` calls.
        let items = self.items;
        let ok = match &items[pi] {
            PatternItem::Token(t) => {
                li < hi && &self.idx.leaves[li].text == t && self.dp(pi + 1, end, li + 1, hi)
            }
            PatternItem::Str(s) => self.match_literal(pi, end, li, hi, s),
            PatternItem::Meta { name, card, regex } => match card {
                Cardinality::One => {
                    self.match_single(pi, end, li, hi, name.as_deref(), regex.as_ref())
                }
                // Many ignores the regex (sibling runs are out of the single-node scope).
                Cardinality::Many => self.match_multi(pi, end, li, hi, name.as_deref()),
                Cardinality::Optional => {
                    self.match_optional(pi, end, li, hi, name.as_deref(), regex.as_ref())
                }
            },
            PatternItem::ContainsOpen { close } => self.match_contains(pi, *close, end, li, hi),
            // Never landed on: the outer DP jumps over `[open+1, close]` to
            // `close+1`, and an INNER sub-DP stops at `pi == end == close`.
            PatternItem::ContainsClose => false,
        };

        if !ok && self.use_memo {
            self.fail.insert((pi, li));
        }
        ok
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
        false
    }

    /// `\(X*)` — match a run of sibling subtrees, restricted to a contiguous run
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
    ) -> bool {
        let idx = self.idx;
        let reach = reachable(li, hi, idx); // descending => greedy longest first
        for next in reach {
            // a same-level `*` run must be a single parent's sibling slice
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

    /// `\{{ INNER \}}` — containment (DESIGN §12). Consume a same-level region
    /// `[li, next)` (a child-aligned sibling slice, like `\*`, possibly empty),
    /// require `INNER` (`items[pi+1..close]`) to match *some descendant node*
    /// fully inside the region (any depth), then continue the outer match at
    /// `items[close+1..end]` from `next`. The region grows greedily (longest
    /// first) so whole-node coverage is preferred, same as `match_multi`.
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
        for next in reachable(li, hi, idx) {
            if !idx.same_level(li, next) {
                continue; // region must be a clean sibling slice
            }
            if self.contains_then_continue(inner, close, cont, end, li, next, hi) {
                return true;
            }
        }
        false
    }

    /// For a fixed region `[reg_lo, reg_hi)`: try to match `INNER` (`items[inner..
    /// inner_end]`) against some descendant inside it, and on a hit continue the
    /// outer match (`items[cont..cont_end]`) from `reg_hi`. INNER bindings thread
    /// forward (visible after the group); a `bound` snapshot/restore around each
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
            // Snapshot is the *pre-group* bindings (captures inside INNER haven't
            // happened yet) — usually empty or tiny, so the clone is cheap.
            let snapshot = self.bound.clone();
            if self.dp(inner, inner_end, cand.start_leaf, cand.end_leaf + 1)
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

    /// `\(X?)` — match zero or one node. Greedy: try one node first, then none
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

/// A metavar's regex constraint (if any) against a candidate's source text.
/// Unanchored (`is_match`): `/get/` is a substring test, `^…$` pins the whole node.
fn regex_ok(regex: Option<&Regex>, text: &str) -> bool {
    regex.is_none_or(|re| re.is_match(text))
}

/// Positions reachable from `li` (within `[li, hi]`) by consuming whole units:
/// a complete named subtree span, or a single anonymous leaf. Returns them
/// descending so the multi-metavar binds greedily (longest first).
fn reachable(li: usize, hi: usize, idx: &Indexed) -> Vec<usize> {
    let n = hi - li;
    let mut reach = vec![false; n + 1];
    reach[0] = true;
    for off in 0..n {
        if !reach[off] {
            continue;
        }
        let p = li + off;
        for sp in &idx.spans_by_start[p] {
            if sp.end_leaf < hi {
                reach[sp.end_leaf + 1 - li] = true;
            }
        }
        if idx.leaves[p].anon {
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

        let p1 = Pattern::compile(r"def \NAME(\(A*)):", &cfg).unwrap();
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

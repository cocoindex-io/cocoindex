//! Tree-trust hazards: a pre-pass over a parsed AST detecting trees whose top
//! structure cannot be trusted (shallow parse errors) and pathologically deep
//! subtrees that recursive walks must treat as opaque.
//!
//! Accessed through [`CodeSource::tree_hazards`](crate::CodeSource::tree_hazards),
//! which memoizes the scan alongside the parse.

use crate::positions::TextRange;

/// Max AST depth for recursive consumers. Walks that recurse with tree depth
/// (structural chunking, frame extraction) would overflow worker-thread stacks
/// on pathological nesting (parser stress tests with thousands of nested
/// braces) — subtrees past this depth are reported as opaque spans instead.
const MAX_TREE_DEPTH: usize = 500;

/// A sizeable ERROR/MISSING node near the top of the tree means error recovery has
/// reshaped the file's structural skeleton (truncated classes, members leaking as
/// siblings) — structure derived from it would misstate the file.
/// Deep or zero-width errors are local hiccups consumers tolerate. Calibrated
/// on llvm/spark corpora: damaged files show >=17B errors at depth <= 3; healthy
/// files (even with thousands of deep error bytes) max out at 1B shallow.
const SHALLOW_ERROR_MAX_DEPTH: usize = 3;
const SHALLOW_ERROR_MIN_BYTES: usize = 8;

/// Pre-pass result: file-level unfitness plus local depth hazards.
pub struct TreeHazards {
    /// A shallow parse error: error recovery reshaped the tree's skeleton, so
    /// no structure derived from the tree is trustworthy.
    pub parse_error: bool,
    /// Shallowest subtrees crossing the depth limit, in source order. Recursive
    /// walks must treat them as opaque: never classified or descended into.
    pub deep_spans: Vec<TextRange>,
}

/// One iterative pre-pass: detects the file-level parse-error hazard and collects
/// local deep-subtree hazards (never descends past the depth limit).
pub(crate) fn scan_tree_hazards(root: tree_sitter::Node) -> TreeHazards {
    let mut hazards = TreeHazards {
        parse_error: root.is_error(),
        deep_spans: Vec::new(),
    };
    if hazards.parse_error {
        return hazards;
    }
    let mut cursor = root.walk();
    let mut depth = 0usize;
    loop {
        let node = cursor.node();
        if depth <= SHALLOW_ERROR_MAX_DEPTH
            && (node.is_error() || node.is_missing())
            && node.end_byte() - node.start_byte() >= SHALLOW_ERROR_MIN_BYTES
        {
            hazards.parse_error = true;
            hazards.deep_spans.clear();
            return hazards;
        }
        let block_descent = depth >= MAX_TREE_DEPTH;
        if block_descent && node.child_count() > 0 {
            hazards
                .deep_spans
                .push(TextRange::new(node.start_byte(), node.end_byte()));
        }
        if !block_descent && cursor.goto_first_child() {
            depth += 1;
            continue;
        }
        loop {
            if cursor.goto_next_sibling() {
                break;
            }
            if !cursor.goto_parent() {
                return hazards;
            }
            depth -= 1;
        }
    }
}

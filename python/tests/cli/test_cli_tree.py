"""Unit tests for tree rendering functions in the CLI module.

These tests directly test the tree building and rendering logic,
which is appropriate for unit testing implementation details.
"""

from __future__ import annotations

import pytest

from cocoindex.cli import _add_path_to_tree, _render_paths_as_tree, TreeNode
from cocoindex._internal.stable_path import ROOT_PATH


class TestTreeRendering:
    """Unit tests for tree rendering functions."""

    def test_tree_with_non_component_intermediate_nodes(self) -> None:
        """Tree rendering should correctly handle intermediate nodes that are not components."""
        # Create a scenario where we have:
        # - / (component)
        # - /"a"/"b"/"c" (component) - but /"a"/"b" is NOT a component
        # - /"a"/"b"/"d" (component) - but /"a"/"b" is NOT a component
        # - /"a"/"x" (component) - but /"a" is NOT a component

        # Manually construct paths
        path_root = ROOT_PATH
        path_abc = ROOT_PATH / "a" / "b" / "c"
        path_abd = ROOT_PATH / "a" / "b" / "d"  # Creates intermediate /"a"/"b" node
        path_ax = ROOT_PATH / "a" / "x"  # Creates intermediate /"a" node

        # Only root and leaf nodes are components (not intermediate /"a" or /"a"/"b")
        component_paths = {path_root, path_abc, path_abd, path_ax}
        all_paths = list(component_paths)

        # Build tree
        root = TreeNode(is_component=(ROOT_PATH in component_paths))
        for path in all_paths:
            _add_path_to_tree(root, path, component_paths)

        # Render tree
        output = _render_paths_as_tree(all_paths)
        lines = output.split("\n")

        # Verify root is a component
        root_line = next(
            (
                l
                for l in lines
                if l.strip() == "/" or l.strip().startswith("/ [component]")
            ),
            None,
        )
        assert root_line is not None, f"Root should be present. Output:\n{output}"
        assert "[component]" in root_line, "Root should be annotated as [component]"

        # Verify intermediate node "a" exists but is NOT a component
        a_line = None
        for line in lines:
            if "├──" in line or "└──" in line:
                parts = line.split()
                connector_idx = None
                for i, part in enumerate(parts):
                    if part in ("├──", "└──"):
                        connector_idx = i
                        break
                if connector_idx is not None and connector_idx + 1 < len(parts):
                    node_name = parts[connector_idx + 1]
                    if node_name == "a":
                        a_line = line
                        break

        assert a_line is not None, (
            f"Should have 'a' intermediate node. Output:\n{output}\nLines: {lines}"
        )
        assert "[component]" not in a_line, (
            f"Intermediate node 'a' should NOT be annotated as [component]. Line: {a_line}"
        )

        # Verify intermediate node "b" exists but is NOT a component (nested under "a")
        b_line = None
        for line in lines:
            if "├──" in line or "└──" in line:
                parts = line.split()
                connector_idx = None
                for i, part in enumerate(parts):
                    if part in ("├──", "└──"):
                        connector_idx = i
                        break
                if connector_idx is not None and connector_idx + 1 < len(parts):
                    node_name = parts[connector_idx + 1]
                    if node_name == "b":
                        b_line = line
                        break

        assert b_line is not None, (
            f"Should have 'b' intermediate node. Output:\n{output}"
        )
        assert "[component]" not in b_line, (
            f"Intermediate node 'b' should NOT be annotated as [component]. Line: {b_line}"
        )

        # Verify leaf nodes are components
        c_line = None
        x_line = None
        for line in lines:
            if "├──" in line or "└──" in line:
                parts = line.split()
                connector_idx = None
                for i, part in enumerate(parts):
                    if part in ("├──", "└──"):
                        connector_idx = i
                        break
                if connector_idx is not None and connector_idx + 1 < len(parts):
                    node_name = parts[connector_idx + 1]
                    if node_name == "c":
                        c_line = line
                    elif node_name == "x":
                        x_line = line

        assert c_line is not None, (
            f"Should have 'c' node. Output:\n{output}\nLines: {lines}"
        )
        assert x_line is not None, (
            f"Should have 'x' node. Output:\n{output}\nLines: {lines}"
        )
        assert "[component]" in c_line, (
            f"Leaf node 'c' should be annotated as [component]. Line: {c_line}"
        )
        assert "[component]" in x_line, (
            f"Leaf node 'x' should be annotated as [component]. Line: {x_line}"
        )

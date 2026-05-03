"""Neo4j connector for CocoIndex.

Public API will be re-exported from ``_target`` once that module lands. For
now only the pure Cypher generators in ``_cypher`` are in place — they have
no runtime dependency on the ``neo4j`` driver and are unit-testable in
isolation.
"""

from . import _cypher

__all__ = ["_cypher"]

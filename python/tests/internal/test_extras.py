"""A missing optional dependency names the extra that installs it."""

from __future__ import annotations

import ast
import importlib
import pathlib
import re
import sys
import tomllib

import pytest

from cocoindex._internal.extras import install_hint

_PYTHON_DIR = pathlib.Path(__file__).resolve().parents[2]
_PACKAGE_SOURCES = sorted((_PYTHON_DIR / "cocoindex").rglob("*.py"))
_EXTRAS: dict[str, list[str]] = tomllib.loads(
    (_PYTHON_DIR.parent / "pyproject.toml").read_text(encoding="utf-8")
)["project"]["optional-dependencies"]

# Connectors that import an optional dependency when they load: the connector,
# the top-level module of a dependency it imports, and the extra providing it.
# (BigQuery and Snowflake import theirs only when they connect.)
_CONNECTORS = [
    ("amazon_s3", "aiobotocore", "amazon_s3"),
    ("falkordb", "falkordb", "falkordb"),
    ("google_drive", "googleapiclient", "google_drive"),
    ("iggy", "apache_iggy", "iggy"),
    ("kafka", "confluent_kafka", "kafka"),
    ("lancedb", "lancedb", "lancedb"),
    ("neo4j", "neo4j", "neo4j"),
    ("oci_object_storage", "oci", "oci"),
    ("postgres", "asyncpg", "postgres"),
    ("qdrant", "qdrant_client", "qdrant"),
    ("surrealdb", "surrealdb", "surrealdb"),
    ("turbopuffer", "turbopuffer", "turbopuffer"),
    ("valkey", "glide", "valkey"),
    ("zvec", "zvec", "zvec"),
]


@pytest.mark.parametrize(("connector", "dependency", "extra"), _CONNECTORS)
def test_connector_without_its_dependency_names_the_extra(
    connector: str, dependency: str, extra: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    package = f"cocoindex.connectors.{connector}"
    # Import the connector afresh; monkeypatch puts the loaded modules back.
    for name in _loaded(package):
        monkeypatch.delitem(sys.modules, name)
    # None entries make importing the dependency raise ImportError. The modules
    # of it that earlier tests loaded need one too: `from pkg.mod import x` finds
    # a loaded `pkg.mod` without looking at `pkg`.
    for name in {dependency, *_loaded(dependency)}:
        monkeypatch.setitem(sys.modules, name, None)

    with pytest.raises(ImportError, match=re.escape(install_hint(extra)) + "$"):
        importlib.import_module(package)


def _loaded(package: str) -> list[str]:
    """The names of `package` and its modules, among those loaded."""
    return [m for m in sys.modules if m == package or m.startswith(package + ".")]


def test_install_hints_name_extras_from_pyproject() -> None:
    named: dict[str, object] = {}
    for path in _PACKAGE_SOURCES:
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            # Modules import the helper by name; public ones alias it with a
            # leading underscore.
            if not (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id in ("install_hint", "_install_hint")
            ):
                continue
            [arg] = [*node.args, *(keyword.value for keyword in node.keywords)]
            location = f"{path.relative_to(_PYTHON_DIR)}:{node.lineno}"
            named[location] = arg.value if isinstance(arg, ast.Constant) else None

    assert named
    assert {loc: extra for loc, extra in named.items() if extra not in _EXTRAS} == {}


def test_no_hard_coded_install_hints() -> None:
    """Hints name an extra through install_hint(), never as `cocoindex[...]`."""
    hard_coded = [
        f"{path.relative_to(_PYTHON_DIR)}:{lineno}"
        for path in _PACKAGE_SOURCES
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
        if re.search(r"\bcocoindex\[\w", line)
    ]
    assert hard_coded == []

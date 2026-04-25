#!/usr/bin/env python3
"""
Update project versions from a GitHub tag reference.

Behavior mirrors the original Bash script:
- Reads GITHUB_REF and looks for refs/tags/v<version>
- If not found, prints a message and exits 0 (no-op)
- Updates the root Cargo.toml version
- Writes python/cocoindex/_version.py with __version__
- Updates pyproject.toml with the PyPi-compatible version

Assumes current working directory is the repository root.
Works on macOS, Linux, and Windows.
"""

from __future__ import annotations

import os
import re
import sys
from collections.abc import Mapping
from pathlib import Path


TAG_PATTERN = re.compile(r"^refs/tags/v(?P<version>.+)$")
VERSION_LINE_PATTERN = re.compile(r'(?m)^(?P<prefix>\s*version\s*=\s*)"[^"]*"')
# Pattern to match dynamic = ["version"] in pyproject.toml
DYNAMIC_VERSION_PATTERN = re.compile(r'(?m)^dynamic\s*=\s*\["version"\]\s*$')

# Pattern for Rust semver pre-release: e.g., "1.0.0-alpha.1", "1.0.0-beta.2", "1.0.0-rc.1"
PRERELEASE_PATTERN = re.compile(
    r"^(?P<base>\d+\.\d+\.\d+)-(?P<prerelease>alpha|beta|rc)\.(?P<num>\d+)$"
)


def rust_version_to_pypi(version: str) -> str:
    """
    Convert Rust/Cargo semver format to PyPi-compatible format.

    Examples:
        "1.0.0" -> "1.0.0"
        "1.0.0-alpha.1" -> "1.0.0a1"
        "1.0.0-beta.2" -> "1.0.0b2"
        "1.0.0-rc.1" -> "1.0.0rc1"
    """
    match = PRERELEASE_PATTERN.match(version)
    if not match:
        # No pre-release suffix, return as-is
        return version

    base = match.group("base")
    prerelease = match.group("prerelease")
    num = match.group("num")

    # Map pre-release identifiers to PyPi format
    pypi_prerelease_map = {
        "alpha": "a",
        "beta": "b",
        "rc": "rc",
    }
    pypi_suffix = pypi_prerelease_map[prerelease]
    return f"{base}{pypi_suffix}{num}"


def extract_version_from_github_ref(env: Mapping[str, str]) -> str | None:
    ref = env.get("GITHUB_REF", "")
    match = TAG_PATTERN.match(ref)
    if not match:
        return None
    return match.group("version")


def update_cargo_version(cargo_toml_path: Path, version: str) -> bool:
    original = cargo_toml_path.read_text(encoding="utf-8")
    updated, count = VERSION_LINE_PATTERN.subn(
        rf'\g<prefix>"{version}"', original, count=1
    )
    if count == 0:
        print("Version line not found in Cargo.toml", file=sys.stderr)
        return False
    cargo_toml_path.write_text(updated, encoding="utf-8", newline="\n")
    return True


def write_python_version(
    version_file_path: Path, pypi_version: str, core_version: str
) -> None:
    """Write both PyPi version and core (Rust) version to _version.py."""
    version_file_path.parent.mkdir(parents=True, exist_ok=True)
    content = f'__version__ = "{pypi_version}"\nCORE_VERSION = "{core_version}"\n'
    version_file_path.write_text(content, encoding="utf-8", newline="\n")


def update_pyproject_version(pyproject_path: Path, version: str) -> bool:
    """Update the version in pyproject.toml by replacing dynamic = ["version"]."""
    original = pyproject_path.read_text(encoding="utf-8")
    updated, count = DYNAMIC_VERSION_PATTERN.subn(
        f'version = "{version}"', original, count=1
    )
    if count == 0:
        print('dynamic = ["version"] not found in pyproject.toml', file=sys.stderr)
        return False
    pyproject_path.write_text(updated, encoding="utf-8", newline="\n")
    return True


def main() -> int:
    version = extract_version_from_github_ref(os.environ)
    if not version:
        print("No version tag found")
        return 0

    print(f"Building release version: {version}")

    # Convert to PyPi-compatible version for Python files
    pypi_version = rust_version_to_pypi(version)
    if pypi_version != version:
        print(f"PyPi version: {pypi_version}")

    cargo_toml = Path("Cargo.toml")
    if not cargo_toml.exists():
        print(f"Cargo.toml not found at: {cargo_toml}", file=sys.stderr)
        return 1

    if not update_cargo_version(cargo_toml, version):
        return 1

    # Write PyPi-compatible version and core version to Python files
    py_version_file = Path("python") / "cocoindex" / "_version.py"
    write_python_version(py_version_file, pypi_version, core_version=version)

    pyproject_toml = Path("pyproject.toml")
    if not update_pyproject_version(pyproject_toml, pypi_version):
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

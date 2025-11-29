#!/usr/bin/env python3
"""
Update project versions from a GitHub tag reference.

Behavior mirrors the original Bash script:
- Reads GITHUB_REF and looks for refs/tags/v<version>
- If not found, prints a message and exits 0 (no-op)
- Updates the root Cargo.toml version
- Writes python/cocoindex/_version.py with __version__

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
        print(f"Version line not found in Cargo.toml", file=sys.stderr)
        return False
    cargo_toml_path.write_text(updated, encoding="utf-8", newline="\n")
    return True


def write_python_version(version_file_path: Path, version: str) -> None:
    version_file_path.parent.mkdir(parents=True, exist_ok=True)
    content = f'__version__ = "{version}"\n'
    version_file_path.write_text(content, encoding="utf-8", newline="\n")


def main() -> int:
    version = extract_version_from_github_ref(os.environ)
    if not version:
        print("No version tag found")
        return 0

    print(f"Building release version: {version}")

    cargo_toml = Path("Cargo.toml")
    if not cargo_toml.exists():
        print(f"Cargo.toml not found at: {cargo_toml}", file=sys.stderr)
        return 1

    if not update_cargo_version(cargo_toml, version):
        return 1

    py_version_file = Path("python") / "cocoindex" / "_version.py"
    write_python_version(py_version_file, version)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

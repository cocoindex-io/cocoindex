from __future__ import annotations

import sys
from ._internal import core as _core
from ._version import CORE_VERSION as _CORE_VERSION


def _sanity_check_engine() -> None:
    engine_file = getattr(_core, "__file__", "<unknown>")
    engine_version = getattr(_core, "__version__", None)

    problems: list[str] = []

    # Version mismatch (if the engine exposes its own version)
    if engine_version is not None and engine_version != _CORE_VERSION:
        problems.append(
            f"Version mismatch: Python package expects core version {_CORE_VERSION!r}, "
            f"but cocoindex._internal.core reports {engine_version!r}."
        )

    if problems:
        # Helpful diagnostic message for users
        msg_lines = [
            "Inconsistent cocoindex installation detected:",
            *[f"  - {p}" for p in problems],
            "",
            f"Python executable: {sys.executable}",
            f"cocoindex package file: {__file__}",
            f"cocoindex._engine file: {engine_file}",
            "",
            "This usually happens when:",
            "  * An old 'cocoindex._engine' .pyd is still present in the",
            "    package directory, or",
            "  * Multiple 'cocoindex' copies exist on sys.path",
            "    (e.g. a local checkout + an installed wheel).",
            "",
            "Suggested fix:",
            "  1. Uninstall cocoindex completely:",
            "       pip uninstall cocoindex",
            "  2. Reinstall it cleanly:",
            "       pip install --no-cache-dir cocoindex",
            "  3. Ensure there is no local 'cocoindex' directory or old",
            "     .pyd shadowing the installed package.",
        ]
        raise RuntimeError("\n".join(msg_lines))


_sanity_check_engine()
del _sanity_check_engine

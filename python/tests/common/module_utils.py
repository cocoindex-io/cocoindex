"""Utilities for dynamic module loading in tests."""

import importlib.util
import sys
from types import ModuleType


def load_module_as(source_path: str, fake_module_name: str) -> ModuleType:
    """
    Load a module file under a fake module name.

    This is useful for simulating in-place code changes by loading different
    source files under the same module name, which affects how the framework
    identifies the function (e.g., for memoization keys).

    Args:
        source_path: Path to the Python source file to load.
        fake_module_name: The module name to register in sys.modules.

    Returns:
        The loaded module object.
    """
    spec = importlib.util.spec_from_file_location(fake_module_name, source_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[fake_module_name] = module
    spec.loader.exec_module(module)
    return module

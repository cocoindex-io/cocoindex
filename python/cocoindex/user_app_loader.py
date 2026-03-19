import importlib
import importlib.util
import os
import sys
import types


class Error(Exception):
    """
    Exception raised when a user app target is invalid or cannot be loaded.
    """

    pass


def _find_package_root(directory: str) -> tuple[str, list[str]]:
    """
    Walk up from *directory* while each level contains ``__init__.py``.

    Returns ``(root_parent, package_parts)`` where *root_parent* is the
    directory above the topmost package and *package_parts* is the list of
    package name segments (outermost first).
    """
    parts: list[str] = []
    current = os.path.abspath(directory)
    while os.path.isfile(os.path.join(current, "__init__.py")):
        parts.append(os.path.basename(current))
        parent = os.path.dirname(current)
        if parent == current:  # filesystem root
            break
        current = parent
    parts.reverse()
    return current, parts


def _import_as_package_module(
    root_parent: str, package_parts: list[str], module_name: str
) -> types.ModuleType:
    """Import *module_name* as a submodule of the package described by *package_parts*."""
    full_module_name = ".".join(package_parts + [module_name])
    if root_parent not in sys.path:
        sys.path.insert(0, root_parent)
    try:
        return importlib.import_module(full_module_name)
    except ImportError as e:
        raise Error(f"Failed importing '{full_module_name}' from package: {e}") from e


def load_user_app(app_target: str) -> types.ModuleType:
    """
    Loads the user's application, which can be a file path or an installed module name.
    Exits on failure.
    """
    looks_like_path = os.sep in app_target or app_target.lower().endswith(".py")

    if looks_like_path:
        if not os.path.isfile(app_target):
            raise Error(f"Application file path not found: {app_target}")
        app_path = os.path.abspath(app_target)
        app_dir = os.path.dirname(app_path)
        module_name = os.path.splitext(os.path.basename(app_path))[0]

        # If the file lives inside a package (directory has __init__.py),
        # load it as a proper submodule so that relative imports work.
        if os.path.isfile(os.path.join(app_dir, "__init__.py")):
            root_parent, package_parts = _find_package_root(app_dir)
            return _import_as_package_module(root_parent, package_parts, module_name)

        if app_dir not in sys.path:
            sys.path.insert(0, app_dir)
        try:
            spec = importlib.util.spec_from_file_location(module_name, app_path)
            if spec is None:
                raise ImportError(f"Could not create spec for file: {app_path}")
            module = importlib.util.module_from_spec(spec)
            sys.modules[spec.name] = module
            if spec.loader is None:
                raise ImportError(f"Could not create loader for file: {app_path}")
            spec.loader.exec_module(module)
            return module
        except (ImportError, FileNotFoundError, PermissionError) as e:
            raise Error(f"Failed importing file '{app_path}': {e}") from e
        finally:
            if app_dir in sys.path and sys.path[0] == app_dir:
                sys.path.pop(0)

    # If the target looks like a bare module name (e.g. "main") and a
    # corresponding file exists in the CWD inside a package, load via the
    # package-qualified name so relative imports work.
    candidate_file = os.path.join(os.getcwd(), app_target + ".py")
    cwd = os.getcwd()
    if os.path.isfile(candidate_file) and os.path.isfile(
        os.path.join(cwd, "__init__.py")
    ):
        root_parent, package_parts = _find_package_root(cwd)
        return _import_as_package_module(root_parent, package_parts, app_target)

    # Try as module
    try:
        return importlib.import_module(app_target)
    except ImportError as e:
        raise Error(f"Failed to load module '{app_target}': {e}") from e
    except Exception as e:
        raise Error(f"Unexpected error importing module '{app_target}': {e}") from e

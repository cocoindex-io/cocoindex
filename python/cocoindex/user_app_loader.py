import os
import sys
import importlib.util
import types


class Error(Exception):
    """
    Exception raised when a user app target is invalid or cannot be loaded.
    """

    pass


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
        # Use "__main__" as the module name so that functions defined in the loaded
        # module have __module__ == "__main__", matching the behavior when running
        # the script directly (python main.py). This keeps memoization cache keys
        # consistent between direct execution and CLI-loaded execution.
        module_name = "__main__"

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

    # Try as module
    try:
        return importlib.import_module(app_target)
    except ImportError as e:
        raise Error(f"Failed to load module '{app_target}': {e}") from e
    except Exception as e:
        raise Error(f"Unexpected error importing module '{app_target}': {e}") from e

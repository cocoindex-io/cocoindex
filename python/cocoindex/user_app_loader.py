import os
import sys
import importlib
import click
import types


def load_user_app(app_target: str) -> types.ModuleType:
    """
    Loads the user's application, which can be a file path or an installed module name.
    Exits on failure.
    """
    if not app_target:
        raise click.ClickException("Application target not provided.")

    looks_like_path = os.sep in app_target or app_target.lower().endswith(".py")

    if looks_like_path:
        if not os.path.isfile(app_target):
            raise click.ClickException(f"Application file path not found: {app_target}")
        app_path = os.path.abspath(app_target)
        app_dir = os.path.dirname(app_path)
        module_name = os.path.splitext(os.path.basename(app_path))[0]

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
            raise click.ClickException(f"Failed importing file '{app_path}': {e}")
        finally:
            if app_dir in sys.path and sys.path[0] == app_dir:
                sys.path.pop(0)

    # Try as module
    try:
        return importlib.import_module(app_target)
    except ImportError as e:
        raise click.ClickException(f"Failed to load module '{app_target}': {e}")
    except Exception as e:
        raise click.ClickException(
            f"Unexpected error importing module '{app_target}': {e}"
        )

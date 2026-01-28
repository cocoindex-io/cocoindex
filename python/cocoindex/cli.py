import os
import sys
from typing import Any, NamedTuple
import pathlib

import click
from dotenv import find_dotenv, load_dotenv

from .user_app_loader import load_user_app, Error as UserAppLoaderError

import asyncio
import cocoindex as coco
import cocoindex.asyncio as coco_aio
from cocoindex._internal.app import AppBase
from cocoindex._internal import core as _core
from cocoindex._internal.environment import (
    Environment,
    LazyEnvironment,
    EnvironmentInfo,
    default_env,
    default_env_loop,
    get_registered_environment_infos,
)
from cocoindex._internal.setting import get_default_db_path
from cocoindex.inspect import list_stable_paths_sync


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


class AppSpecifier(NamedTuple):
    """Parsed app specifier."""

    module_ref: str
    app_name: str | None = None
    env_name: str | None = None


def _parse_app_target(specifier: str) -> AppSpecifier:
    """
    Parse 'module_or_path[:app_name[@env_name]]' into AppSpecifier.

    Examples:
        './main.py' -> AppSpecifier('./main.py', None, None)
        './main.py:app2' -> AppSpecifier('./main.py', 'app2', None)
        './main.py:app2@alpha' -> AppSpecifier('./main.py', 'app2', 'alpha')
        'mymodule:my_app@default' -> AppSpecifier('mymodule', 'my_app', 'default')
    """
    parts = specifier.split(":", 1)
    module_ref = parts[0]

    if not module_ref:
        raise click.BadParameter(
            f"Module/path part is missing in specifier: '{specifier}'. "
            "Expected format like 'myapp.py' or 'myapp.py:app_name'.",
            param_hint="APP_TARGET",
        )

    if len(parts) == 1:
        return AppSpecifier(module_ref=module_ref)

    app_part = parts[1]
    if not app_part:
        return AppSpecifier(module_ref=module_ref)

    # Parse app_name[@env_name]
    if "@" in app_part:
        app_name, env_name = app_part.split("@", 1)
        if not env_name:
            raise click.BadParameter(
                f"Environment name is missing after '@' in specifier '{specifier}'.",
                param_hint="APP_TARGET",
            )
    else:
        app_name = app_part
        env_name = None

    if app_name and not app_name.isidentifier():
        raise click.BadParameter(
            f"Invalid app name '{app_name}' in specifier '{specifier}'. "
            "App name must be a valid Python identifier.",
            param_hint="APP_TARGET",
        )

    return AppSpecifier(module_ref=module_ref, app_name=app_name, env_name=env_name)


def _get_persisted_app_names(env: Environment) -> set[str]:
    """Get the set of app names persisted in the given environment's database."""
    try:
        names = _core.list_app_names(env._core_env)
        return set(names) if names else set()
    except Exception:
        return set()


def _format_db_path(env: Environment) -> str:
    """Format the database path for display."""
    if not env.settings.db_path:
        return "(unknown)"
    path = env.settings.db_path
    try:
        cwd = os.getcwd()
        abs_path = os.path.abspath(str(path))
        if abs_path.startswith(cwd + os.sep):
            return "./" + os.path.relpath(abs_path, cwd)
        return str(path)
    except Exception:
        return str(path)


def _confirm_yes(prompt: str) -> bool:
    """Prompt user to type 'yes' explicitly. Returns True only if user types 'yes'."""
    response: str = click.prompt(prompt, default="", show_default=False)
    return response.lower() == "yes"


def _format_env_header(env_name: str, db_path: str) -> str:
    """Format the environment header for display."""
    if env_name:
        return f"{env_name} ({db_path}):"
    return f"{db_path}:"


def _print_app_group(
    env_name: str,
    db_path: str,
    apps: list[AppBase[Any, Any]],
    persisted_names: set[str],
) -> bool:
    """Print a group of apps under an environment. Returns True if any app is not persisted."""
    has_missing = False
    click.echo(_format_env_header(env_name, db_path))
    for app in sorted(apps, key=lambda a: a._name):
        if app._name in persisted_names:
            click.echo(f"  {app._name}")
        else:
            click.echo(f"  {app._name} [+]")
            has_missing = True
    return has_missing


def _ls_from_module(module_ref: str) -> None:
    """List apps from a loaded module, grouped by environment."""
    try:
        load_user_app(module_ref)
    except UserAppLoaderError as e:
        raise RuntimeError(f"Failed to load module '{module_ref}'") from e

    env_infos = get_registered_environment_infos()
    if not env_infos:
        click.echo(f"No apps are defined in '{module_ref}'.")
        return

    # Sort: explicit environments first (by name), default environment last
    def sort_key(info: EnvironmentInfo) -> tuple[int, str]:
        env = info.env
        if env is default_env():
            return (1, "")
        return (0, info.env_name or "")

    sorted_infos = sorted(env_infos, key=sort_key)

    has_missing = False
    first_group = True

    for info in sorted_infos:
        apps = info.get_apps()
        if not apps:
            continue

        env = info.env
        if env is None:
            continue

        if not first_group:
            click.echo("")
        first_group = False

        env_name = info.env_name or ""
        actual_env = env._get_env_sync()
        db_path = _format_db_path(actual_env)
        persisted_names = _get_persisted_app_names(actual_env)
        has_missing |= _print_app_group(env_name, db_path, apps, persisted_names)

    if first_group:
        click.echo(f"No apps are defined in '{module_ref}'.")
        return

    if has_missing:
        click.echo("")
        click.echo("Notes:")
        click.echo(
            "  [+]: Apps present in module, but not yet run (no persisted state)."
        )


def _ls_from_database(db_path: str) -> None:
    """List all persisted apps from a specific database."""
    import pathlib

    from cocoindex._internal.setting import Settings

    db_path_obj = pathlib.Path(db_path)
    if not db_path_obj.exists():
        raise click.ClickException(f"Database path does not exist: {db_path}")

    try:
        env = Environment(Settings(db_path=db_path_obj))
        persisted_names = _get_persisted_app_names(env)
    except Exception as e:
        raise click.ClickException(f"Failed to open database: {e}") from e

    if not persisted_names:
        click.echo("No persisted apps found in the database.")
        return

    formatted_path = _format_db_path(env)
    click.echo(f"{formatted_path}:")
    for name in sorted(persisted_names):
        click.echo(f"  {name}")


def _load_app(app_target: str) -> AppBase[Any, Any]:
    """
    Load an app from a specifier.

    Supports formats:
        - 'path/to/app.py' - loads the only registered app
        - 'path/to/app.py:app_name' - loads the app with 'app_name'
        - 'path/to/app.py:app_name@env_name' - loads the app with 'app_name' in environment 'env_name'
    """
    spec = _parse_app_target(app_target)

    try:
        load_user_app(spec.module_ref)
    except UserAppLoaderError as e:
        raise RuntimeError(f"Failed to load module '{spec.module_ref}'") from e

    # Get target environments (filter by env_name if specified)
    env_infos = get_registered_environment_infos()
    if spec.env_name:
        env_infos = [info for info in env_infos if info.env_name == spec.env_name]
        if not env_infos:
            raise click.ClickException(
                f"No environment named '{spec.env_name}' found after loading '{spec.module_ref}'."
            )

    # Get all apps from target environments
    apps: list[AppBase[Any, Any]] = []
    for info in env_infos:
        apps.extend(info.get_apps())

    # Filter by app name if specified
    if spec.app_name:
        matching = [a for a in apps if a._name == spec.app_name]
        if not matching:
            available = ", ".join(sorted(set(a._name for a in apps))) or "none"
            raise click.ClickException(
                f"No app named '{spec.app_name}' found after loading '{spec.module_ref}'. "
                f"Available apps: {available}"
            )

        if len(matching) > 1:
            # Multiple apps with the same name in different environments
            available_envs = ", ".join(
                a._environment.name or "(unnamed)" for a in matching
            )
            raise click.ClickException(
                f"Multiple apps named '{spec.app_name}' found in different environments: {available_envs}. "
                f"Please specify environment with ':app_name@env_name' syntax."
            )
        app = matching[0]
    else:
        # No app name specified
        if len(apps) == 1:
            app = apps[0]
        elif len(apps) > 1:
            available = ", ".join(sorted(set(a._name for a in apps)))
            raise click.ClickException(
                f"Multiple apps found in '{spec.module_ref}': {available}. "
                "Please specify which app to use with ':app_name' syntax."
            )
        else:
            raise click.ClickException(
                f"No apps found after loading '{spec.module_ref}'. "
                "Make sure the module creates a coco.App(...) instance."
            )

    return app


def _create_project_files(project_name: str, project_dir: str) -> None:
    """Create project files for a new CocoIndex project."""

    project_path = pathlib.Path(project_dir)
    project_path.mkdir(parents=True, exist_ok=True)

    # Create main.py
    main_py_content = f'''"""CocoIndex app template."""
import pathlib
from typing import Iterator

import cocoindex as coco


@coco.lifespan
def coco_lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
    """Configure the CocoIndex environment."""
    builder.settings.db_path = pathlib.Path("./cocoindex.db")
    yield


@coco.function
def app_main() -> None:
    """Define your main pipeline here.

    Common pattern:
      1) Declare targets/target states under stable 'setup/...' paths.
      2) Enumerate inputs (files, DB rows, etc.).
      3) Mount per input processing unit using a stable path.

    Note: app_main can accept parameters (e.g., sourcedir/outdir) passed via coco.App(...)
    """

    # 1) Declare targets/target states
    # Example (local filesystem):
    #   target = coco.mount_run(
    #       coco.component_subpath("setup"),
    #       localfs.declare_dir_target,
    #       outdir,
    #   ).result()

    # 2) Enumerate inputs
    # Example (walk a directory):
    #   files = localfs.walk_dir(
    #       sourcedir,
    #       path_matcher=PatternFilePathMatcher(included_patterns=["*.pdf"]),
    #   )

    # 3) Mount a processing unit for each input under a stable path
    # Example:
    #   for f in files:
    #       coco.mount(
    #           coco.component_subpath("process", str(f.relative_path)),
    #           process_file_function,
    #           f,
    #           target,
    #       )

    pass


app = coco.App(
    coco.AppConfig(name="{project_name}"),
    app_main,
)
'''
    (project_path / "main.py").write_text(main_py_content)

    # Create pyproject.toml
    pyproject_toml_content = f"""[project]
name = "{project_name}"
version = "0.1.0"
description = "A CocoIndex application"
requires-python = ">=3.11"
dependencies = [
    "cocoindex>={coco.__version__}",
]

[tool.uv]
prerelease = "explicit"
"""
    (project_path / "pyproject.toml").write_text(pyproject_toml_content)

    # Create README.md
    readme_content = f"""# {project_name}

A CocoIndex application.

## Getting Started

Run the app:
```bash
uv run cocoindex update main.py
```

## Project Structure

- `main.py` - Main application file with your CocoIndex app definition
- `pyproject.toml` - Project metadata and dependencies
"""
    (project_path / "README.md").write_text(readme_content)


# ---------------------------------------------------------------------------
# CLI group
# ---------------------------------------------------------------------------


@click.group()
@click.version_option(
    None,
    "-V",
    "--version",
    package_name="cocoindex",
    message="%(prog)s version %(version)s",
)
@click.option(
    "-e",
    "--env-file",
    type=click.Path(
        exists=True, file_okay=True, dir_okay=False, readable=True, resolve_path=True
    ),
    help="Path to a .env file to load environment variables from. "
    "If not provided, attempts to load '.env' from the current directory.",
    default=None,
    show_default=False,
)
@click.option(
    "-d",
    "--app-dir",
    help="Load apps from the specified directory. Default to the current directory.",
    default="",
    show_default=True,
)
def cli(env_file: str | None = None, app_dir: str | None = "") -> None:
    """CLI for CocoIndex."""
    dotenv_path = env_file or find_dotenv(usecwd=True)

    if load_dotenv(dotenv_path=dotenv_path):
        loaded_env_path = os.path.abspath(dotenv_path)
        click.echo(f"Loaded environment variables from: {loaded_env_path}\n", err=True)

    if app_dir is not None:
        sys.path.insert(0, app_dir)


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


@cli.command()
@click.argument("app_target", type=str, required=False)
@click.option(
    "--db",
    type=str,
    default=None,
    help="Path to database to list apps from (only used when APP_TARGET is not specified).",
)
def ls(app_target: str | None, db: str | None) -> None:
    """
    List all apps.

    If `APP_TARGET` (`path/to/app.py` or `module`) is provided, lists apps defined in that module and their persisted status, grouped by environment.

    If `APP_TARGET` is omitted and `--db` is provided, lists all apps from the specified database.
    """
    if app_target:
        if db:
            click.echo(
                "Warning: --db is ignored when APP_TARGET is specified.", err=True
            )
        spec = _parse_app_target(app_target)
        _ls_from_module(spec.module_ref)
    elif db:
        _ls_from_database(db)
    else:
        # Try to use default db path from environment variable
        default_db = get_default_db_path()
        if default_db:
            _ls_from_database(str(default_db))
        else:
            raise click.ClickException(
                "Please specify either APP_TARGET or --db option "
                "(or set COCOINDEX_DB environment variable).\n"
                "  cocoindex ls ./app.py        # List apps from module\n"
                "  cocoindex ls --db ./my.db    # List apps from database"
            )


@cli.command()
@click.argument("app_target", type=str)
def show(app_target: str) -> None:
    """
    Show the app's stable paths.

    `APP_TARGET`: `path/to/app.py`, `module`, `path/to/app.py:app_name`, or `module:app_name`.
    """
    app = _load_app(app_target)
    paths = list_stable_paths_sync(app)
    click.echo(f"Found {len(paths)} stable paths:")
    for path in paths:
        click.echo(f"  {path}")


async def _stop_all_environments() -> None:
    for env_info in get_registered_environment_infos():
        env = env_info.env
        if isinstance(env, LazyEnvironment):
            await env.stop()


async def _update_app(app: AppBase[Any, Any], *args: Any, **kwargs: Any) -> Any:
    if isinstance(app, coco_aio.App):
        return await app.update(*args, **kwargs)
    if isinstance(app, coco.App):
        return await asyncio.to_thread(app.update, *args, **kwargs)
    raise ValueError(f"Invalid app: {app}. Expected coco.App or coco_aio.App.")


async def _drop_app(app: AppBase[Any, Any], *args: Any, **kwargs: Any) -> None:
    if isinstance(app, coco_aio.App):
        await app.drop(*args, **kwargs)
        return
    if isinstance(app, coco.App):
        await asyncio.to_thread(app.drop, *args, **kwargs)
        return
    raise ValueError(f"Invalid app: {app}. Expected coco.App or coco_aio.App.")


@cli.command()
@click.argument("app_target", type=str)
def update(app_target: str) -> None:
    """
    Run a v1 app once (one-time update).

    `APP_TARGET`: `path/to/app.py`, `module`, `path/to/app.py:app_name`, or `module:app_name`.
    """
    app = _load_app(app_target)

    async def _do() -> None:
        try:
            env = await app._environment._get_env()
            print(
                f"Running app '{app._name}' from environment '{env.name}' (db path: {env.settings.db_path})"
            )
            await _update_app(app, report_to_stdout=True)
        finally:
            await _stop_all_environments()

    env_loop = default_env_loop()
    asyncio.run_coroutine_threadsafe(_do(), env_loop).result()


@cli.command()
@click.argument("app_target", type=str)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Skip confirmation prompt.",
)
def drop(app_target: str, force: bool = False) -> None:
    """
    Drop an app and all its target states.

    This will:

    \b
    - Revert all target states created by the app (e.g., drop tables, delete rows)
    - Clear the app's internal state database

    `APP_TARGET`: `path/to/app.py`, `module`, `path/to/app.py:app_name`, or `module:app_name`.
    """
    app = _load_app(app_target)

    # Get the actual environment to check persisted state
    env = app._environment._get_env_sync()
    persisted_names = _get_persisted_app_names(env)

    click.echo(
        f"Preparing to drop app '{app._name}' from environment '{env.name}' (db path: {env.settings.db_path})"
    )

    if app._name not in persisted_names:
        click.echo(f"App '{app._name}' has no persisted state. Nothing to drop.")
        return

    if not force:
        if not _confirm_yes(
            f"Type 'yes' to drop app '{app._name}' and all its target states"
        ):
            click.echo("Drop operation aborted.")
            return

    async def _do() -> None:
        try:
            await _drop_app(app, report_to_stdout=True)
        finally:
            await _stop_all_environments()
        click.echo(
            f"Dropped app '{app._name}' from environment '{env.name}' and reverted its target states."
        )

    env_loop = default_env_loop()
    asyncio.run_coroutine_threadsafe(_do(), env_loop).result()


@cli.command()
@click.argument("project_name", type=str, required=False)
@click.option(
    "--dir",
    type=click.Path(file_okay=False, dir_okay=True, writable=True),
    default=None,
    help="Directory to create the project in.",
)
def init(project_name: str | None, dir: str | None) -> None:
    """
    Initialize a new CocoIndex project.

    Creates a new project directory with starter files:
    1. main.py (Main application file)
    2. pyproject.toml (Project metadata and dependencies)
    3. README.md (Quick start guide)

    `PROJECT_NAME`: Name of the project (defaults to current directory name if not specified).
    """
    # Determine project directory
    if dir:
        project_dir = dir
        if not project_name:
            project_name = pathlib.Path(dir).resolve().name
    elif project_name:
        project_dir = project_name
    else:
        # Use current directory
        project_dir = "."
        project_name = pathlib.Path.cwd().resolve().name

    # Validate project name
    if project_name and not project_name.replace("_", "").replace("-", "").isalnum():
        raise click.BadParameter(
            f"Invalid project name '{project_name}'. "
            "Project name must contain only alphanumeric characters, hyphens, and underscores.",
            param_hint="PROJECT_NAME",
        )

    project_path = pathlib.Path(project_dir)

    # Check if directory exists and has files
    if project_path.exists() and any(project_path.iterdir()):
        if not click.confirm(
            f"Directory '{project_dir}' already exists and is not empty. "
            "Continue and overwrite existing files?"
        ):
            click.echo("Init cancelled.")
            return

    try:
        _create_project_files(project_name, project_dir)
        click.echo(f"Created CocoIndex project '{project_name}' in '{project_dir}'")
        click.echo("\nNext steps:")
        if project_dir != ".":
            click.echo(f"  1. cd {project_dir}")
            click.echo("  2. uv run cocoindex update main.py")
        else:
            click.echo("  1. uv run cocoindex update main.py")
    except Exception as e:
        raise click.ClickException(f"Failed to create project: {e}") from e


if __name__ == "__main__":
    cli()

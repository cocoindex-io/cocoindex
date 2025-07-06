import atexit
import asyncio
import datetime
import importlib.util
import os
import signal
import sys
import threading
import types
from types import FrameType
from typing import Any, Iterable

import click
import watchfiles
from dotenv import find_dotenv, load_dotenv
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from . import flow, lib, setting
from .setup import flow_names_with_setup
from .runtime import execution_context

# Create ServerSettings lazily upon first call, as environment variables may be loaded from files, etc.
COCOINDEX_HOST = "https://cocoindex.io"


def _parse_app_flow_specifier(specifier: str) -> tuple[str, str | None]:
    """Parses 'module_or_path[:flow_name]' into (module_or_path, flow_name | None)."""
    parts = specifier.split(":", 1)  # Split only on the first colon
    app_ref = parts[0]

    if not app_ref:
        raise click.BadParameter(
            f"Application module/path part is missing or invalid in specifier: '{specifier}'. "
            "Expected format like 'myapp.py' or 'myapp:MyFlow'.",
            param_hint="APP_SPECIFIER",
        )

    if len(parts) == 1:
        return app_ref, None

    flow_ref_part = parts[1]

    if not flow_ref_part:  # Handles empty string after colon
        return app_ref, None

    if not flow_ref_part.isidentifier():
        raise click.BadParameter(
            f"Invalid format for flow name part ('{flow_ref_part}') in specifier '{specifier}'. "
            "If a colon separates the application from the flow name, the flow name should typically be "
            "a valid identifier (e.g., alphanumeric with underscores, not starting with a number).",
            param_hint="APP_SPECIFIER",
        )
    return app_ref, flow_ref_part


def _get_app_ref_from_specifier(
    specifier: str,
) -> str:
    """
    Parses the APP_TARGET to get the application reference (path or module).
    Issues a warning if a flow name component is also provided in it.
    """
    app_ref, flow_ref = _parse_app_flow_specifier(specifier)

    if flow_ref is not None:
        click.echo(
            click.style(
                f"Ignoring flow name '{flow_ref}' in '{specifier}': "
                f"this command operates on the entire app/module '{app_ref}'.",
                fg="yellow",
            ),
            err=True,
        )
    return app_ref


def _load_user_app(app_target: str) -> types.ModuleType:
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


def _initialize_cocoindex_in_process() -> None:
    settings = setting.Settings.from_env()
    lib.init(settings)
    atexit.register(lib.stop)


@click.group()
@click.version_option(package_name="cocoindex", message="%(prog)s version %(version)s")
@click.option(
    "--env-file",
    type=click.Path(
        exists=True, file_okay=True, dir_okay=False, readable=True, resolve_path=True
    ),
    help="Path to a .env file to load environment variables from. "
    "If not provided, attempts to load '.env' from the current directory.",
    default=None,
    show_default=False,
)
def cli(env_file: str | None = None) -> None:
    """
    CLI for Cocoindex.
    """
    dotenv_path = env_file or find_dotenv(usecwd=True)

    if load_dotenv(dotenv_path=dotenv_path):
        loaded_env_path = os.path.abspath(dotenv_path)
        click.echo(f"Loaded environment variables from: {loaded_env_path}\n", err=True)

    try:
        _initialize_cocoindex_in_process()
    except Exception as e:
        raise click.ClickException(f"Failed to initialize CocoIndex library: {e}")


@cli.command()
@click.argument("app_target", type=str, required=False)
def ls(app_target: str | None) -> None:
    """
    List all flows.

    If APP_TARGET (path/to/app.py or a module) is provided, lists flows
    defined in the app and their backend setup status.

    If APP_TARGET is omitted, lists all flows that have a persisted
    setup in the backend.
    """
    persisted_flow_names = flow_names_with_setup()
    if app_target:
        app_ref = _get_app_ref_from_specifier(app_target)
        _load_user_app(app_ref)

        current_flow_names = set(flow.flow_names())

        if not current_flow_names:
            click.echo(f"No flows are defined in '{app_ref}'.")
            return

        has_missing = False
        persisted_flow_names_set = set(persisted_flow_names)
        for name in sorted(current_flow_names):
            if name in persisted_flow_names_set:
                click.echo(name)
            else:
                click.echo(f"{name} [+]")
                has_missing = True

        if has_missing:
            click.echo("")
            click.echo("Notes:")
            click.echo(
                "  [+]: Flows present in the current process, but missing setup."
            )

    else:
        if not persisted_flow_names:
            click.echo("No persisted flow setups found in the backend.")
            return

        for name in sorted(persisted_flow_names):
            click.echo(name)


@cli.command()
@click.argument("app_flow_specifier", type=str)
@click.option(
    "--color/--no-color", default=True, help="Enable or disable colored output."
)
@click.option("--verbose", is_flag=True, help="Show verbose output with full details.")
def show(app_flow_specifier: str, color: bool, verbose: bool) -> None:
    """
    Show the flow spec and schema.

    APP_FLOW_SPECIFIER: Specifies the application and optionally the target flow.
    Can be one of the following formats:

    \b
      - path/to/your_app.py
      - an_installed.module_name
      - path/to/your_app.py:SpecificFlowName
      - an_installed.module_name:SpecificFlowName

    :SpecificFlowName can be omitted only if the application defines a single flow.
    """
    app_ref, flow_ref = _parse_app_flow_specifier(app_flow_specifier)
    _load_user_app(app_ref)

    fl = _flow_by_name(flow_ref)
    console = Console(no_color=not color)
    console.print(fl._render_spec(verbose=verbose))
    console.print()
    table = Table(
        title=f"Schema for Flow: {fl.name}",
        title_style="cyan",
        header_style="bold magenta",
    )
    table.add_column("Field", style="cyan")
    table.add_column("Type", style="green")
    table.add_column("Attributes", style="yellow")
    for field_name, field_type, attr_str in fl._get_schema():
        table.add_row(field_name, field_type, attr_str)
    console.print(table)


def _setup_flows(
    flow_iter: Iterable[flow.Flow],
    *,
    force: bool,
    quiet: bool = False,
    always_show_setup: bool = False,
) -> None:
    setup_bundle = flow.make_setup_bundle(flow_iter)
    description, is_up_to_date = setup_bundle.describe()
    if always_show_setup or not is_up_to_date:
        click.echo(description)
    if is_up_to_date:
        if not quiet:
            click.echo("Setup is already up to date.")
        return
    if not force and not click.confirm(
        "Changes need to be pushed. Continue? [yes/N]",
        default=False,
        show_default=False,
    ):
        return
    setup_bundle.apply(report_to_stdout=not quiet)


def _show_no_live_update_hint() -> None:
    click.secho(
        "NOTE: No change capture mechanism exists. See https://cocoindex.io/docs/core/flow_methods#live-update for more details.\n",
        fg="yellow",
    )


async def _update_all_flows_with_hint_async(
    options: flow.FlowLiveUpdaterOptions,
) -> None:
    await flow.update_all_flows_async(options)
    if options.live_mode:
        _show_no_live_update_hint()


@cli.command()
@click.argument("app_target", type=str)
@click.option(
    "-f",
    "--force",
    is_flag=True,
    show_default=True,
    default=False,
    help="Force setup without confirmation prompts.",
)
def setup(app_target: str, force: bool) -> None:
    """
    Check and apply backend setup changes for flows, including the internal storage and target (to export to).

    APP_TARGET: path/to/app.py or installed_module.
    """
    app_ref = _get_app_ref_from_specifier(app_target)
    _load_user_app(app_ref)
    _setup_flows(flow.flows().values(), force=force, always_show_setup=True)


@cli.command("drop")
@click.argument("app_target", type=str, required=False)
@click.argument("flow_name", type=str, nargs=-1)
@click.option(
    "-f",
    "--force",
    is_flag=True,
    show_default=True,
    default=False,
    help="Force drop without confirmation prompts.",
)
def drop(app_target: str | None, flow_name: tuple[str, ...], force: bool) -> None:
    """
    Drop the backend setup for flows.

    \b
    Modes of operation:
    1. Drop all flows defined in an app: `cocoindex drop <APP_TARGET>`
    2. Drop specific named flows: `cocoindex drop <APP_TARGET> [FLOW_NAME...]`
    """
    app_ref = None

    if not app_target:
        raise click.UsageError(
            "Missing arguments. You must either provide an APP_TARGET (to target app-specific flows) "
            "or use the --all flag."
        )

    app_ref = _get_app_ref_from_specifier(app_target)
    _load_user_app(app_ref)

    flows: Iterable[flow.Flow]
    if flow_name:
        flows = []
        for name in flow_name:
            try:
                flows.append(flow.flow_by_name(name))
            except KeyError:
                click.echo(
                    f"Warning: Failed to get flow `{name}`. Ignored.",
                    err=True,
                )
    else:
        flows = flow.flows().values()

    flow_full_names = ", ".join(fl.full_name for fl in flows)
    click.echo(
        f"Preparing to drop specified flows: {flow_full_names} (in '{app_ref}').",
        err=True,
    )

    if not flows:
        click.echo("No flows identified for the drop operation.")
        return

    setup_bundle = flow.make_drop_bundle(flows)
    description, is_up_to_date = setup_bundle.describe()
    click.echo(description)
    if is_up_to_date:
        click.echo("No flows need to be dropped.")
        return
    if not force and not click.confirm(
        f"\nThis will apply changes to drop setup for: {flow_full_names}. Continue? [yes/N]",
        default=False,
        show_default=False,
    ):
        click.echo("Drop operation aborted by user.")
        return
    setup_bundle.apply(report_to_stdout=True)


@cli.command()
@click.argument("app_flow_specifier", type=str)
@click.option(
    "-L",
    "--live",
    is_flag=True,
    show_default=True,
    default=False,
    help="Continuously watch changes from data sources and apply to the target index.",
)
@click.option(
    "--setup",
    is_flag=True,
    show_default=True,
    default=False,
    help="Automatically setup backends for the flow if it's not setup yet.",
)
@click.option(
    "-f",
    "--force",
    is_flag=True,
    show_default=True,
    default=False,
    help="Force setup without confirmation prompts.",
)
@click.option(
    "-q",
    "--quiet",
    is_flag=True,
    show_default=True,
    default=False,
    help="Avoid printing anything to the standard output, e.g. statistics.",
)
def update(
    app_flow_specifier: str,
    live: bool,
    setup: bool,  # pylint: disable=redefined-outer-name
    force: bool,
    quiet: bool,
) -> None:
    """
    Update the index to reflect the latest data from data sources.

    APP_FLOW_SPECIFIER: path/to/app.py, module, path/to/app.py:FlowName, or module:FlowName.
    If :FlowName is omitted, updates all flows.
    """
    app_ref, flow_name = _parse_app_flow_specifier(app_flow_specifier)
    _load_user_app(app_ref)

    if live:
        click.secho(
            "NOTE: Flow code changes will NOT be reflected until you restart to load the new code.\n",
            fg="yellow",
        )

    options = flow.FlowLiveUpdaterOptions(live_mode=live, print_stats=not quiet)
    if flow_name is None:
        if setup:
            _setup_flows(
                flow.flows().values(),
                force=force,
                quiet=quiet,
            )
        execution_context.run(_update_all_flows_with_hint_async(options))
    else:
        fl = flow.flow_by_name(flow_name)
        if setup:
            _setup_flows((fl,), force=force, quiet=quiet)
        with flow.FlowLiveUpdater(fl, options) as updater:
            updater.wait()
            if options.live_mode:
                _show_no_live_update_hint()


@cli.command()
@click.argument("app_flow_specifier", type=str)
@click.option(
    "-o",
    "--output-dir",
    type=str,
    required=False,
    help="The directory to dump the output to.",
)
@click.option(
    "--cache/--no-cache",
    is_flag=True,
    show_default=True,
    default=True,
    help="Use already-cached intermediate data if available.",
)
def evaluate(
    app_flow_specifier: str, output_dir: str | None, cache: bool = True
) -> None:
    """
    Evaluate the flow and dump flow outputs to files.

    Instead of updating the index, it dumps what should be indexed to files.
    Mainly used for evaluation purpose.

    \b
    APP_FLOW_SPECIFIER: Specifies the application and optionally the target flow.
    Can be one of the following formats:
      - path/to/your_app.py
      - an_installed.module_name
      - path/to/your_app.py:SpecificFlowName
      - an_installed.module_name:SpecificFlowName

    :SpecificFlowName can be omitted only if the application defines a single flow.
    """
    app_ref, flow_ref = _parse_app_flow_specifier(app_flow_specifier)
    _load_user_app(app_ref)

    fl = _flow_by_name(flow_ref)
    if output_dir is None:
        output_dir = f"eval_{setting.get_app_namespace(trailing_delimiter='_')}{fl.name}_{datetime.datetime.now().strftime('%y%m%d_%H%M%S')}"
    options = flow.EvaluateAndDumpOptions(output_dir=output_dir, use_cache=cache)
    fl.evaluate_and_dump(options)


@cli.command()
@click.argument("app_target", type=str)
@click.option(
    "-a",
    "--address",
    type=str,
    help="The address to bind the server to, in the format of IP:PORT. "
    "If unspecified, the address specified in COCOINDEX_SERVER_ADDRESS will be used.",
)
@click.option(
    "-c",
    "--cors-origin",
    type=str,
    help="The origins of the clients (e.g. CocoInsight UI) to allow CORS from. "
    "Multiple origins can be specified as a comma-separated list. "
    "e.g. `https://cocoindex.io,http://localhost:3000`. "
    "Origins specified in COCOINDEX_SERVER_CORS_ORIGINS will also be included.",
)
@click.option(
    "-ci",
    "--cors-cocoindex",
    is_flag=True,
    show_default=True,
    default=False,
    help=f"Allow {COCOINDEX_HOST} to access the server.",
)
@click.option(
    "-cl",
    "--cors-local",
    type=int,
    help="Allow http://localhost:<port> to access the server.",
)
@click.option(
    "-L",
    "--live-update",
    is_flag=True,
    show_default=True,
    default=False,
    help="Continuously watch changes from data sources and apply to the target index.",
)
@click.option(
    "--setup",
    is_flag=True,
    show_default=True,
    default=False,
    help="Automatically setup backends for the flow if it's not setup yet.",
)
@click.option(
    "-f",
    "--force",
    is_flag=True,
    show_default=True,
    default=False,
    help="Force setup without confirmation prompts.",
)
@click.option(
    "-q",
    "--quiet",
    is_flag=True,
    show_default=True,
    default=False,
    help="Avoid printing anything to the standard output, e.g. statistics.",
)
@click.option(
    "-r",
    "--reload",
    is_flag=True,
    show_default=True,
    default=False,
    help="Enable auto-reload on code changes.",
)
def server(
    app_target: str,
    address: str | None,
    live_update: bool,
    setup: bool,  # pylint: disable=redefined-outer-name
    force: bool,
    quiet: bool,
    cors_origin: str | None,
    cors_cocoindex: bool,
    cors_local: int | None,
    reload: bool,
) -> None:
    """
    Start a HTTP server providing REST APIs.

    It will allow tools like CocoInsight to access the server.

    APP_TARGET: path/to/app.py or installed_module.
    """
    app_ref = _get_app_ref_from_specifier(app_target)
    args = (
        app_ref,
        address,
        cors_origin,
        cors_cocoindex,
        cors_local,
        live_update,
        setup,
        force,
        quiet,
    )

    if reload:
        watch_paths = {os.getcwd()}
        if os.path.isfile(app_ref):
            watch_paths.add(os.path.dirname(os.path.abspath(app_ref)))
        else:
            try:
                spec = importlib.util.find_spec(app_ref)
                if spec and spec.origin:
                    watch_paths.add(os.path.dirname(os.path.abspath(spec.origin)))
            except ImportError:
                pass

        watchfiles.run_process(
            *watch_paths,
            target=_reloadable_server_target,
            args=args,
            watch_filter=watchfiles.PythonFilter(),
            callback=lambda changes: click.secho(
                f"\nDetected changes in {len(changes)} file(s), reloading server...\n",
                fg="cyan",
            ),
        )
    else:
        click.secho(
            "NOTE: Flow code changes will NOT be reflected until you restart to load the new code. Use --reload to enable auto-reload.\n",
            fg="yellow",
        )
        _run_server(*args)


def _reloadable_server_target(*args: Any, **kwargs: Any) -> None:
    """Reloadable target for the watchfiles process."""
    _initialize_cocoindex_in_process()
    _run_server(*args, **kwargs)


def _run_server(
    app_ref: str,
    address: str | None = None,
    cors_origin: str | None = None,
    cors_cocoindex: bool = False,
    cors_local: int | None = None,
    live_update: bool = False,
    run_setup: bool = False,
    force: bool = False,
    quiet: bool = False,
) -> None:
    """Helper function to run the server with specified settings."""
    _load_user_app(app_ref)

    server_settings = setting.ServerSettings.from_env()
    cors_origins: set[str] = set(server_settings.cors_origins or [])
    if cors_origin is not None:
        cors_origins.update(setting.ServerSettings.parse_cors_origins(cors_origin))
    if cors_cocoindex:
        cors_origins.add(COCOINDEX_HOST)
    if cors_local is not None:
        cors_origins.add(f"http://localhost:{cors_local}")
    server_settings.cors_origins = list(cors_origins)

    if address is not None:
        server_settings.address = address

    if run_setup:
        _setup_flows(
            flow.flows().values(),
            force=force,
            quiet=quiet,
        )

    lib.start_server(server_settings)

    if COCOINDEX_HOST in cors_origins:
        click.echo(f"Open CocoInsight at: {COCOINDEX_HOST}/cocoinsight")

    click.secho("Press Ctrl+C to stop the server.", fg="yellow")

    if live_update:
        options = flow.FlowLiveUpdaterOptions(live_mode=True, print_stats=not quiet)
        asyncio.run_coroutine_threadsafe(
            _update_all_flows_with_hint_async(options), execution_context.event_loop
        )

    shutdown_event = threading.Event()

    def handle_signal(signum: int, frame: FrameType | None) -> None:
        shutdown_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    shutdown_event.wait()


def _flow_name(name: str | None) -> str:
    names = flow.flow_names()
    available = ", ".join(sorted(names))
    if name is not None:
        if name not in names:
            raise click.BadParameter(
                f"Flow '{name}' not found.\nAvailable: {available if names else 'None'}"
            )
        return name
    if len(names) == 0:
        raise click.UsageError("No flows available in the loaded application.")
    elif len(names) == 1:
        return names[0]
    else:
        console = Console()
        index = 0

        while True:
            console.clear()
            console.print(
                Panel.fit("Select a Flow", title_align="left", border_style="cyan")
            )
            for i, fname in enumerate(names):
                console.print(
                    f"> [bold green]{fname}[/bold green]"
                    if i == index
                    else f"  {fname}"
                )

            key = click.getchar()
            if key == "\x1b[A":  # Up arrow
                index = (index - 1) % len(names)
            elif key == "\x1b[B":  # Down arrow
                index = (index + 1) % len(names)
            elif key in ("\r", "\n"):  # Enter
                console.clear()
                return names[index]


def _flow_by_name(name: str | None) -> flow.Flow:
    return flow.flow_by_name(_flow_name(name))


if __name__ == "__main__":
    cli()

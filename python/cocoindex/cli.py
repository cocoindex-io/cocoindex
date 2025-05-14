import click
import datetime
import sys
import importlib.util
import os
import atexit

from rich.console import Console
from rich.table import Table

from . import flow, lib, setting, query
from .setup import sync_setup, drop_setup, flow_names_with_setup, apply_setup_changes

# Create ServerSettings lazily upon first call, as environment variables may be loaded from files, etc.
COCOINDEX_HOST = 'https://cocoindex.io'

def _load_user_app(app_path: str):
    """Loads the user's application file as a module. Exits on failure."""
    if not app_path:
        click.echo("Internal Error: Application path not provided.", err=True)
        sys.exit(1)

    app_path = os.path.abspath(app_path)
    app_dir = os.path.dirname(app_path)
    module_name = os.path.splitext(os.path.basename(app_path))[0]

    original_sys_path = list(sys.path)
    if app_dir not in sys.path:
        sys.path.insert(0, app_dir)

    try:
        spec = importlib.util.spec_from_file_location(module_name, app_path)
        if spec is None:
            raise ImportError(f"Could not load spec for file: {app_path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        return module
    except Exception as e:
        raise click.ClickException(f"Failed importing application module '{os.path.basename(app_path)}': {e}")
    finally:
        sys.path = original_sys_path

def _ensure_flows_and_handlers_built():
     """Builds flows and handlers after app load. Exits on failure."""
     try:
        flow.ensure_all_flows_built()
        query.ensure_all_handlers_built()
     except Exception as e:
        click.echo(f"\nError: Failed processing flows/handlers from application.", err=True)
        click.echo(f"Reason: {e}", err=True)
        sys.exit(1)

@click.group()
@click.version_option(package_name="cocoindex", message="%(prog)s version %(version)s")
def cli():
    """
    CLI for Cocoindex. Requires --app for most commands.
    """
    try:
        settings = setting.Settings.from_env()
        lib.init(settings)
        atexit.register(lib.stop)
    except Exception as e:
        raise click.ClickException(f"Failed to initialize CocoIndex library: {e}")

@cli.command()
@click.option(
    '--app', 'app_path', required=False,
    type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True, resolve_path=True),
    help="Path to the Python file defining flows."
)
@click.option(
    "-a", "--all", "show_all", is_flag=True, show_default=True, default=False,
    help="Also show all flows with persisted setup, even if not defined in the current process.")
def ls(app_path: str | None, show_all: bool):
    """
    List all flows.
    """
    current_flow_names = set()

    if app_path:
        _load_user_app(app_path)
        current_flow_names = set(flow.flow_names())
    elif not show_all:
         raise click.UsageError("The --app <path/to/app.py> option is required unless using --all.")

    persisted_flow_names = flow_names_with_setup()
    remaining_persisted_flow_names = set(persisted_flow_names)

    has_missing_setup = False
    has_extra_setup = False

    for name in current_flow_names:
        if name in remaining_persisted_flow_names:
            remaining_persisted_flow_names.remove(name)
            suffix = ''
        else:
            suffix = ' [+]'
            has_missing_setup = True
        click.echo(f'{name}{suffix}')

    if show_all:
        for name in persisted_flow_names:
            if name in remaining_persisted_flow_names:
                click.echo(f'{name} [?]')
                has_extra_setup = True

    if has_missing_setup or has_extra_setup:
        click.echo('')
        click.echo('Notes:')
        if has_missing_setup:
            click.echo('  [+]: Flows present in the current process, but missing setup.')
        if has_extra_setup:
            click.echo('  [?]: Flows with persisted setup, but not in the current process.')

@cli.command()
@click.option(
    '--app', 'app_path', required=True,
    type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True, resolve_path=True),
    help="Path to the Python file defining the flow."
)
@click.argument("flow_name", type=str, required=False)
@click.option("--color/--no-color", default=True, help="Enable or disable colored output.")
@click.option("--verbose", is_flag=True, help="Show verbose output with full details.")
def show(app_path: str, flow_name: str | None, color: bool, verbose: bool):
    """
    Show the flow spec and schema in a readable format.
    """
    _load_user_app(app_path)

    flow = _flow_by_name(flow_name)
    console = Console(no_color=not color)
    console.print(flow._render_spec(verbose=verbose))

    console.print()
    table = Table(
        title=f"Schema for Flow: {flow.name}",
        title_style="cyan",
        header_style="bold magenta"
    )
    table.add_column("Field", style="cyan")
    table.add_column("Type", style="green")
    table.add_column("Attributes", style="yellow")

    for field_name, field_type, attr_str in flow._get_schema():
        table.add_row(field_name, field_type, attr_str)

    console.print(table)

@cli.command()
@click.option(
    '--app', 'app_path', required=True,
    type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True, resolve_path=True),
    help="Path to the Python file defining flows to set up."
)
def setup(app_path: str):
    """
    Check and apply backend setup changes for flows, including the internal and target storage
    (to export).
    """
    _load_user_app(app_path)
    setup_status = sync_setup()
    click.echo(setup_status)
    if setup_status.is_up_to_date():
        click.echo("No changes need to be pushed.")
        return
    if not click.confirm(
        "Changes need to be pushed. Continue? [yes/N]", default=False, show_default=False):
        return
    apply_setup_changes(setup_status)

@cli.command()
@click.option(
    '--app', 'app_path', required=False,
    type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True, resolve_path=True),
    help="Path to the app file (needed if not using --all or specific names)."
)
@click.argument("flow_name", type=str, nargs=-1)
@click.option(
    "-a", "--all", "drop_all", is_flag=True, show_default=True, default=False,
    help="Drop the backend setup for all flows with persisted setup, "
         "even if not defined in the current process.")
def drop(app_path: str | None, flow_name: tuple[str, ...], drop_all: bool):
    """
    Drop the backend setup for specified flows.
    If no flow is specified, all flows defined in the current process will be dropped.
    """
    if not app_path:
        raise click.UsageError("The --app <path> option is required when dropping flows defined in the app (and not using --all or specific flow names).")
    _load_user_app(app_path)

    if drop_all:
        flow_names = flow_names_with_setup()
    elif len(flow_name) == 0:
        flow_names = [fl.name for fl in flow.flows()]
    else:
        flow_names = list(flow_name)
    setup_status = drop_setup(flow_names)
    click.echo(setup_status)
    if setup_status.is_up_to_date():
        click.echo("No flows need to be dropped.")
        return
    if not click.confirm(
        "Changes need to be pushed. Continue? [yes/N]", default=False, show_default=False):
        return
    apply_setup_changes(setup_status)

@cli.command()
@click.option(
    '--app', 'app_path', required=True,
    type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True, resolve_path=True),
    help="Path to the Python file defining flows."
)
@click.argument("flow_name", type=str, required=False)
@click.option(
    "-L", "--live", is_flag=True, show_default=True, default=False,
    help="Continuously watch changes from data sources and apply to the target index.")
@click.option(
    "-q", "--quiet", is_flag=True, show_default=True, default=False,
    help="Avoid printing anything to the standard output, e.g. statistics.")
def update(app_path: str, flow_name: str | None, live: bool, quiet: bool):
    """
    Update the index to reflect the latest data from data sources.
    """
    _load_user_app(app_path)
    options = flow.FlowLiveUpdaterOptions(live_mode=live, print_stats=not quiet)
    if flow_name is None:
        return flow.update_all_flows(options)
    else:
        with flow.FlowLiveUpdater(_flow_by_name(flow_name), options) as updater:
            updater.wait()
            return updater.update_stats()

@cli.command()
@click.option(
    '--app', 'app_path', required=True,
    type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True, resolve_path=True),
    help="Path to the Python file defining the flow."
)
@click.argument("flow_name", type=str, required=False)
@click.option(
    "-o", "--output-dir", type=str, required=False,
    help="The directory to dump the output to.")
@click.option(
    "--cache/--no-cache", is_flag=True, show_default=True, default=True,
    help="Use already-cached intermediate data if available. "
         "Note that we only reuse existing cached data without updating the cache "
         "even if it's turned on.")
def evaluate(app_path: str, flow_name: str | None, output_dir: str | None, cache: bool = True):
    """
    Evaluate the flow and dump flow outputs to files.

    Instead of updating the index, it dumps what should be indexed to files.
    Mainly used for evaluation purpose.
    """
    _load_user_app(app_path)
    fl = _flow_by_name(flow_name)
    if output_dir is None:
        output_dir = f"eval_{fl.name}_{datetime.datetime.now().strftime('%y%m%d_%H%M%S')}"
    options = flow.EvaluateAndDumpOptions(output_dir=output_dir, use_cache=cache)
    fl.evaluate_and_dump(options)

@cli.command()
@click.option(
    "--app", "app_path", required=True,
    type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True, resolve_path=True),
    help="Path to the Python file defining flows and handlers."
)
@click.option(
    "-a", "--address", type=str,
    help="The address to bind the server to, in the format of IP:PORT. "
         "If unspecified, the address specified in COCOINDEX_SERVER_ADDRESS will be used.")
@click.option(
    "-c", "--cors-origin", type=str,
    help="The origins of the clients (e.g. CocoInsight UI) to allow CORS from. "
         "Multiple origins can be specified as a comma-separated list. "
         "e.g. `https://cocoindex.io,http://localhost:3000`. "
         "Origins specified in COCOINDEX_SERVER_CORS_ORIGINS will also be included.")
@click.option(
    "-ci", "--cors-cocoindex", is_flag=True, show_default=True, default=False,
    help=f"Allow {COCOINDEX_HOST} to access the server.")
@click.option(
    "-cl", "--cors-local", type=int,
    help="Allow http://localhost:<port> to access the server.")
@click.option(
    "-L", "--live-update", is_flag=True, show_default=True, default=False,
    help="Continuously watch changes from data sources and apply to the target index.")
@click.option(
    "-q", "--quiet", is_flag=True, show_default=True, default=False,
    help="Avoid printing anything to the standard output, e.g. statistics.")
def server(app_path: str, address: str | None, live_update: bool, quiet: bool,
           cors_origin: str | None, cors_cocoindex: bool, cors_local: int | None):
    """
    Start a HTTP server providing REST APIs.

    It will allow tools like CocoInsight to access the server.
    """
    _load_user_app(app_path)
    _ensure_flows_and_handlers_built()

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

    lib.start_server(server_settings)

    if live_update:
        options = flow.FlowLiveUpdaterOptions(live_mode=True, print_stats=not quiet)
        flow.update_all_flows(options)
    if COCOINDEX_HOST in cors_origins:
        click.echo(f"Open CocoInsight at: {COCOINDEX_HOST}/cocoinsight")
    input("Press Enter to stop...")


def _flow_name(name: str | None) -> str:
    names = flow.flow_names()
    if name is not None:
        if name not in names:
            raise click.BadParameter(f"Flow {name} not found")
        return name
    if len(names) == 0:
        raise click.UsageError("No flows available")
    elif len(names) == 1:
        return names[0]
    else:
        raise click.UsageError("Multiple flows available, please specify --name")

def _flow_by_name(name: str | None) -> flow.Flow:
    return flow.flow_by_name(_flow_name(name))

if __name__ == "__main__":
    cli()
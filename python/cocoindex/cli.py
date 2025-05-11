import click
import datetime

from rich.console import Console
from rich.table import Table

from . import flow, lib, setting
from .setup import sync_setup, drop_setup, flow_names_with_setup, apply_setup_changes

@click.group()
def cli():
    """
    CLI for Cocoindex.
    """

@cli.command()
@click.option(
    "-a", "--all", "show_all", is_flag=True, show_default=True, default=False,
    help="Also show all flows with persisted setup, even if not defined in the current process.")
def ls(show_all: bool):
    """
    List all flows.
    """
    current_flow_names = flow.flow_names()
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
@click.argument("flow_name", type=str, required=False)
@click.option("--color/--no-color", default=True, help="Enable or disable colored output.")
@click.option("--verbose", is_flag=True, help="Show verbose output with full details.")
def show(flow_name: str | None, color: bool, verbose: bool):
    """
    Show the flow spec and schema in a readable format with colored output.
    """
    flow = _flow_by_name(flow_name)
    console = Console(no_color=not color)
    console.print(flow._render_spec(verbose=verbose))

    console.print()
    table = Table(
        title=f"Schema for Flow: {flow.name}",
        show_header=True,
        header_style="bold magenta"
    )
    table.add_column("Field", style="cyan")
    table.add_column("Type", style="green")
    table.add_column("Attributes", style="yellow")

    for field_name, field_type, attr_str in flow._get_schema():
        table.add_row(field_name, field_type, attr_str)

    console.print(table)

@cli.command()
def setup():
    """
    Check and apply backend setup changes for flows, including the internal and target storage
    (to export).
    """
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
@click.argument("flow_name", type=str, nargs=-1)
@click.option(
    "-a", "--all", "drop_all", is_flag=True, show_default=True, default=False,
    help="Drop the backend setup for all flows with persisted setup, "
         "even if not defined in the current process.")
def drop(flow_name: tuple[str, ...], drop_all: bool):
    """
    Drop the backend setup for specified flows.
    If no flow is specified, all flows defined in the current process will be dropped.
    """
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
@click.argument("flow_name", type=str, required=False)
@click.option(
    "-L", "--live", is_flag=True, show_default=True, default=False,
    help="Continuously watch changes from data sources and apply to the target index.")
@click.option(
    "-q", "--quiet", is_flag=True, show_default=True, default=False,
    help="Avoid printing anything to the standard output, e.g. statistics.")
def update(flow_name: str | None, live: bool, quiet: bool):
    """
    Update the index to reflect the latest data from data sources.
    """
    options = flow.FlowLiveUpdaterOptions(live_mode=live, print_stats=not quiet)
    if flow_name is None:
        return flow.update_all_flows(options)
    else:
        with flow.FlowLiveUpdater(_flow_by_name(flow_name), options) as updater:
            updater.wait()
            return updater.update_stats()

@cli.command()
@click.argument("flow_name", type=str, required=False)
@click.option(
    "-o", "--output-dir", type=str, required=False,
    help="The directory to dump the output to.")
@click.option(
    "--cache/--no-cache", is_flag=True, show_default=True, default=True,
    help="Use already-cached intermediate data if available. "
         "Note that we only reuse existing cached data without updating the cache "
         "even if it's turned on.")
def evaluate(flow_name: str | None, output_dir: str | None, cache: bool = True):
    """
    Evaluate the flow and dump flow outputs to files.

    Instead of updating the index, it dumps what should be indexed to files.
    Mainly used for evaluation purpose.
    """
    fl = _flow_by_name(flow_name)
    if output_dir is None:
        output_dir = f"eval_{fl.name}_{datetime.datetime.now().strftime('%y%m%d_%H%M%S')}"
    options = flow.EvaluateAndDumpOptions(output_dir=output_dir, use_cache=cache)
    fl.evaluate_and_dump(options)

# Create ServerSettings lazily upon first call, as environment variables may be loaded from files, etc.
COCOINDEX_HOST = 'https://cocoindex.io'

@cli.command()
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
def server(address: str | None, live_update: bool, quiet: bool, cors_origin: str | None,
           cors_cocoindex: bool, cors_local: int | None):
    """
    Start a HTTP server providing REST APIs.

    It will allow tools like CocoInsight to access the server.
    """
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

import asyncio
import click
import datetime
from rich.console import Console

from . import flow, lib
from .setup import sync_setup, drop_setup, flow_names_with_setup, apply_setup_changes
from .runtime import execution_context

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
    current_flow_names = [fl.name for fl in flow.flows()]
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
@click.option("--color/--no-color", default=True)
def show(flow_name: str | None, color: bool):
    """
    Show the flow spec in a readable format with colored output.
    """
    flow = _flow_by_name(flow_name)
    console = Console(no_color=not color)
    console.print(flow._render_text())

@cli.command()
def setup():
    """
    Check and apply backend setup changes for flows, including the internal and target storage
    (to export).
    """
    status_check = sync_setup()
    click.echo(status_check)
    if status_check.is_up_to_date():
        click.echo("No changes need to be pushed.")
        return
    if not click.confirm(
        "Changes need to be pushed. Continue? [yes/N]", default=False, show_default=False):
        return
    apply_setup_changes(status_check)

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
    status_check = drop_setup(flow_names)
    click.echo(status_check)
    if status_check.is_up_to_date():
        click.echo("No flows need to be dropped.")
        return
    if not click.confirm(
        "Changes need to be pushed. Continue? [yes/N]", default=False, show_default=False):
        return
    apply_setup_changes(status_check)

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
    async def _update():
        if flow_name is None:
            await flow.update_all_flows(options)
        else:
            updater = await flow.FlowLiveUpdater.create(_flow_by_name(flow_name), options)
            await updater.wait()
    execution_context.run(_update())

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

_default_server_settings = lib.ServerSettings.from_env()

@cli.command()
@click.option(
    "-a", "--address", type=str, default=_default_server_settings.address,
    help="The address to bind the server to, in the format of IP:PORT.")
@click.option(
    "-c", "--cors-origin", type=str, default=_default_server_settings.cors_origin,
    help="The origin of the client (e.g. CocoInsight UI) to allow CORS from. "
         "e.g. `http://cocoindex.io` if you want to allow CocoInsight to access the server.")
@click.option(
    "-L", "--live-update", is_flag=True, show_default=True, default=False,
    help="Continuously watch changes from data sources and apply to the target index.")
@click.option(
    "-q", "--quiet", is_flag=True, show_default=True, default=False,
    help="Avoid printing anything to the standard output, e.g. statistics.")
def server(address: str, live_update: bool, quiet: bool, cors_origin: str | None):
    """
    Start a HTTP server providing REST APIs.

    It will allow tools like CocoInsight to access the server.
    """
    lib.start_server(lib.ServerSettings(address=address, cors_origin=cors_origin))
    if live_update:
        options = flow.FlowLiveUpdaterOptions(live_mode=True, print_stats=not quiet)
        execution_context.run(flow.update_all_flows(options))
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

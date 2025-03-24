import click
import datetime

from . import flow, lib
from .setup import check_setup_status, CheckSetupStatusOptions, apply_setup_changes

@click.group()
def cli():
    """
    CLI for Cocoindex.
    """

@cli.command()
def ls():
    """
    List all available flows.
    """
    for name in flow.flow_names():
        click.echo(name)

@cli.command()
@click.argument("flow_name", type=str, required=False)
def show(flow_name: str | None):
    """
    Show the flow spec.
    """
    click.echo(str(_flow_by_name(flow_name)))

@cli.command()
@click.option(
    "-D", "--delete_legacy_flows", is_flag=True, show_default=True, default=False,
    help="Also check / delete flows existing before but no longer exist.")
def setup(delete_legacy_flows):
    """
    Check and apply backend setup changes for flows, including the internal and target storage (to export).
    """
    options = CheckSetupStatusOptions(delete_legacy_flows=delete_legacy_flows)
    status_check = check_setup_status(options)
    print(status_check)
    if status_check.is_up_to_date():
        return
    if not click.confirm(
        "Changes need to be pushed. Continue? [yes/N]", default=False, show_default=False):
        return
    apply_setup_changes(status_check)

@cli.command()
@click.argument("flow_name", type=str, required=False)
def update(flow_name: str | None):
    """
    Update the index defined by the flow.
    """
    stats = _flow_by_name(flow_name).update()
    print(stats)

@cli.command()
@click.argument("flow_name", type=str, required=False)
@click.option(
    "-o", "--output-dir", type=str, required=False,
    help="The directory to dump the output to.")
@click.option(
    "-c", "--use-cache", is_flag=True, show_default=True, default=True,
    help="Use already-cached intermediate data if available. "
         "Note that we only reuse existing cached data without updating the cache "
         "even if it's turned on.")
def evaluate(flow_name: str | None, output_dir: str | None, use_cache: bool = True):
    """
    Evaluate the flow and dump flow outputs to files.

    Instead of updating the index, it dumps what should be indexed to files.
    Mainly used for evaluation purpose.
    """
    fl = _flow_by_name(flow_name)
    if output_dir is None:
        output_dir = f"eval_{fl.name}_{datetime.datetime.now().strftime('%y%m%d_%H%M%S')}"
    options = flow.EvaluateAndDumpOptions(output_dir=output_dir, use_cache=use_cache)
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
def server(address: str, cors_origin: str | None):
    """
    Start a HTTP server providing REST APIs.

    It will allow tools like CocoInsight to access the server.
    """
    lib.start_server(lib.ServerSettings(address=address, cors_origin=cors_origin))
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

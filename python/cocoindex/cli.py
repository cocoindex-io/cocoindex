"""CLI for CocoIndex."""

import click
from cocoindex.storage import get_storage

@click.group()
def cli():
    """CocoIndex CLI tool."""
    pass

@cli.command()
@click.argument('path', required=False, default=None)
@click.option('-l', '--long', is_flag=True, help='Show detailed information for each stable path.')
@click.option('-r', '--recursive', is_flag=True, help='Show children recursively when a path is given.')
def show(path, long, recursive):
    """Display information about stable paths in the LMDB store.

    If PATH is provided, only show information for that specific path.
    With -r, also show all children recursively.
    With -l, print detailed information (metadata, size, etc.).
    """
    storage = get_storage()
    
    if path is None:
        paths = storage.list_keys()
    else:
        if not storage.exists(path):
            click.echo(f"Path '{path}' does not exist.", err=True)
            return
        if recursive:
            paths = [p for p in storage.list_keys() if p == path or p.startswith(path + '/')]
        else:
            paths = [path]
    
    for p in sorted(paths):
        if long:
            data = storage.get(p)
            if data is not None:
                details = storage.get_metadata(p) if hasattr(storage, 'get_metadata') else {}
                size = len(data)
                click.echo(f"{p} (size={size}, metadata={details})")
            else:
                click.echo(f"{p} (no data)")
        else:
            click.echo(p)

if __name__ == '__main__':
    cli()
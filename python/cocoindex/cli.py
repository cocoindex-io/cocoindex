#!/usr/bin/env python3
"""CocoIndex CLI tool."""

from __future__ import annotations

import click
import cocoindex as coco
from cocoindex.storage import get_lmdb_storage


@click.group()
def cli():
    """CocoIndex CLI - manage and inspect pipelines."""
    pass


@cli.command()
@click.option("-l", is_flag=True, help="Print detailed LMDB information for each stable path.")
@click.option("-r", is_flag=True, help="Recursively show children of the specified path.")
@click.argument("path", required=False, default=None)
def show(l, r, path):
    """Display information about stable paths in the LMDB storage.

    If PATH is provided, show information only for that path.
    Use -r to include all children recursively.
    """
    storage = get_lmdb_storage()
    if path:
        if r:
            # recursively show children
            paths = storage.get_all_paths(prefix=path)
        else:
            paths = [path] if storage.path_exists(path) else []
    else:
        paths = storage.get_all_paths()

    if not paths:
        click.echo("No paths found.")
        return

    for p in sorted(paths):
        if l:
            detail = storage.get_path_detail(p)
            click.echo(f"Path: {p}")
            click.echo(f"  Data: {detail.data}")
            click.echo(f"  Metadata: {detail.metadata}")
            click.echo(f"  Type: {detail.type}")
            click.echo(f"  Size: {detail.size}")
            click.echo("---")
        else:
            click.echo(p)


if __name__ == "__main__":
    cli()
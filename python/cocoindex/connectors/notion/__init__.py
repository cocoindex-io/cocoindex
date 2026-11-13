"""Read-only Notion data-source ingestion."""

from . import _source
from ._source import *  # noqa: F401, F403

__all__ = _source.__all__

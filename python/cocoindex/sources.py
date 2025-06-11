"""All builtin sources."""

from . import op
import datetime


class LocalFile(op.SourceSpec):
    """Import data from local file system."""

    _op_category = op.OpCategory.SOURCE

    path: str
    binary: bool = False

    # If provided, only files matching these patterns will be included.
    # See https://docs.rs/globset/latest/globset/index.html#syntax for the syntax of the patterns.
    included_patterns: list[str] | None = None

    # If provided, files matching these patterns will be excluded.
    # See https://docs.rs/globset/latest/globset/index.html#syntax for the syntax of the patterns.
    excluded_patterns: list[str] | None = None


class GoogleDrive(op.SourceSpec):
    """Import data from Google Drive."""

    _op_category = op.OpCategory.SOURCE

    service_account_credential_path: str
    root_folder_ids: list[str]
    binary: bool = False
    recent_changes_poll_interval: datetime.timedelta | None = None


class AmazonS3(op.SourceSpec):
    """Import data from an Amazon S3 bucket. Supports optional prefix and file filtering by glob patterns."""

    _op_category = op.OpCategory.SOURCE

    bucket_name: str
    prefix: str | None = None
    binary: bool = False
    included_patterns: list[str] | None = None
    excluded_patterns: list[str] | None = None
    sqs_queue_url: str | None = None

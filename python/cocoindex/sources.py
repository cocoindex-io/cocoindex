"""All builtin sources."""

from . import op
from .auth_registry import TransientAuthEntryReference
from .setting import DatabaseConnectionSpec
from dataclasses import dataclass
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


class AzureBlob(op.SourceSpec):
    """
    Import data from an Azure Blob Storage container. Supports optional prefix and file filtering by glob patterns.

    Authentication mechanisms taken in the following order:
    - SAS token (if provided)
    - Account access key (if provided)
    - Default Azure credential
    """

    _op_category = op.OpCategory.SOURCE

    account_name: str
    container_name: str
    prefix: str | None = None
    binary: bool = False
    included_patterns: list[str] | None = None
    excluded_patterns: list[str] | None = None

    sas_token: TransientAuthEntryReference[str] | None = None
    account_access_key: TransientAuthEntryReference[str] | None = None


@dataclass
class PostgresNotification:
    """Notification for a PostgreSQL table."""

    # Optional: name of the PostgreSQL channel to use.
    # If not provided, will generate a default channel name.
    channel_name: str | None = None


class Postgres(op.SourceSpec):
    """Import data from a PostgreSQL table."""

    _op_category = op.OpCategory.SOURCE

    # Table name to read from (required)
    table_name: str

    # Database connection reference (optional - uses default if not provided)
    database: TransientAuthEntryReference[DatabaseConnectionSpec] | None = None

    # Optional: specific columns to include (if None, includes all columns)
    included_columns: list[str] | None = None

    # Optional: column name to use for ordinal tracking (for incremental updates)
    # Should be a timestamp, serial, or other incrementing column
    ordinal_column: str | None = None

    # Optional: when set, supports change capture from PostgreSQL notification.
    notification: PostgresNotification | None = None

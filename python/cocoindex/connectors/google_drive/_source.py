"""Google Drive source utilities.

This module provides a read-only API for ingesting files from Google Drive.
Change notifications will be added later.
"""

from __future__ import annotations

import asyncio
import io
from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePath
from typing import Any, AsyncIterator, Iterator, Sequence, Self

try:
    from google.oauth2.service_account import Credentials  # type: ignore
    from googleapiclient.discovery import build  # type: ignore
    from googleapiclient.http import MediaIoBaseDownload  # type: ignore
except ImportError as e:
    raise ImportError(
        "google-auth and google-api-python-client are required to use the Google Drive source. "
        "Please install cocoindex[google_drive]."
    ) from e

from cocoindex.connectorkits import connection
from cocoindex.resources import file


# Default base dir for unregistered Google Drive access (not registered)
# The value is an empty string since Google Drive doesn't need a resolved base path
_DEFAULT_DRIVE_BASE_DIR = connection.keyed_value("cocoindex/google_drive", "")


class DriveFilePath(file.FilePath[str]):
    """
    File path for Google Drive files.

    The resolved path is the Google Drive file ID (string).
    """

    __slots__ = ("_file_id",)

    _file_id: str

    def __init__(
        self,
        path: str | PurePath,
        *,
        file_id: str,
        _base_dir: connection.KeyedConnection[str] | None = None,
    ) -> None:
        super().__init__(
            _base_dir if _base_dir is not None else _DEFAULT_DRIVE_BASE_DIR,
            PurePath(path),
        )
        self._file_id = file_id

    def resolve(self) -> str:
        """Return the Google Drive file ID."""
        return self._file_id

    def _with_path(self, path: PurePath) -> Self:
        """Create a new DriveFilePath with the given path."""
        return type(self)(path, file_id=self._file_id, _base_dir=self._base_dir)  # type: ignore[return-value]


_DRIVE_SCOPE = "https://www.googleapis.com/auth/drive.readonly"
_FOLDER_MIME = "application/vnd.google-apps.folder"
_DOCS_MIME = "application/vnd.google-apps.document"
_SHEETS_MIME = "application/vnd.google-apps.spreadsheet"
_SLIDES_MIME = "application/vnd.google-apps.presentation"

_EXPORT_MIME_BY_TYPE = {
    _DOCS_MIME: "text/plain",
    _SHEETS_MIME: "text/csv",
    _SLIDES_MIME: "text/plain",
}


@dataclass
class DriveFileInfo:
    file_id: str
    name: str
    mime_type: str
    size: int
    modified_time: datetime


class DriveFile(file.FileLike[str]):
    """Represents a file entry from Google Drive."""

    _service: Any
    _mime_type: str
    _file_id: str

    def __init__(self, service: Any, info: DriveFileInfo) -> None:
        file_path = DriveFilePath(info.name, file_id=info.file_id)
        metadata = file.FileMetadata(size=info.size, modified_time=info.modified_time)
        super().__init__(file_path, _metadata=metadata)
        self._service = service
        self._mime_type = info.mime_type
        self._file_id = info.file_id

    async def _fetch_metadata(self) -> file.FileMetadata:
        """Fetch metadata from the Google Drive API."""

        def _fetch() -> file.FileMetadata:
            response = (
                self._service.files()
                .get(
                    fileId=self._file_id,
                    fields="size, modifiedTime",
                )
                .execute()
            )
            size_raw = response.get("size")
            size = int(size_raw) if size_raw is not None else 0
            return file.FileMetadata(
                size=size,
                modified_time=_parse_modified_time(response.get("modifiedTime")),
            )

        return await asyncio.to_thread(_fetch)

    def _read_sync(self, size: int = -1) -> bytes:
        """Synchronously read file content (internal helper)."""
        if size != -1:
            raise ValueError("Partial reads are not supported for Google Drive files.")

        if self._mime_type in _EXPORT_MIME_BY_TYPE:
            export_mime = _EXPORT_MIME_BY_TYPE[self._mime_type]
            request = self._service.files().export_media(
                fileId=self._file_id, mimeType=export_mime
            )
        else:
            request = self._service.files().get_media(fileId=self._file_id)

        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return fh.getvalue()

    async def _read_impl(self, size: int = -1) -> bytes:
        """Read file content via Google Drive API in a thread pool."""
        return await asyncio.to_thread(self._read_sync, size)


@dataclass
class GoogleDriveSourceSpec:
    """Specification for a Google Drive source."""

    service_account_credential_path: str
    root_folder_ids: Sequence[str]
    mime_types: Sequence[str] | None = None


def _build_service(credential_path: str) -> Any:
    creds = Credentials.from_service_account_file(  # type: ignore[no-untyped-call]
        credential_path,
        scopes=[_DRIVE_SCOPE],
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _parse_modified_time(value: str | None) -> datetime:
    if not value:
        return datetime.fromtimestamp(0)
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _list_children(service: Any, folder_id: str) -> list[DriveFileInfo]:
    query = f"'{folder_id}' in parents and trashed = false"
    page_token = None
    results: list[DriveFileInfo] = []
    while True:
        response = (
            service.files()
            .list(
                q=query,
                fields="nextPageToken, files(id, name, mimeType, size, modifiedTime)",
                pageToken=page_token,
            )
            .execute()
        )
        for f in response.get("files", []):
            size_raw = f.get("size")
            size = int(size_raw) if size_raw is not None else 0
            results.append(
                DriveFileInfo(
                    file_id=f["id"],
                    name=f["name"],
                    mime_type=f["mimeType"],
                    size=size,
                    modified_time=_parse_modified_time(f.get("modifiedTime")),
                )
            )
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    return results


def list_files(spec: GoogleDriveSourceSpec) -> Iterator[DriveFile]:
    """List files under the given root folders (recursively)."""
    service = _build_service(spec.service_account_credential_path)
    folders = list(spec.root_folder_ids)

    while folders:
        folder_id = folders.pop(0)
        for info in _list_children(service, folder_id):
            if info.mime_type == _FOLDER_MIME:
                folders.append(info.file_id)
                continue
            if spec.mime_types and info.mime_type not in spec.mime_types:
                continue
            yield DriveFile(service, info)


class GoogleDriveSource:
    """Source wrapper for Google Drive files."""

    def __init__(
        self,
        *,
        service_account_credential_path: str,
        root_folder_ids: Sequence[str],
        mime_types: Sequence[str] | None = None,
    ) -> None:
        self._spec = GoogleDriveSourceSpec(
            service_account_credential_path=service_account_credential_path,
            root_folder_ids=root_folder_ids,
            mime_types=mime_types,
        )

    async def files(self) -> AsyncIterator[DriveFile]:
        """Async iterate over Google Drive files."""
        from cocoindex.connectorkits.async_adapters import sync_to_async_iter

        async for f in sync_to_async_iter(lambda: list_files(self._spec)):
            yield f

    async def items(self) -> AsyncIterator[tuple[str, DriveFile]]:
        """Async iterate as (key, file) pairs for use with mount_each().

        The key is the file's name path.
        """
        async for f in self.files():
            yield (f.file_path.path.as_posix(), f)


__all__ = [
    "DriveFile",
    "DriveFileInfo",
    "GoogleDriveSource",
    "GoogleDriveSourceSpec",
    "list_files",
]

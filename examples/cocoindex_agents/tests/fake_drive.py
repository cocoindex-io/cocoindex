"""An in-memory Google Drive, served through the slice of the Drive API the connector calls.

``FakeDrive`` holds folders and files; ``service()`` is what
``google_drive._source._build_service`` would return: ``files().list(...)``,
``get(...)``, ``get_media(...)`` and ``export_media(...)`` requests whose
``execute()`` answers from memory. Binary files carry an MD5 as Drive does;
Google-native files (a Doc, a Sheet) have no MD5 and no bytes of their own,
only an export. ``downloads`` counts every ``get_media`` and ``export_media``
served, so a test can say that an unchanged file was validated without one.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

from cocoindex.connectors.google_drive import _source

FOLDER = _source._FOLDER_MIME
DOC = _source._DOCS_MIME
SHEET = _source._SHEETS_MIME


@dataclass
class Entry:
    name: str
    mime_type: str
    parent: str | None
    content: bytes = b""
    modified: int = 0  # a clock tick; rendered as an ISO time


@dataclass
class FakeDrive:
    entries: dict[str, Entry] = field(default_factory=dict)
    clock: int = 0
    downloads: int = 0
    lists: int = 0

    def _tick(self) -> int:
        self.clock += 1
        return self.clock

    def _add(self, entry: Entry, file_id: str | None = None) -> str:
        file_id = file_id or f"id-{len(self.entries) + 1}"
        entry.modified = self._tick()
        self.entries[file_id] = entry
        return file_id

    def folder(self, name: str, parent: str | None = None) -> str:
        return self._add(Entry(name, FOLDER, parent))

    def put(
        self, parent: str, name: str, content: bytes, mime_type: str = "text/plain"
    ) -> str:
        """A binary file: Drive stores its bytes and reports their MD5."""
        return self._add(Entry(name, mime_type, parent, content))

    def doc(self, parent: str, name: str, markdown: str) -> str:
        """A Google Doc: no bytes, no MD5; exported as Markdown."""
        return self._add(Entry(name, DOC, parent, markdown.encode()))

    def sheet(self, parent: str, name: str, csv: str) -> str:
        return self._add(Entry(name, SHEET, parent, csv.encode()))

    def edit(self, file_id: str, content: bytes | str) -> None:
        entry = self.entries[file_id]
        entry.content = content.encode() if isinstance(content, str) else content
        entry.modified = self._tick()

    def touch(self, file_id: str) -> None:
        self.entries[file_id].modified = self._tick()

    def rename(self, file_id: str, name: str) -> None:
        self.entries[file_id].name = name
        self.entries[file_id].modified = self._tick()

    def delete(self, file_id: str) -> None:
        del self.entries[file_id]

    # --- the API surface ---

    def service(self) -> _Service:
        return _Service(self)

    def _metadata(self, file_id: str, entry: Entry) -> dict[str, str]:
        record = {
            "id": file_id,
            "name": entry.name,
            "mimeType": entry.mime_type,
            "modifiedTime": f"2026-11-05T00:{entry.modified // 60:02d}:{entry.modified % 60:02d}Z",
        }
        if entry.mime_type not in (FOLDER, DOC, SHEET):
            record["size"] = str(len(entry.content))
            record["md5Checksum"] = hashlib.md5(entry.content).hexdigest()
        return record


@dataclass
class _Request:
    answer: object

    def execute(self, num_retries: int = 0) -> object:
        return self.answer


@dataclass
class _Files:
    drive: FakeDrive

    def list(self, *, q: str, fields: str, pageToken: str | None) -> _Request:
        self.drive.lists += 1
        folder_id = q.split("'")[1]
        return _Request(
            {
                "files": [
                    self.drive._metadata(file_id, entry)
                    for file_id, entry in self.drive.entries.items()
                    if entry.parent == folder_id
                ]
            }
        )

    def get(self, *, fileId: str, fields: str) -> _Request:
        return _Request(self.drive._metadata(fileId, self.drive.entries[fileId]))

    def get_media(self, *, fileId: str) -> _Request:
        self.drive.downloads += 1
        return _Request(self.drive.entries[fileId].content)

    def export_media(self, *, fileId: str, mimeType: str) -> _Request:
        self.drive.downloads += 1
        entry = self.drive.entries[fileId]
        assert entry.mime_type in (DOC, SHEET), "only Google-native files export"
        return _Request(entry.content)


@dataclass
class _Service:
    drive: FakeDrive

    def files(self) -> _Files:
        return _Files(self.drive)

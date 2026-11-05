"""A Google Drive folder as a source (DESIGN.md §3, *Sources*; §7).

The same contract as the local folder (``sources.FolderSource``): widened
``stats``, a hash-stable ``sample``, ``read(key)``, the planner-only
``keys``, and every read recorded as a data dependency of the session that
made it. What differs is where the records point and how they validate:

- A file is keyed by its **path under the folder** (``notes/Meeting notes``),
  as a local file is by its relative path, so a worker's output can be named
  after it. Google Docs, Sheets and Slides have no bytes of their own; the
  connector exports them (Markdown, CSV, text), and that export is what
  ``read`` and a worker's ``read_input`` return. A PDF stored in Drive is a
  document the model reads natively, as a local one is (``documents.py``).
- The **account** is whoever signed in on this machine
  (``cocoindex google-drive sign-in``): the model names the folder and never
  a credential. The account's address, not its tokens, is in the source's
  memo key and in every record, so a refreshed token changes nothing and a
  different account re-runs what was read.
- An observation validates by walking the folder again through the Drive
  API and comparing a digest, as a local one does by walking the directory.
  A file read is keyed by its path and validated by resolving the path
  again: the same file with the same ``modifiedTime`` validates without a
  download; a changed time compares the MD5 Drive gives a binary file, or
  re-exports a Google-native file and compares its digest.
- The fan-out's walk records each file's id, ``modifiedTime`` and MD5, so an
  edit, an addition, a removal or a rename re-runs the fan-out's session and
  probes its workers.

The validators read the token file at the path stored in the record (a
location, not a secret); a sign-out or a different account signed in there
leaves every record invalid, so the session re-runs and reports the missing
sign-in as a contained failure until someone signs in again.
"""

from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass

import cocoindex as coco
from cocoindex.connectors import google_drive
from cocoindex.resources.file import PatternFilePathMatcher

from ..documents import Document
from ..errors import ToolError
from ..spec import Json
from .folder import (
    FolderSource,
    content_of,
    digest_bytes,
    digest_of_json,
    file_key_arg,
)

# A pasted Drive link: https://drive.google.com/drive/folders/<id>, with or
# without /u/<n>/ and a query string. A folder id itself is one path segment.
_FOLDER_LINK = re.compile(r"/folders/([^/?#]+)")
_FOLDER_ID = re.compile(r"^[A-Za-z0-9_-]+$")


def folder_id_of(text: str) -> str:
    """The folder id a prompt's link or id denotes; anything else is the model's error."""
    text = text.strip()
    match = _FOLDER_LINK.search(text)
    if match:
        return match.group(1)
    if _FOLDER_ID.match(text):
        return text
    raise ToolError(
        f"{text!r} is not a Google Drive folder link or id "
        "(expected https://drive.google.com/drive/folders/<id>)"
    )


@dataclass(frozen=True)
class DriveSourceRef:
    """What identifies a Drive source in a record: the folder, and the account that read it.

    ``token_path`` is where the validator finds the sign-in (a location, never
    a secret); ``account`` is the address it must find signed in there.
    """

    token_path: str
    account: str
    folder_id: str
    patterns: tuple[str, ...]


@dataclass(frozen=True)
class DriveObservation:
    """The key of a Drive observation record: which source, which call."""

    source: DriveSourceRef
    method: str
    args: str  # canonical JSON of the call's arguments


@dataclass(frozen=True)
class DriveFileRef:
    """The key of a Drive file-read record: which source, which file by path."""

    source: DriveSourceRef
    key: str


# What a file read saw: the file's id, its modifiedTime and its content
# fingerprint (Drive's MD5 for a binary file, a digest of the export for a
# Google-native one); ``None`` when no file had that path.
DriveFileState = tuple[str, str, str] | None


def _source_of(ref: DriveSourceRef) -> GoogleDriveFolderSource | None:
    """The recorded source, if the recorded account is still signed in at its token path.

    ``None`` makes the record invalid, so the session re-runs: its body meets the
    same missing sign-in in ``open_source``, where an open session contains it
    (DESIGN.md §7). Raising here instead would fail the whole run, since the
    engine propagates a validator's exception.
    """
    account = google_drive.UserAccount(ref.token_path)
    try:
        email = account.email
    except google_drive.SignInError:
        return None
    if email != ref.account:
        return None
    return GoogleDriveFolderSource(ref.folder_id, ref.patterns, account)


async def _validate_observation(
    key: DriveObservation, prev: bytes
) -> coco.MemoStateOutcome:
    """Observe again as the recorded account; valid iff byte-identical."""
    src = _source_of(key.source)
    if src is None:
        return coco.MemoStateOutcome(state=prev)
    try:
        observed = await src.observe(key.method, json.loads(key.args))
    except google_drive.SignInError:  # refused at refresh: expired or revoked
        return coco.MemoStateOutcome(state=prev)
    digest = digest_of_json(observed)
    return coco.MemoStateOutcome(state=digest, memo_valid=digest == prev)


async def _validate_file_read(
    key: DriveFileRef, prev: DriveFileState
) -> coco.MemoStateOutcome:
    """Resolve the path again; an unchanged modifiedTime validates without a download."""
    src = _source_of(key.source)
    if src is None:
        return coco.MemoStateOutcome(state=prev)
    try:
        file = (await src.files()).get(key.key)
    except google_drive.SignInError:  # refused at refresh: expired or revoked
        return coco.MemoStateOutcome(state=prev)
    if prev is None or file is None:
        return coco.MemoStateOutcome(
            state=prev, memo_valid=prev is None and file is None
        )
    file_id, modified, fingerprint = prev
    if file.info.file_id != file_id:
        return coco.MemoStateOutcome(state=prev)
    if _modified_time(file) == modified:
        return coco.MemoStateOutcome(state=prev, memo_valid=True)
    try:
        state = await _file_state(file)
    except google_drive.SignInError:
        return coco.MemoStateOutcome(state=prev)
    return coco.MemoStateOutcome(state=state, memo_valid=state[2] == fingerprint)


OBSERVATION = coco.DataDep(
    "cocoindex_agents.google_drive.observation", validate=_validate_observation
)
FILE_READ = coco.DataDep(
    "cocoindex_agents.google_drive.file", validate=_validate_file_read
)


def _modified_time(file: google_drive.DriveFile) -> str:
    return file.info.modified_time.isoformat()


async def _file_state(file: google_drive.DriveFile) -> tuple[str, str, str]:
    """A read's state. Downloads only a Google-native file (it has no MD5)."""
    fingerprint = file.info.md5_checksum
    if fingerprint is None:
        fingerprint = digest_bytes(await file.read()).hex()
    return (file.info.file_id, _modified_time(file), fingerprint)


class GoogleDriveFolderSource(FolderSource[google_drive.DriveFile]):
    """A ``src:`` handle over one Drive folder, read as the signed-in account."""

    kind = "google_drive"
    observation = OBSERVATION

    def __init__(
        self,
        folder_id: str,
        patterns: tuple[str, ...],
        account: google_drive.UserAccount | None = None,
    ) -> None:
        self.folder_id = folder_id
        self.patterns = patterns
        self.account = account if account is not None else google_drive.UserAccount()
        # Read once: a missing sign-in fails here, in the tool that opens the
        # source, and the address goes into every record this source makes.
        self.email = self.account.email

    def __coco_memo_key__(self) -> object:
        return ("google_drive", self.email, self.folder_id, self.patterns)

    def ref(self) -> DriveSourceRef:
        return DriveSourceRef(
            str(self.account.token_path), self.email, self.folder_id, self.patterns
        )

    def _matcher(self) -> PatternFilePathMatcher | None:
        return (
            PatternFilePathMatcher(included_patterns=list(self.patterns))
            if self.patterns
            else None
        )

    async def files(self) -> dict[str, google_drive.DriveFile]:
        """One walk through the Drive API, keyed by path; a path two files share keeps both.

        Drive allows two files of one name in a folder; the second (by id)
        gets its id appended so neither hides the other.
        """
        spec = google_drive.GoogleDriveSourceSpec(
            None, [self.folder_id], user_account=self.account
        )
        listed = await asyncio.to_thread(lambda: list(google_drive.list_files(spec)))
        matcher = self._matcher()
        found: dict[str, google_drive.DriveFile] = {}
        for file in sorted(listed, key=lambda f: (f.file_path.path, f.info.file_id)):
            if matcher is not None and not matcher.is_file_included(
                file.file_path.path
            ):
                continue
            key = file.file_path.path.as_posix()
            if key in found:
                key = f"{key}#{file.info.file_id}"
            found[key] = file
        return dict(sorted(found.items()))

    def observation_key(self, method: str, args: str) -> object:
        return DriveObservation(self.ref(), method, args)

    async def entries(self, files: dict[str, google_drive.DriveFile]) -> Json:
        return [
            (key, file.info.file_id, _modified_time(file), file.info.md5_checksum or "")
            for key, file in files.items()
        ]

    async def read(self, args: dict[str, Json]) -> str | Document:
        """Resolved by walking the folder (a Drive path is not an address), recorded
        as a read of that path alone: the walk itself is not recorded, so an edit
        elsewhere in the folder does not touch the session.
        """
        file_key = file_key_arg(args)
        file = (await self.files()).get(file_key)
        state: DriveFileState = None
        data = b""
        if file is not None:
            data = await file.read()
            state = (
                file.info.file_id,
                _modified_time(file),
                file.info.md5_checksum or digest_bytes(data).hex(),
            )
        coco.use_data_dep(
            FILE_READ, key=DriveFileRef(self.ref(), file_key), state=state
        )
        if file is None:
            raise ToolError(f"no file {file_key!r} in Drive folder {self.folder_id}")
        return await content_of(file_key, file, data)

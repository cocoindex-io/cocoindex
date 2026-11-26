"""Google Docs target: a doc the target creates and keeps in step with declared Markdown.

The doc is found by a tag of the target's own (a Drive ``appProperties`` entry
holding an id kept in the tracking record), not by its name, so a person may
rename or move it. A change is written in place through the Docs API's
``batchUpdate``: only the blocks whose content changed (see ``_docs.py``), with
``requiredRevisionId`` so that an edit made while the target writes is read and
planned around rather than overwritten. A block a person edited since the
target wrote it keeps the person's text and is reported. Deleting the
declaration moves the doc to the trash.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from dataclasses import dataclass
from typing import Any, Collection, NamedTuple, Sequence

from cocoindex._internal.extras import install_hint

try:
    import httplib2  # type: ignore
    from google_auth_httplib2 import AuthorizedHttp  # type: ignore
    from googleapiclient.discovery import build  # type: ignore
    from googleapiclient.errors import HttpError  # type: ignore
except ImportError as e:
    raise ImportError(
        "google-auth and google-api-python-client are required to use the Google Docs target. "
        + install_hint("google_docs")
    ) from e

import msgspec

import cocoindex as coco
from cocoindex._internal.context_keys import ContextKey, ContextProvider
from cocoindex._internal.datatype import TypeChecker
from cocoindex.connectorkits.fingerprint import fingerprint_str
from cocoindex.connectors.google_drive._oauth import SignInRequiredError

from . import _docs, _markdown
from ._auth import UserAccount

_logger = logging.getLogger(__name__)

# Attempts after the first for a Docs or Drive request that may be repeated
# safely; googleapiclient retries a 5xx, a 429 and a rate-limit 403 with backoff.
# A batchUpdate is safe to repeat: its requiredRevisionId refuses a second
# application. Creating a doc is not, so it is never retried.
_RETRIES = 5
# Reads and plans of one write, when a person edits between the read and the write.
_WRITE_ATTEMPTS = 3
_DOC_MIME = "application/vnd.google-apps.document"
# The Drive appProperties key that tags a doc with the target's id for it.
_TAG_PROPERTY = "cocoindexDoc"
# Raised when what a declaration writes changes, so every doc is planned again.
_FORMAT_VERSION = 1


def _new_http() -> Any:
    """The transport under every Docs and Drive request; tests replace it."""
    return httplib2.Http(timeout=60)


def doc_url(doc_id: str) -> str:
    return f"https://docs.google.com/document/d/{doc_id}/edit"


# --- target state ------------------------------------------------------------


class _DocKey(NamedTuple):
    account_key: str  # the ContextKey the UserAccount is provided under
    name: str


_KEY_CHECKER = TypeChecker(tuple[str, str])


class _DocRecord(msgspec.Struct, frozen=True):
    # The target's id for the doc, in the doc's appProperties: Google assigns
    # the doc's own id when the sink creates it, after this record is fixed.
    tag: str
    content_fingerprint: bytes
    # The blocks the declaration had: a later write tells a block a person
    # deleted from one that is new.
    block_ids: tuple[str, ...]


@dataclass(frozen=True)
class _DocAction:
    key: _DocKey
    tags: tuple[str, ...]
    blocks: tuple[_markdown.Block, ...] | None  # None: trash the doc
    written_blocks: frozenset[str] | None


def _content_fingerprint(content: str) -> bytes:
    return fingerprint_str(f"{_FORMAT_VERSION}\n{content}")


class _DocHandler(coco.TargetHandler[str, _DocRecord, None]):
    def reconcile(
        self,
        key: coco.StableKey,
        desired_target_state: str | coco.NonExistenceType,
        prev_possible_records: Collection[_DocRecord],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_DocAction, _DocRecord, None] | None:
        doc_key = _DocKey(*_KEY_CHECKER.check(key))
        tags = sorted({record.tag for record in prev_possible_records})
        if coco.is_non_existence(desired_target_state):
            if not tags:
                return None
            return coco.TargetReconcileOutput(
                action=_DocAction(doc_key, tuple(tags), None, None),
                sink=_sink,
                tracking_record=coco.NON_EXISTENCE,
            )
        content_fp = _content_fingerprint(desired_target_state)
        tag = tags[0] if tags else uuid.uuid4().hex
        if (
            not prev_may_be_missing
            and prev_possible_records
            and all(r.content_fingerprint == content_fp for r in prev_possible_records)
        ):
            return None
        blocks = tuple(_markdown.parse_blocks(desired_target_state))
        written: frozenset[str] | None = None
        if len(prev_possible_records) == 1 and not prev_may_be_missing:
            written = frozenset(next(iter(prev_possible_records)).block_ids)
        return coco.TargetReconcileOutput(
            action=_DocAction(doc_key, (tag,), blocks, written),
            sink=_sink,
            tracking_record=_DocRecord(
                tag=tag,
                content_fingerprint=content_fp,
                block_ids=tuple(block.block_id for block in blocks),
            ),
        )


# --- talking to Google ---------------------------------------------------------


def _explained(exc: HttpError) -> Exception | None:
    """What to do about a refusal Google explains; ``None`` for any other."""
    try:
        error = json.loads(exc.content)["error"]
    except (ValueError, KeyError, TypeError):
        return None
    if not isinstance(error, dict):
        return None
    reasons = {
        str(item.get("reason"))
        for item in (error.get("details") or []) + (error.get("errors") or [])
        if isinstance(item, dict)
    }
    message = str(error.get("message") or "")
    if reasons & {"SERVICE_DISABLED", "accessNotConfigured"}:
        return RuntimeError(
            "A Google API the Google Docs target needs is not enabled for the OAuth "
            f"client's project: {message} Enable the Google Docs API and the Google "
            "Drive API there (APIs & Services > Library), then run again."
        )
    if reasons & {"ACCESS_TOKEN_SCOPE_INSUFFICIENT", "insufficientPermissions"}:
        return SignInRequiredError(
            "The stored sign-in does not grant access to the files CocoIndex creates "
            "in Google Drive. Sign in with `cocoindex auth sign-in google-docs`."
        )
    return None


def _execute(request: Any, retries: int = _RETRIES) -> Any:
    try:
        return request.execute(num_retries=retries)
    except HttpError as exc:
        explained = _explained(exc)
        if explained is None:
            raise
        raise explained from exc


class _Google:
    """The Docs and Drive calls of one write, signed with the account's token."""

    def __init__(self, account: UserAccount) -> None:
        http = AuthorizedHttp(account._credentials(), http=_new_http())
        self._docs = build("docs", "v1", http=http, cache_discovery=False)
        self._drive = build("drive", "v3", http=http, cache_discovery=False)

    def find(self, tag: str) -> str | None:
        """The doc tagged ``tag`` that is not in the trash; the oldest, if copies carry the tag."""
        query = (
            f"appProperties has {{ key='{_TAG_PROPERTY}' and value='{tag}' }} "
            "and trashed = false"
        )
        response = _execute(
            self._drive.files().list(
                q=query, spaces="drive", fields="files(id, createdTime)", pageSize=100
            )
        )
        files = sorted(
            response.get("files", []),
            key=lambda f: (str(f.get("createdTime", "")), str(f["id"])),
        )
        return str(files[0]["id"]) if files else None

    def create(self, name: str, tag: str) -> str:
        body = {
            "name": name,
            "mimeType": _DOC_MIME,
            "appProperties": {_TAG_PROPERTY: tag},
        }
        created = _execute(
            self._drive.files().create(body=body, fields="id"), retries=0
        )
        return str(created["id"])

    def trash(self, doc_id: str) -> None:
        try:
            _execute(
                self._drive.files().update(
                    fileId=doc_id, body={"trashed": True}, supportsAllDrives=True
                )
            )
        except HttpError as exc:
            if exc.resp.status != 404:
                raise

    def get(self, doc_id: str) -> Any:
        return _execute(
            self._docs.documents().get(
                documentId=doc_id, fields="revisionId,body(content),namedRanges"
            )
        )

    def update(
        self, doc_id: str, requests: list[dict[str, Any]], revision_id: str
    ) -> bool:
        """Apply ``requests`` to revision ``revision_id``; ``False`` when the doc moved on."""
        body = {
            "requests": requests,
            "writeControl": {"requiredRevisionId": revision_id},
        }
        try:
            _execute(self._docs.documents().batchUpdate(documentId=doc_id, body=body))
        except HttpError as exc:
            if exc.resp.status != 400:
                raise
            current = _execute(
                self._docs.documents().get(documentId=doc_id, fields="revisionId")
            )
            if current.get("revisionId") == revision_id:
                raise
            return False
        return True


def _apply_action(account: UserAccount, action: _DocAction) -> None:
    google = _Google(account)
    name = action.key.name
    if action.blocks is None:
        for tag in action.tags:
            doc_id = google.find(tag)
            if doc_id is not None:
                google.trash(doc_id)
                _logger.info(
                    "Moved Google Doc %r to the trash: %s", name, doc_url(doc_id)
                )
        return

    (tag,) = action.tags
    written_blocks = action.written_blocks
    doc_id = google.find(tag)
    if doc_id is None:
        doc_id = google.create(name, tag)
        # A new doc (the first, or one replacing a doc that was deleted or
        # trashed) has none of the blocks yet; none of them was deleted by a person.
        written_blocks = None
        _logger.info("Created Google Doc %r: %s", name, doc_url(doc_id))

    for _ in range(_WRITE_ATTEMPTS):
        state = _docs.read_document(google.get(doc_id))
        plan = _docs.plan_write(state, action.blocks, written_blocks)
        if not plan.requests or google.update(doc_id, plan.requests, state.revision_id):
            break
    else:
        raise RuntimeError(
            f"Google Doc {name!r} ({doc_url(doc_id)}) changed every time CocoIndex "
            f"read it to write, {_WRITE_ATTEMPTS} times; run again once it is quiet."
        )
    if plan.person_edits:
        _logger.warning(
            "Google Doc %r (%s) keeps a person's edits; CocoIndex left these blocks "
            "as they are: %s",
            name,
            doc_url(doc_id),
            "; ".join(_describe(edit) for edit in plan.person_edits),
        )


def _describe(edit: _docs.PersonEdit) -> str:
    if edit.kind == "deleted":
        what = "deleted; the flow still declares it, and it was not written back"
    elif edit.kind == "undeclared":
        what = "edited, then no longer declared: left with the person's text"
    elif edit.withheld:
        what = "edited; the flow's newer version was not written"
    else:
        what = "edited"
    return f"{edit.label!r} ({what})"


async def _apply(
    context_provider: ContextProvider, actions: Sequence[_DocAction]
) -> None:
    for action in actions:
        account = context_provider.get(action.key.account_key, UserAccount)
        await asyncio.to_thread(_apply_action, account, action)


_sink = coco.TargetActionSink.from_async_fn(_apply)
_provider = coco.register_root_target_states_provider(
    "cocoindex/google_docs", _DocHandler()
)


# --- public API ----------------------------------------------------------------


@coco.fn
def declare_doc(account: ContextKey[UserAccount], name: str, content: str) -> None:
    """Declare a Google Doc named ``name`` whose content is ``content``, in Markdown (experimental).

    The target creates the doc in the account's My Drive on the first run and
    keeps it in step after that, rewriting only the blocks (a heading and its
    paragraphs) whose content changed. A block a person edited since the
    target wrote it keeps the person's text and is reported in a warning.
    When the declaration goes away, the doc moves to the trash.

    Args:
        account: The ``ContextKey`` the run provides the signed-in
            ``UserAccount`` under. Its name, with ``name``, identifies the doc.
        name: The doc's title when it is created, and its identity: a person
            may rename or move the doc, and the target keeps writing to it.
        content: Markdown: headings, paragraphs, bullet lists, links, bold,
            italic and inline code.
    """
    if not name:
        raise ValueError("declare_doc needs a non-empty name")
    target_state = _provider.target_state(_DocKey(account.key, name), content)
    coco.declare_target_state(target_state)


__all__ = ["declare_doc"]

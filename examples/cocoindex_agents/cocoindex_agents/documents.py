"""Documents: files the model reads natively (a PDF) rather than as text.

What enters the transcript for such a file is a small **reference** — its
key, media type, size and content digest — never its bytes: the transcript
is the control plane and the memo key of every later turn (DESIGN.md §1,
§4), and a memo value must stay small. The bytes travel beside it: a tool
that reads a document attaches it to the session's ``Documents`` store,
the store reaches the model client through the turn's hints (excluded from
the memo key, never stored), and the client puts the document into the
request right after the step whose result refers to it. A changed file
changes the digest in the reference, so exactly the turns that saw it miss;
an unchanged one renders byte-identical, so the turn memo and the
provider's prompt cache both hit.

Which files are documents is decided by media type, from the file name,
never by flow. Today the set is ``application/pdf`` alone: the one type
every provider accepts inline through LiteLLM's ``file`` content part
(OpenAI chat completions take only PDFs that way; Anthropic and Gemini
take more). A file of any other type is read as text, as before.
"""

from __future__ import annotations

import base64
import mimetypes
from dataclasses import dataclass

from cocoindex.connectorkits.fingerprint import fingerprint_bytes

from .spec import Json

DOCUMENT_MEDIA_TYPES = frozenset({"application/pdf"})


def document_media_type(name: str) -> str | None:
    """The media type of a file the model reads natively, from its name; ``None`` for text."""
    media_type, _ = mimetypes.guess_type(name)
    return media_type if media_type in DOCUMENT_MEDIA_TYPES else None


@dataclass(frozen=True)
class Document:
    """One document a tool read: its bytes, and the reference that stands for it in the transcript."""

    key: str
    media_type: str
    data: bytes
    digest: str  # hex of the engine's fingerprint of the bytes (``fingerprint_bytes``)

    @classmethod
    def of(cls, key: str, media_type: str, data: bytes) -> Document:
        return cls(key, media_type, data, fingerprint_bytes(data).hex())

    def reference(self) -> dict[str, Json]:
        """What the transcript holds: small, and different whenever the bytes are."""
        return {
            "key": self.key,
            "media_type": self.media_type,
            "bytes": len(self.data),
            "digest": self.digest,
        }

    def data_url(self) -> str:
        return f"data:{self.media_type};base64," + base64.b64encode(self.data).decode(
            "ascii"
        )


def is_reference(value: Json) -> bool:
    """A tool result value that is a document reference (``Document.reference()``)."""
    return (
        isinstance(value, dict)
        and value.get("media_type") in DOCUMENT_MEDIA_TYPES
        and isinstance(value.get("digest"), str)
    )


class Documents:
    """The documents one session run attached, by key and digest. Run-local; never stored.

    One store per session, shared by its tools and its turns: a tool attaches
    what it read and returns the reference; the client asks for the documents
    a result refers to when it renders that step. A reference always resolves
    within a run, because tools execute in the session's body before any turn
    that could render their results.
    """

    def __init__(self) -> None:
        # By key and digest: two files with the same bytes are two documents.
        self._documents: dict[tuple[str, str], Document] = {}

    def attach(self, document: Document) -> dict[str, Json]:
        self._documents[document.key, document.digest] = document
        return document.reference()

    def referenced_by(self, value: Json) -> list[Document]:
        """The documents a tool result refers to: a reference, or a list of them."""
        out: list[Document] = []
        for item in value if isinstance(value, list) else [value]:
            if not is_reference(item):
                continue
            document = self._documents.get((item["key"], item["digest"]))
            if document is None:
                raise LookupError(
                    f"document {item['key']!r} is referenced but was not attached in this run"
                )
            out.append(document)
        return out

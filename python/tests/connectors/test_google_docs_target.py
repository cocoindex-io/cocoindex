"""The Google Docs target, against a fake Google Docs and Drive API.

No network and no real account. ``FakeGoogle`` plays the Docs API (a document
kept as UTF-16 code units, with Docs' style inheritance on insertion, named
ranges and revision ids), the Drive API (doc creation, lookup by
``appProperties``, trash) behind the ``httplib2`` transport the target signs
with, and Google's OAuth endpoints behind ``httpx.MockTransport``. Its named
ranges grow when text is inserted at either of their boundaries, the least
helpful reading of Docs' behavior, so the target must not depend on it.

A *comment* in the fake is anchored to the code units of a quoted text, as a
Docs comment is: it survives only if those units were never deleted.
"""

from __future__ import annotations

import copy
import json
import logging
import os
import pathlib
import socket
import stat
import threading
import urllib.parse
from dataclasses import dataclass, field
from typing import Any, Callable

import pytest
from click.testing import CliRunner

import cocoindex as coco
from tests import common

try:
    import httplib2  # type: ignore[import-untyped]
    import httpx

    from cocoindex.connectors import google_docs, google_drive
    from cocoindex.connectors.google_docs import _auth, _docs, _markdown, _target
    from cocoindex.connectors.google_drive import _oauth

    HAS_GOOGLE = True
except ImportError:
    HAS_GOOGLE = False

pytestmark = pytest.mark.skipif(
    not HAS_GOOGLE,
    reason="google-auth / google-api-python-client / httpx are not installed",
)

coco_env = common.create_test_env(__file__)

GDOCS: coco.ContextKey[google_docs.UserAccount] = coco.ContextKey("gdocs")
CLIENT_ID = "123-fake.apps.googleusercontent.com"
CLIENT_SECRET = "fake-client-secret"
EMAIL = "someone@example.com"


# --- the fake document ----------------------------------------------------------


@dataclass(eq=False)
class _Unit:
    """One UTF-16 code unit; a newline carries its paragraph's style and bullet."""

    ch: str
    style: dict[str, Any]
    para: dict[str, Any] | None = None


class BadRequest(Exception):
    pass


def _code_units(text: str) -> list[str]:
    raw = text.encode("utf-16-le", "surrogatepass")
    return [
        raw[i : i + 2].decode("utf-16-le", "surrogatepass")
        for i in range(0, len(raw), 2)
    ]


class FakeDoc:
    """A document body: index ``i`` is ``units[i - 1]``; index 0 is the section break."""

    def __init__(self, doc_id: str) -> None:
        self.doc_id = doc_id
        self.units = [
            _Unit(
                "\n", {}, {"style": {"namedStyleType": "NORMAL_TEXT"}, "bullet": None}
            )
        ]
        self.ranges: dict[str, tuple[str, int, int]] = {}  # id -> (name, start, end)
        self.revision = 1
        self._next_range = 0
        self.comments: dict[str, tuple[str, list[_Unit]]] = {}

    @property
    def end(self) -> int:
        return len(self.units) + 1

    def spans(self) -> list[tuple[int, int]]:
        spans, start = [], 1
        for i, unit in enumerate(self.units):
            if unit.ch == "\n":
                spans.append((start, i + 2))
                start = i + 2
        return spans

    def _overlapping(self, start: int, end: int) -> list[tuple[int, int]]:
        return [(s, e) for s, e in self.spans() if s < end and e > start]

    def _newline_at_or_after(self, index: int) -> _Unit:
        for unit in self.units[index - 1 :]:
            if unit.ch == "\n":
                return unit
        raise BadRequest("no paragraph")

    # Editing, as the Docs API does it.

    def insert(self, index: int, text: str) -> None:
        if not 1 <= index <= self.end - 1 or not text:
            raise BadRequest(f"insertText at {index} (end {self.end})")
        pos = index - 1
        style = dict(self.units[pos - 1].style) if pos > 0 else {}
        para = self._newline_at_or_after(index).para  # the paragraph being split
        new = [
            _Unit(ch, dict(style), copy.deepcopy(para) if ch == "\n" else None)
            for ch in _code_units(text)
        ]
        self.units[pos:pos] = new
        n = len(new)
        for rid, (name, s, e) in list(self.ranges.items()):
            if index < s:
                s, e = s + n, e + n
            elif index <= e:
                e += n  # grows at either boundary: the least helpful reading
            self.ranges[rid] = (name, s, e)

    def delete(self, start: int, end: int, *, keep_last: bool = True) -> None:
        if not 1 <= start < end <= (self.end - 1 if keep_last else self.end):
            raise BadRequest(f"deleteContentRange [{start}, {end}) (end {self.end})")
        del self.units[start - 1 : end - 1]
        n = end - start

        def shift(x: int) -> int:
            return x if x <= start else (start if x <= end else x - n)

        for rid, (name, s, e) in list(self.ranges.items()):
            s, e = shift(s), shift(e)
            if s >= e:
                del self.ranges[rid]
            else:
                self.ranges[rid] = (name, s, e)

    def _check_range(self, start: int, end: int) -> None:
        if not 1 <= start < end <= self.end:
            raise BadRequest(f"range [{start}, {end}) (end {self.end})")

    def apply(self, request: dict[str, Any]) -> None:
        ((kind, body),) = request.items()
        rng = body.get("range") or {}
        start, end = int(rng.get("startIndex", 0)), int(rng.get("endIndex", 0))
        if kind == "insertText":
            self.insert(body["location"]["index"], body["text"])
        elif kind == "deleteContentRange":
            self.delete(start, end)
        elif kind == "updateTextStyle":
            self._check_range(start, end)
            fields = body["fields"]
            if not fields:
                raise BadRequest("no fields")
            for unit in self.units[start - 1 : end - 1]:
                if fields == "*":
                    unit.style = copy.deepcopy(body["textStyle"])
                    continue
                for name in fields.split(","):
                    if name in body["textStyle"]:
                        unit.style[name] = copy.deepcopy(body["textStyle"][name])
                    else:
                        unit.style.pop(name, None)
        elif kind == "updateParagraphStyle":
            self._check_range(start, end)
            for s, e in self._overlapping(start, end):
                para = self.units[e - 2].para
                assert para is not None
                if body["fields"] == "*":
                    para["style"] = copy.deepcopy(body["paragraphStyle"])
                else:
                    for name in body["fields"].split(","):
                        para["style"][name] = body["paragraphStyle"][name]
        elif kind == "deleteParagraphBullets":
            self._check_range(start, end)
            for s, e in self._overlapping(start, end):
                para = self.units[e - 2].para
                assert para is not None
                if para["bullet"] is not None:
                    # Docs keeps the nesting visible as an indent.
                    para["style"]["indentStart"] = {
                        "magnitude": 36 * (para["bullet"] + 1)
                    }
                    para["bullet"] = None
        elif kind == "createParagraphBullets":
            self._check_range(start, end)
            for s, e in reversed(self._overlapping(start, end)):
                tabs = 0
                while self.units[s - 1 + tabs].ch == "\t":
                    tabs += 1
                if tabs:
                    self.delete(s, s + tabs)
                para = self.units[e - 2 - tabs].para
                assert para is not None
                para["bullet"] = min(tabs, 8)
        elif kind == "createNamedRange":
            self._check_range(start, end)
            self._next_range += 1
            self.ranges[f"nr-{self._next_range}"] = (body["name"], start, end)
        elif kind == "deleteNamedRange":
            if body.get("namedRangeId") not in self.ranges:
                raise BadRequest(f"no named range {body.get('namedRangeId')}")
            del self.ranges[body["namedRangeId"]]
        else:
            raise BadRequest(f"unknown request {kind}")

    def clone(self) -> tuple[FakeDoc, dict[int, tuple[_Unit, _Unit]]]:
        """A copy to try a batch on, and (copy, original) by the copy's id.

        The map holds every copy, so no unit the batch creates can take the id
        of a copy the batch deleted.
        """
        trial = FakeDoc(self.doc_id)
        trial.units = [
            _Unit(u.ch, copy.deepcopy(u.style), copy.deepcopy(u.para))
            for u in self.units
        ]
        trial.ranges = dict(self.ranges)
        trial._next_range = self._next_range
        return trial, {id(c): (c, u) for c, u in zip(trial.units, self.units)}

    def commit(self, trial: FakeDoc, origin: dict[int, tuple[_Unit, _Unit]]) -> None:
        """Take the batch's result, keeping the unit objects that survived it."""
        units = []
        for c in trial.units:
            pair = origin.get(id(c))
            if pair is None:
                units.append(c)
            else:
                original = pair[1]
                original.style, original.para = c.style, c.para
                units.append(original)
        self.units, self.ranges = units, trial.ranges
        self._next_range = trial._next_range

    # What documents.get returns.

    def to_json(self) -> dict[str, Any]:
        content: list[dict[str, Any]] = [
            {"endIndex": 1, "sectionBreak": {"sectionStyle": {}}}
        ]
        for s, e in self.spans():
            units = self.units[s - 1 : e - 1]
            elements = []
            i = 0
            while i < len(units):
                j = i
                while j + 1 < len(units) and units[j + 1].style == units[i].style:
                    j += 1
                text = "".join(u.ch for u in units[i : j + 1])
                text = text.encode("utf-16-le", "surrogatepass").decode("utf-16-le")
                elements.append(
                    {
                        "startIndex": s + i,
                        "endIndex": s + j + 1,
                        "textRun": {
                            "content": text,
                            "textStyle": copy.deepcopy(units[i].style),
                        },
                    }
                )
                i = j + 1
            para = units[-1].para
            assert para is not None
            paragraph: dict[str, Any] = {
                "elements": elements,
                "paragraphStyle": copy.deepcopy(para["style"]),
            }
            if para["bullet"] is not None:
                paragraph["bullet"] = {"listId": "kix.list"}
                if para["bullet"]:
                    paragraph["bullet"]["nestingLevel"] = para["bullet"]
            content.append({"startIndex": s, "endIndex": e, "paragraph": paragraph})
        named: dict[str, Any] = {}
        for rid, (name, s, e) in self.ranges.items():
            named.setdefault(name, {"name": name, "namedRanges": []})[
                "namedRanges"
            ].append(
                {
                    "namedRangeId": rid,
                    "name": name,
                    "ranges": [{"startIndex": s, "endIndex": e}],
                }
            )
        return {
            "documentId": self.doc_id,
            "revisionId": f"rev-{self.revision}",
            "body": {"content": content},
            "namedRanges": named,
        }

    # What a test reads, and what a person does.

    def lines(self) -> list[tuple[str, int | None, str]]:
        """Each paragraph as (named style, nesting level, text), the empty last one left out."""
        out = []
        for s, e in self.spans()[:-1]:
            units = self.units[s - 1 : e - 2]
            text = "".join(u.ch for u in units).encode("utf-16-le", "surrogatepass")
            para = self.units[e - 2].para
            assert para is not None
            out.append(
                (
                    para["style"].get("namedStyleType", ""),
                    para["bullet"],
                    text.decode("utf-16-le"),
                )
            )
        return out

    def find(self, text: str) -> int:
        joined = "".join(u.ch for u in self.units)
        offset = joined.find("".join(_code_units(text)))
        assert offset >= 0, f"{text!r} not in the doc"
        return offset + 1

    def range_names(self) -> dict[str, str]:
        """Named range id -> name."""
        return {rid: name for rid, (name, _, _) in self.ranges.items()}

    def range_text(self, rid: str) -> str:
        _, s, e = self.ranges[rid]
        return "".join(u.ch for u in self.units[s - 1 : e - 1])

    def person_replace(self, old: str, new: str) -> None:
        index = self.find(old)
        self.insert(index + len(_code_units(old)), new)
        self.delete(index, index + len(_code_units(old)))
        self.revision += 1

    def person_delete_paragraph(self, text: str) -> None:
        index = self.find(text)
        span = next((s, e) for s, e in self.spans() if s <= index < e)
        self.delete(*span)
        self.revision += 1

    def comment(self, quoted: str) -> str:
        index = self.find(quoted)
        cid = f"c{len(self.comments) + 1}"
        units = self.units[index - 1 : index - 1 + len(_code_units(quoted))]
        self.comments[cid] = (quoted, units)
        return cid

    def comment_survives(self, cid: str) -> bool:
        quoted, units = self.comments[cid]
        alive = {id(u) for u in self.units}
        return (
            all(id(u) in alive for u in units)
            and "".join(u.ch for u in units) == quoted
        )


# --- the fake Google --------------------------------------------------------------


@dataclass
class FakeGoogle:
    docs: dict[str, FakeDoc] = field(default_factory=dict)
    files: dict[str, dict[str, Any]] = field(default_factory=dict)
    valid_tokens: set[str] = field(default_factory=set)
    refresh_tokens: set[str] = field(default_factory=lambda: {"refresh-1"})
    issued: int = 0
    calls: list[tuple[str, str]] = field(default_factory=list)  # (method, path)
    token_requests: list[dict[str, str]] = field(default_factory=list)
    # Runs once right after the next documents.get: a person typing meanwhile.
    after_read: Callable[[FakeDoc], None] | None = None
    disabled_api: str | None = None  # "docs" refuses every Docs call as not enabled
    grant_scope: str = ""

    def issue(self) -> str:
        self.issued += 1
        token = f"access-{self.issued}"
        self.valid_tokens.add(token)
        return token

    def writes(self) -> list[tuple[str, str]]:
        return [call for call in self.calls if call[0] != "GET"]

    def only_doc(self) -> FakeDoc:
        (doc,) = [self.docs[i] for i, f in self.files.items() if not f["trashed"]]
        return doc

    # The Docs and Drive APIs, as googleapiclient calls them.

    def serve(
        self, uri: str, method: str, body: Any, headers: dict[str, str]
    ) -> tuple[int, Any]:
        auth = {k.lower(): v for k, v in (headers or {}).items()}.get(
            "authorization", ""
        )
        if auth.removeprefix("Bearer ") not in self.valid_tokens:
            return 401, {"error": {"code": 401, "message": "Invalid Credentials"}}
        parts = urllib.parse.urlsplit(uri)
        path = urllib.parse.unquote(parts.path)
        query = dict(urllib.parse.parse_qsl(parts.query))
        if isinstance(body, bytes):
            body = body.decode()
        payload = json.loads(body) if body else None
        self.calls.append((method, path))
        if parts.netloc == "docs.googleapis.com":
            if self.disabled_api == "docs":
                return 403, {
                    "error": {
                        "code": 403,
                        "message": "Google Docs API has not been used in project 42 "
                        "before or it is disabled.",
                        "status": "PERMISSION_DENIED",
                        "details": [
                            {
                                "@type": "type.googleapis.com/google.rpc.ErrorInfo",
                                "reason": "SERVICE_DISABLED",
                            }
                        ],
                    }
                }
            return self._docs_api(method, path, query, payload)
        assert parts.netloc == "www.googleapis.com", uri
        return self._drive_api(method, path, query, payload)

    def _docs_api(
        self, method: str, path: str, query: dict[str, str], payload: Any
    ) -> tuple[int, Any]:
        rest = path.removeprefix("/v1/documents/")
        if method == "GET":
            doc = self.docs.get(rest)
            if doc is None:
                return 404, {"error": {"code": 404, "message": "not found"}}
            if query.get("fields") == "revisionId":
                return 200, {"revisionId": f"rev-{doc.revision}"}
            result = doc.to_json()
            if self.after_read is not None:
                edit, self.after_read = self.after_read, None
                edit(doc)
            return 200, result
        doc_id = rest.removesuffix(":batchUpdate")
        doc = self.docs[doc_id]
        required = (payload.get("writeControl") or {}).get("requiredRevisionId")
        if required is not None and required != f"rev-{doc.revision}":
            return 400, {
                "error": {
                    "code": 400,
                    "message": "The required revision ID does not match.",
                }
            }
        # All or nothing, as Docs applies a batch.
        trial, origin = doc.clone()
        try:
            for request in payload["requests"]:
                trial.apply(request)
        except BadRequest as exc:
            return 400, {"error": {"code": 400, "message": str(exc)}}
        doc.commit(trial, origin)
        doc.revision += 1
        return 200, {
            "documentId": doc_id,
            "replies": [{} for _ in payload["requests"]],
            "writeControl": {"requiredRevisionId": f"rev-{doc.revision}"},
        }

    def _drive_api(
        self, method: str, path: str, query: dict[str, str], payload: Any
    ) -> tuple[int, Any]:
        if path == "/drive/v3/files" and method == "POST":
            doc_id = f"doc-{len(self.files) + 1}"
            self.files[doc_id] = {
                "id": doc_id,
                "name": payload["name"],
                "mimeType": payload["mimeType"],
                "appProperties": dict(payload.get("appProperties") or {}),
                "trashed": False,
                "createdTime": f"2026-11-26T00:00:{len(self.files):02d}Z",
            }
            self.docs[doc_id] = FakeDoc(doc_id)
            return 200, {"id": doc_id}
        if path == "/drive/v3/files" and method == "GET":
            q = query["q"]
            key, value = (
                q.split("key='")[1].split("'")[0],
                q.split("value='")[1].split("'")[0],
            )
            assert "trashed = false" in q
            found = [
                {
                    "id": f["id"],
                    "createdTime": f["createdTime"],
                    "appProperties": dict(f["appProperties"]),
                }
                for f in self.files.values()
                if f["appProperties"].get(key) == value and not f["trashed"]
            ]
            return 200, {"files": found}
        file_id = path.removeprefix("/drive/v3/files/")
        if method == "PATCH" and file_id in self.files:
            # Drive merges appProperties key by key; the doc's revision is
            # untouched, as metadata is not content.
            payload = dict(payload)
            self.files[file_id]["appProperties"].update(
                payload.pop("appProperties", None) or {}
            )
            self.files[file_id].update(payload)
            return 200, {"id": file_id}
        return 404, {"error": {"code": 404, "message": "File not found"}}

    # Google's OAuth endpoints, as the sign-in and the token refresh call them (httpx).

    def oauth(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url).split("?")[0]
        if url == _oauth.TOKEN_ENDPOINT:
            form = dict(urllib.parse.parse_qsl(request.content.decode()))
            self.token_requests.append(form)
            assert (
                form["client_id"] == CLIENT_ID
                and form["client_secret"] == CLIENT_SECRET
            )
            if form["grant_type"] == "refresh_token":
                if form["refresh_token"] not in self.refresh_tokens:
                    return httpx.Response(400, json={"error": "invalid_grant"})
                return httpx.Response(
                    200, json={"access_token": self.issue(), "expires_in": 3599}
                )
            return httpx.Response(
                200,
                json={
                    "access_token": self.issue(),
                    "refresh_token": "refresh-1",
                    "expires_in": 3599,
                    "scope": self.grant_scope,
                },
            )
        if url == _oauth.ABOUT_ENDPOINT:
            return httpx.Response(200, json={"user": {"emailAddress": EMAIL}})
        if url == _oauth.REVOCATION_ENDPOINT:
            return httpx.Response(200)
        return httpx.Response(404)


class FakeHttp:
    """The ``httplib2.Http`` the target signs requests on, answered by ``FakeGoogle``."""

    def __init__(self, google: FakeGoogle) -> None:
        self.google = google

    def request(
        self,
        uri: str,
        method: str = "GET",
        body: Any = None,
        headers: dict[str, str] | None = None,
        **_: Any,
    ) -> tuple[Any, bytes]:
        status, payload = self.google.serve(uri, method, body, headers or {})
        response = httplib2.Response(
            {"status": status, "content-type": "application/json"}
        )
        return response, json.dumps(payload).encode()


# --- fixtures ----------------------------------------------------------------------


def _write_token_file(path: pathlib.Path, access_token: str, expires_at: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "version": _oauth.TOKEN_FILE_VERSION,
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "refresh_token": "refresh-1",
                "access_token": access_token,
                "expires_at": expires_at,
                "email": EMAIL,
            }
        )
    )
    path.chmod(0o600)


@dataclass
class Setup:
    google: FakeGoogle
    token_path: pathlib.Path
    content: dict[str, str | None] = field(default_factory=lambda: {"content": None})

    def app(self, name: str) -> coco.App[[], None]:
        content = self.content

        @coco.fn
        def main() -> None:
            if content["content"] is not None:
                google_docs.declare_doc(GDOCS, "Weekly brief", content["content"])

        return coco.App(coco.AppConfig(name=name, environment=coco_env), main)

    def run(self, app: coco.App[[], None], content: str | None) -> None:
        self.content["content"] = content
        self.google.calls.clear()
        app.update_blocking()


@pytest.fixture
def setup(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> Setup:
    google = FakeGoogle(grant_scope=_auth.DRIVE_FILE_SCOPE)
    monkeypatch.setattr(_target, "_new_http", lambda: FakeHttp(google))
    monkeypatch.setattr(
        _oauth,
        "_http_client",
        lambda: httpx.Client(transport=httpx.MockTransport(google.oauth)),
    )
    token_path = tmp_path / "google_docs_token.json"
    _write_token_file(token_path, google.issue(), expires_at=4102444800.0)  # 2100
    coco_env.context_provider.provide(GDOCS, google_docs.UserAccount(token_path))
    return Setup(google, token_path)


BRIEF = """\
Kept current from the sources.

# Brief

## Decisions

- Ship the **connector** in 1.3.
  - Docs come with it, see [the plan](https://example.com/plan).
- Keep `drive.readonly` users unaffected.

## Risks

Google may change *named range* behavior.

## Sources

https://example.com/a and https://example.com/b.
"""


def _blocks(markdown: str) -> list[_markdown.Block]:
    return _markdown.parse_blocks(markdown)


def _ranges_by_block(doc: FakeDoc) -> dict[str, tuple[str, str]]:
    """Block id -> (named range id, the range's text)."""
    out = {}
    for rid, name in doc.range_names().items():
        assert name.startswith(_docs.RANGE_PREFIX)
        out[name.split(":")[1]] = (rid, doc.range_text(rid))
    return out


# --- creating ----------------------------------------------------------------------


def test_the_first_run_creates_the_doc_with_a_named_range_per_block(
    setup: Setup,
) -> None:
    setup.run(setup.app("create"), BRIEF)
    google = setup.google
    (file,) = google.files.values()
    assert file["name"] == "Weekly brief"
    assert file["mimeType"] == "application/vnd.google-apps.document"
    # The tag the doc is found by, and the stamp of the target's last write.
    assert set(file["appProperties"]) == {"cocoindexDoc", "cocoindexWrite"}
    doc = google.only_doc()
    assert doc.lines() == [
        ("NORMAL_TEXT", None, "Kept current from the sources."),
        ("HEADING_1", None, "Brief"),
        ("HEADING_2", None, "Decisions"),
        ("NORMAL_TEXT", 0, "Ship the connector in 1.3."),
        ("NORMAL_TEXT", 1, "Docs come with it, see the plan."),
        ("NORMAL_TEXT", 0, "Keep drive.readonly users unaffected."),
        ("HEADING_2", None, "Risks"),
        ("NORMAL_TEXT", None, "Google may change named range behavior."),
        ("HEADING_2", None, "Sources"),
        ("NORMAL_TEXT", None, "https://example.com/a and https://example.com/b."),
    ]
    # Inline styles, as Docs reports them.
    runs = {
        (u.ch, json.dumps(u.style, sort_keys=True))
        for u in doc.units
        if u.style and u.ch not in "\n"
    }
    styles = {style for _, style in runs}
    assert any('"bold": true' in s for s in styles)
    assert any('"italic": true' in s for s in styles)
    assert any('"fontFamily": "Roboto Mono"' in s for s in styles)
    assert any('"url": "https://example.com/plan"' in s for s in styles)
    # One named range per block, spanning exactly its paragraphs.
    blocks = _blocks(BRIEF)
    by_block = _ranges_by_block(doc)
    assert set(by_block) == {b.block_id for b in blocks}
    assert (
        by_block[blocks[3].block_id][1]
        == "Risks\nGoogle may change named range behavior.\n"
    )
    # Read back, the doc is exactly what was declared: nothing left to write.
    state = _docs.read_document(doc.to_json())
    assert (
        _docs.plan_write(state, blocks, frozenset(b.block_id for b in blocks)).requests
        == []
    )
    # One create, one batchUpdate, one stamp.
    assert [m for m, _ in google.writes()] == ["POST", "POST", "PATCH"]


def test_an_unchanged_rerun_makes_no_api_call(setup: Setup) -> None:
    app = setup.app("unchanged")
    setup.run(app, BRIEF)
    setup.run(app, BRIEF)
    assert setup.google.calls == []


def test_a_preview_plans_without_calling_google(setup: Setup) -> None:
    app = setup.app("preview")
    setup.content["content"] = BRIEF
    actions = app.update_blocking(preview=True)
    assert actions is not None and len(actions) == 1
    assert setup.google.calls == [] and setup.google.files == {}


# --- in-place updates ------------------------------------------------------------------


def test_a_changed_section_rewrites_only_its_changed_paragraphs(setup: Setup) -> None:
    app = setup.app("change_one")
    setup.run(app, BRIEF)
    doc = setup.google.only_doc()
    before = _ranges_by_block(doc)
    on_risks = doc.comment("Google may change")
    on_kept_bullet = doc.comment("Ship the")
    on_sources = doc.comment("https://example.com/a")

    changed = BRIEF.replace(
        "- Keep `drive.readonly` users unaffected.",
        "- Keep `drive.readonly` users **entirely** unaffected.",
    )
    setup.run(app, changed)

    # One batchUpdate, and the stamp.
    assert [m for m, _ in setup.google.writes()] == ["POST", "PATCH"]
    assert (
        "NORMAL_TEXT",
        0,
        "Keep drive.readonly users entirely unaffected.",
    ) in doc.lines()
    after = _ranges_by_block(doc)
    decisions = _blocks(BRIEF)[2].block_id
    for block_id, (rid, text) in before.items():
        if block_id == decisions:
            assert after[block_id][0] != rid  # recreated with the new fingerprint
            assert "entirely" in after[block_id][1]
        else:
            assert after[block_id] == (rid, text)  # the same named range, the same text
    # Comments on other sections, and on the section's unchanged bullet, survive.
    assert doc.comment_survives(on_risks)
    assert doc.comment_survives(on_sources)
    assert doc.comment_survives(on_kept_bullet)
    # And the doc reads back as declared.
    state = _docs.read_document(doc.to_json())
    assert _docs.plan_write(state, _blocks(changed), None).requests == []


def test_a_removed_section_and_a_new_one(setup: Setup) -> None:
    app = setup.app("remove_add")
    setup.run(app, BRIEF)
    doc = setup.google.only_doc()
    before = _ranges_by_block(doc)
    on_sources = doc.comment("https://example.com/b")

    without_risks = BRIEF.replace(
        "## Risks\n\nGoogle may change *named range* behavior.\n\n", ""
    )
    setup.run(app, without_risks)
    assert [text for _, _, text in doc.lines()] == [
        "Kept current from the sources.",
        "Brief",
        "Decisions",
        "Ship the connector in 1.3.",
        "Docs come with it, see the plan.",
        "Keep drive.readonly users unaffected.",
        "Sources",
        "https://example.com/a and https://example.com/b.",
    ]
    after = _ranges_by_block(doc)
    risks = _blocks(BRIEF)[3].block_id
    assert risks not in after
    assert {k: v for k, v in before.items() if k != risks} == after
    assert doc.comment_survives(on_sources)

    # A new section between two others: only the one before it has its named
    # range recreated (the new text went inside it), with the same text.
    with_open_questions = without_risks.replace(
        "## Sources", "## Open questions\n\n- Which folder?\n\n## Sources"
    )
    setup.run(app, with_open_questions)
    lines = [text for _, _, text in doc.lines()]
    assert lines[6:9] == ["Open questions", "Which folder?", "Sources"]
    final = _ranges_by_block(doc)
    blocks = _blocks(with_open_questions)
    assert final[blocks[3].block_id][1] == "Open questions\nWhich folder?\n"
    assert final[blocks[2].block_id][1] == after[blocks[2].block_id][1]
    for block in (blocks[0], blocks[1], blocks[4]):
        assert final[block.block_id] == after[block.block_id]
    assert doc.comment_survives(on_sources)
    state = _docs.read_document(doc.to_json())
    assert _docs.plan_write(state, blocks, None).requests == []


def test_a_new_first_section_and_reordered_sections(setup: Setup) -> None:
    app = setup.app("reorder")
    setup.run(app, "## A\n\none\n\n## B\n\ntwo\n\n## C\n\nthree\n")
    doc = setup.google.only_doc()
    on_b = doc.comment("two")
    setup.run(app, "# Top\n\n## A\n\none\n\n## C\n\nthree\n\n## B\n\ntwo\n")
    assert [text for _, _, text in doc.lines()] == [
        "Top",
        "A",
        "one",
        "C",
        "three",
        "B",
        "two",
    ]
    # Moving a block rewrites it: "B" or "C", whichever had to move.
    assert doc.comment_survives(on_b) or doc.lines()[3][2] == "C"
    state = _docs.read_document(doc.to_json())
    blocks = _blocks("# Top\n\n## A\n\none\n\n## C\n\nthree\n\n## B\n\ntwo\n")
    assert _docs.plan_write(state, blocks, None).requests == []


def test_text_outside_the_basic_plane_counts_utf16_units(setup: Setup) -> None:
    app = setup.app("utf16")
    setup.run(app, "## Café 🎉\n\n- 🚀 **launch** day\n\n## Next\n\nplain\n")
    setup.run(app, "## Café 🎉\n\n- 🚀 **launch** day 2\n\n## Next\n\nplain\n")
    doc = setup.google.only_doc()
    assert doc.lines() == [
        ("HEADING_2", None, "Café 🎉"),
        ("NORMAL_TEXT", 0, "🚀 launch day 2"),
        ("HEADING_2", None, "Next"),
        ("NORMAL_TEXT", None, "plain"),
    ]
    bold = "".join(u.ch for u in doc.units if u.style.get("bold"))
    assert bold == "launch"


# --- a person's edits -------------------------------------------------------------------


def _warnings(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [
        r
        for r in caplog.records
        if r.name == _target.__name__ and r.levelno >= logging.WARNING
    ]


def test_a_persons_edit_is_kept_and_reported(
    setup: Setup, caplog: pytest.LogCaptureFixture
) -> None:
    app = setup.app("person_edit")
    setup.run(app, BRIEF)
    doc = setup.google.only_doc()
    doc.person_replace("Google may change", "Google could change")
    risks_rid = _ranges_by_block(doc)[_blocks(BRIEF)[3].block_id][0]

    # The flow changes both the section the person edited and another one.
    changed = BRIEF.replace(
        "*named range* behavior", "*named range* behavior soon"
    ).replace("Ship the", "Ship the new")
    with caplog.at_level(logging.WARNING, logger=_target.__name__):
        setup.run(app, changed)

    texts = [text for _, _, text in doc.lines()]
    assert "Google could change named range behavior." in texts  # the person's, kept
    assert "Ship the new connector in 1.3." in texts  # the other change, written
    assert risks_rid in doc.ranges  # the edited block's range is not touched
    (record,) = _warnings(caplog)
    assert "'Brief > Risks' (edited; the flow's newer version was not written)" in (
        record.getMessage()
    )
    assert (
        "Weekly brief" in record.getMessage()
        and "docs.google.com" in record.getMessage()
    )

    # Once the flow says what the person wrote, the block is the target's again.
    caplog.clear()
    adopted = changed.replace(
        "Google may change *named range* behavior soon.",
        "Google could change *named range* behavior.",
    )
    with caplog.at_level(logging.WARNING, logger=_target.__name__):
        setup.run(app, adopted)
    assert not _warnings(caplog)
    blocks = _blocks(adopted)
    state = _docs.read_document(doc.to_json())
    assert state.ranges[blocks[3].block_id].written == _markdown.fingerprint(
        blocks[3].paragraphs
    )
    # The next change to it is written again.
    setup.run(app, adopted.replace("could change", "will change"))
    assert (
        "NORMAL_TEXT",
        None,
        "Google will change named range behavior.",
    ) in doc.lines()


def test_a_section_a_person_deleted_stays_deleted(
    setup: Setup, caplog: pytest.LogCaptureFixture
) -> None:
    app = setup.app("person_delete")
    setup.run(app, BRIEF)
    doc = setup.google.only_doc()
    _, start, end = next(
        (n, s, e) for n, s, e in doc.ranges.values() if doc.find("Risks") == s
    )
    doc.delete(start, end)
    doc.revision += 1
    with caplog.at_level(logging.WARNING, logger=_target.__name__):
        setup.run(app, BRIEF.replace("Ship the", "Ship the new"))
    texts = [text for _, _, text in doc.lines()]
    assert "Risks" not in texts and "Ship the new connector in 1.3." in texts
    assert "'Brief > Risks' (deleted" in caplog.text


def test_an_edit_between_the_read_and_the_write_is_not_overwritten(
    setup: Setup,
) -> None:
    app = setup.app("concurrent")
    setup.run(app, BRIEF)
    doc = setup.google.only_doc()
    # A person edits "Risks" right after the target read the doc to write.
    setup.google.after_read = lambda d: d.person_replace("Google may", "Google might")
    setup.run(app, BRIEF.replace("Google may change", "Google may well change"))
    # The first batchUpdate was refused (stale revision); the second read saw the
    # person's edit, and kept it.
    batch_updates = [c for c in setup.google.writes() if c[1].endswith(":batchUpdate")]
    assert len(batch_updates) == 1
    assert (
        "NORMAL_TEXT",
        None,
        "Google might change named range behavior.",
    ) in doc.lines()


# --- the trash, and the doc's identity --------------------------------------------------


def test_deleting_the_declaration_trashes_the_doc(setup: Setup) -> None:
    app = setup.app("trash")
    setup.run(app, BRIEF)
    (doc_id,) = setup.google.files
    setup.run(app, None)
    assert setup.google.files[doc_id]["trashed"] is True
    assert doc_id in setup.google.docs  # in the trash, not deleted for good
    assert setup.google.writes() == [("PATCH", f"/drive/v3/files/{doc_id}")]
    # A second drop has nothing left to do.
    setup.run(app, None)
    assert setup.google.calls == []


def test_a_renamed_doc_keeps_being_written_and_a_trashed_one_is_replaced(
    setup: Setup,
) -> None:
    app = setup.app("identity")
    setup.run(app, BRIEF)
    (doc_id,) = setup.google.files
    setup.google.files[doc_id]["name"] = "Renamed by a person"
    setup.run(app, BRIEF.replace("Ship the", "Ship the new"))
    assert list(setup.google.files) == [doc_id]
    assert setup.google.files[doc_id]["name"] == "Renamed by a person"
    assert "Ship the new connector in 1.3." in [
        t for _, _, t in setup.google.docs[doc_id].lines()
    ]

    # Trashed by a person: the next change writes a new doc, in full.
    setup.google.files[doc_id]["trashed"] = True
    setup.run(app, BRIEF)
    new_doc = setup.google.only_doc()
    assert new_doc.doc_id != doc_id
    assert len(new_doc.lines()) == 10


# --- the account ------------------------------------------------------------------------


def test_an_expired_access_token_is_refreshed_during_a_run(setup: Setup) -> None:
    _write_token_file(setup.token_path, "access-stale", expires_at=0)
    setup.run(setup.app("refresh"), BRIEF)
    assert setup.google.only_doc().lines()
    (refresh,) = setup.google.token_requests
    assert (
        refresh["grant_type"] == "refresh_token"
        and refresh["refresh_token"] == "refresh-1"
    )
    stored = json.loads(setup.token_path.read_text())
    assert stored["access_token"] == f"access-{setup.google.issued}"
    assert stored["refresh_token"] == "refresh-1"
    if os.name != "nt":
        assert stat.S_IMODE(setup.token_path.stat().st_mode) == 0o600


def test_a_disabled_docs_api_says_what_to_enable(setup: Setup) -> None:
    setup.google.disabled_api = "docs"
    with pytest.raises(Exception) as excinfo:
        setup.run(setup.app("disabled"), BRIEF)
    assert "Enable the Google Docs API" in "".join(
        str(e) for e in _chain(excinfo.value)
    )


def _chain(exc: BaseException) -> list[BaseException]:
    out: list[BaseException] = []
    while exc is not None and exc not in out:
        out.append(exc)
        exc = exc.__cause__ or exc.__context__  # type: ignore[assignment]
    return out


def test_a_google_drive_account_is_not_taken_for_a_docs_one(setup: Setup) -> None:
    coco_env.context_provider.provide(
        GDOCS,
        google_drive.UserAccount(setup.token_path),  # type: ignore[arg-type]
    )
    with pytest.raises(Exception) as excinfo:
        setup.run(setup.app("drive_account"), BRIEF)
    assert "expected UserAccount" in "".join(str(e) for e in _chain(excinfo.value))
    assert setup.google.calls == []


# --- signing in -----------------------------------------------------------------------


def test_sign_in_asks_for_drive_file_only_and_keeps_its_own_token_file(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from cocoindex.cli import cli

    google = FakeGoogle(grant_scope=_auth.DRIVE_FILE_SCOPE)
    monkeypatch.setattr(
        _oauth,
        "_http_client",
        lambda: httpx.Client(transport=httpx.MockTransport(google.oauth)),
    )
    monkeypatch.setattr(socket, "getfqdn", lambda name="": name)
    client = tmp_path / "client_secret.json"
    client.write_text(
        json.dumps(
            {"installed": {"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET}}
        )
    )
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setenv("APPDATA", str(tmp_path / "config"))
    asked: list[dict[str, str]] = []

    def browser(url: str) -> None:
        params = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))
        asked.append(params)
        landing = (
            params["redirect_uri"]
            + "?"
            + urllib.parse.urlencode({"code": "code-1", "state": params["state"]})
        )
        threading.Thread(target=lambda: httpx.get(landing), daemon=True).start()

    interactive = _oauth.run_sign_in

    def scripted(
        client_path: pathlib.Path | None, token: pathlib.Path, **kw: Any
    ) -> str:
        return interactive(
            client_path,
            token,
            grant=kw["grant"],
            http_client=httpx.Client(transport=httpx.MockTransport(google.oauth)),
            open_browser=browser,
            read_line=lambda: None,
            browser_available=True,
            timeout=10,
        )

    monkeypatch.setattr(_oauth, "run_sign_in", scripted)
    result = CliRunner().invoke(cli, ["auth", "sign-in", "google-docs", str(client)])
    assert result.exit_code == 0, result.output
    assert f"Signed in as {EMAIL}." in result.output
    # The notice says what drive.file lets CocoIndex do, before the consent.
    assert "only the files it creates in your Google Drive" in result.output
    assert "edits and trashes only the docs it created" in result.output
    assert "It cannot see or change any other file." in result.output
    (params,) = asked
    assert params["scope"] == "https://www.googleapis.com/auth/drive.file"
    # Its own token file; the Google Drive source's sign-in is untouched.
    docs_token = tmp_path / "config" / "cocoindex" / "google_docs_token.json"
    assert google_docs.UserAccount().token_path == docs_token
    assert json.loads(docs_token.read_text())["email"] == EMAIL
    assert not google_drive.UserAccount().is_signed_in()
    assert google_docs.UserAccount().__coco_memo_key__() == (
        "cocoindex.google_docs.user_account",
        EMAIL,
    )

    result = CliRunner().invoke(cli, ["auth", "sign-out", "google-docs"])
    assert result.exit_code == 0 and "Signed out." in result.output
    assert not docs_token.exists()


def test_a_consent_without_drive_file_is_refused(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    google = FakeGoogle(grant_scope="openid")
    client = tmp_path / "client_secret.json"
    client.write_text(
        json.dumps(
            {"installed": {"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET}}
        )
    )

    def browser(url: str) -> None:
        params = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))
        landing = (
            params["redirect_uri"]
            + "?"
            + urllib.parse.urlencode({"code": "code-1", "state": params["state"]})
        )
        threading.Thread(target=lambda: httpx.get(landing), daemon=True).start()

    with pytest.raises(google_docs.SignInError, match="files CocoIndex creates"):
        _oauth.run_sign_in(
            client,
            tmp_path / "t.json",
            grant=_auth.DOCS_GRANT,
            http_client=httpx.Client(transport=httpx.MockTransport(google.oauth)),
            open_browser=browser,
            read_line=lambda: None,
            say=lambda _: None,
            browser_available=True,
            timeout=10,
        )


def test_a_run_without_a_sign_in_names_the_docs_sign_in(
    setup: Setup, tmp_path: pathlib.Path
) -> None:
    coco_env.context_provider.provide(
        GDOCS, google_docs.UserAccount(tmp_path / "none.json")
    )
    with pytest.raises(Exception) as excinfo:
        setup.run(setup.app("no_sign_in"), BRIEF)
    assert "cocoindex auth sign-in google-docs" in "".join(
        str(e) for e in _chain(excinfo.value)
    )


# --- the Markdown subset -----------------------------------------------------------------


def test_the_markdown_subset() -> None:
    blocks = _markdown.parse_blocks(
        "Intro *it* and __bold__, snake_case, `a*b`, \\*literal\\*.\n"
        "# One\n"
        "## Two ##\n"
        "- a\n"
        "    - b, [x](https://w.org/A_(b))\n"
        "      continued\n"
        "- c <https://q.org>\n"
        "\n"
        "```\n"
        "# not a heading\n"
        "```\n"
        "### Two\n"
        "## Two\n"
    )
    assert [b.path for b in blocks] == [
        (),
        ("One",),
        ("One", "Two"),
        ("One", "Two", "Two"),
        ("One", "Two"),
    ]
    # A repeated heading path gets a block id of its own.
    assert len({b.block_id for b in blocks}) == 5
    intro = blocks[0].paragraphs[0]
    assert intro.text == "Intro it and bold, snake_case, a*b, *literal*."
    assert [r.text for r in intro.runs if r.italic] == ["it"]
    assert [r.text for r in intro.runs if r.bold] == ["bold"]
    assert [r.text for r in intro.runs if r.code] == ["a*b"]
    two = blocks[2].paragraphs
    assert [(p.bullet, p.text) for p in two[1:4]] == [
        (0, "a"),
        (1, "b, x continued"),
        (0, "c https://q.org"),
    ]
    assert [r.link for r in two[2].runs if r.link] == ["https://w.org/A_(b)"]
    assert two[4] == _markdown.Paragraph(
        "NORMAL_TEXT", None, (_markdown.Run("# not a heading", code=True),)
    )


def test_paragraphs_added_in_and_after_a_section_touch_no_other_range(
    setup: Setup,
) -> None:
    app = setup.app("insert_inside")
    original = "## A\n\na1\n\na2\n\n## B\n\nb1\n\nb2\n\n## C\n\nc1\n"
    setup.run(app, original)
    doc = setup.google.only_doc()
    before = _ranges_by_block(doc)
    on_b1, on_b2 = doc.comment("b1"), doc.comment("b2")
    changed = original.replace("b1\n\nb2\n", "b1\n\n- middle\n\nb2\n\nb3 *tail*\n")
    setup.run(app, changed)
    assert [text for _, _, text in doc.lines()] == [
        "A",
        "a1",
        "a2",
        "B",
        "b1",
        "middle",
        "b2",
        "b3 tail",
        "C",
        "c1",
    ]
    after = _ranges_by_block(doc)
    blocks = _blocks(changed)
    for block in (blocks[0], blocks[2]):
        assert after[block.block_id] == before[block.block_id]
    assert after[blocks[1].block_id][1] == "B\nb1\nmiddle\nb2\nb3 tail\n"
    assert doc.comment_survives(on_b1) and doc.comment_survives(on_b2)
    assert (
        _docs.plan_write(_docs.read_document(doc.to_json()), blocks, None).requests
        == []
    )


def test_an_undeclared_block_a_person_edited_is_left_with_the_persons_text(
    setup: Setup, caplog: pytest.LogCaptureFixture
) -> None:
    app = setup.app("release")
    setup.run(app, "## A\n\na1\n\n## B\n\nb1\n")
    doc = setup.google.only_doc()
    doc.person_replace("b1", "b1 with a note")
    with caplog.at_level(logging.WARNING, logger=_target.__name__):
        setup.run(app, "## A\n\na1\n")
    assert [text for _, _, text in doc.lines()] == ["A", "a1", "B", "b1 with a note"]
    assert len(doc.ranges) == 1  # the target let go of B
    assert "no longer declared" in caplog.text


def _random_markdown(rng: Any, sections: list[str]) -> str:
    variants = [
        "plain {}",
        "- item {}",
        "  - nested {}",
        "- **bold** {} with [link](https://e.com/{})",
        "`code` {} 🎉",
        "*italic* {}",
    ]
    parts = []
    if rng.random() < 0.3:
        parts.append(f"preamble {rng.randrange(3)}")
    for name in sections:
        heading = f"## {name}" if rng.random() < 0.8 else f"## **{name}**"
        parts.append(heading)
        for _ in range(rng.randrange(5)):
            parts.append(
                rng.choice(variants).format(rng.randrange(4), rng.randrange(4))
            )
    return "\n\n".join(parts) + "\n"


def test_random_successive_versions_leave_exactly_the_declared_doc(
    setup: Setup,
) -> None:
    import random

    rng = random.Random(20261126)
    app = setup.app("fuzz")
    pool = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta"]
    sections = rng.sample(pool, 3)
    for step in range(40):
        roll = rng.random()
        if roll < 0.15 and len(sections) < len(pool):
            sections.insert(
                rng.randrange(len(sections) + 1),
                rng.choice([s for s in pool if s not in sections]),
            )
        elif roll < 0.25 and len(sections) > 1:
            sections.remove(rng.choice(sections))
        elif roll < 0.35 and len(sections) > 1:
            i, j = rng.sample(range(len(sections)), 2)
            sections[i], sections[j] = sections[j], sections[i]
        markdown = _random_markdown(rng, sections)
        setup.run(app, markdown)
        doc = setup.google.only_doc()
        expected = [(p.style, p.bullet, p.text) for p in _markdown.parse(markdown)]
        assert doc.lines() == expected, f"step {step}:\n{markdown}"
        blocks = _blocks(markdown)
        state = _docs.read_document(doc.to_json())
        assert _docs.plan_write(state, blocks, None).requests == [], f"step {step}"
        assert set(state.ranges) == {b.block_id for b in blocks}


# --- reading back a person's edits --------------------------------------------------


def _read(setup: Setup, declared: str) -> google_docs.DocRead | None:
    """``read_doc`` run in a component of its own app, as a flow would."""
    out: dict[str, google_docs.DocRead | None] = {}

    @coco.fn
    async def main() -> None:
        out["read"] = await google_docs.read_doc(GDOCS, "Weekly brief", declared)

    setup.google.calls.clear()
    coco.App(
        coco.AppConfig(name="read_back", environment=coco_env), main
    ).update_blocking()
    return out["read"]


def _person_deletes_block(doc: FakeDoc, block: _markdown.Block) -> None:
    (rid,) = [
        r for r, n in doc.range_names().items() if n.split(":")[1] == block.block_id
    ]
    _, start, end = doc.ranges[rid]
    doc.delete(start, end)
    doc.revision += 1


def test_read_doc_hands_back_a_persons_edits_in_the_declared_markdown(
    setup: Setup,
) -> None:
    setup.run(setup.app("read_back_doc"), BRIEF)
    doc = setup.google.only_doc()
    (doc_id,) = setup.google.files

    # Nothing changed by a person: the declared text exactly, and two reads.
    read = _read(setup, BRIEF)
    assert read == google_docs.DocRead(
        url=f"https://docs.google.com/document/d/{doc_id}/edit",
        content=BRIEF,
        edited=(),
        deleted=(),
    )
    assert [m for m, _ in setup.google.calls] == ["GET", "GET"]  # find, read
    assert setup.google.writes() == []

    # A person rewords a sentence in Risks (bold included), deletes Sources,
    # comments on Decisions, and adds an empty paragraph to the preamble.
    doc.person_replace("Google may change", "Google could change")
    doc.comment("Ship the")
    doc.insert(doc.find("Kept current") + len("Kept current from the sources."), "\n")
    doc.revision += 1
    _person_deletes_block(doc, _blocks(BRIEF)[4])

    read = _read(setup, BRIEF)
    assert read is not None
    assert read.edited == (("Brief", "Risks"),)
    assert read.deleted == (("Brief", "Sources"),)
    assert read.content == BRIEF.replace(
        "## Risks\n\nGoogle may change *named range* behavior.\n\n",
        "## Risks\n\nGoogle could change _named range_ behavior.\n\n",
    ).replace("## Sources\n\nhttps://example.com/a and https://example.com/b.\n", "")
    # The person's text parses back to what the doc holds.
    risks = _markdown.parse_blocks(read.content)[3]
    state = _docs.read_document(doc.to_json())
    assert _markdown.fingerprint(risks.paragraphs) == _markdown.fingerprint(
        state.content(state.ranges[risks.block_id])
    )
    assert setup.google.writes() == []  # reading writes nothing


def test_declaring_the_content_read_back_makes_the_blocks_the_targets_again(
    setup: Setup, caplog: pytest.LogCaptureFixture
) -> None:
    app = setup.app("adopt")
    setup.run(app, BRIEF)
    doc = setup.google.only_doc()
    doc.person_replace("Google may change", "Google could change")
    on_edit = doc.comment("could change")
    read = _read(setup, BRIEF)
    assert read is not None and read.edited == (("Brief", "Risks"),)

    with caplog.at_level(logging.WARNING, logger=_target.__name__):
        setup.run(app, read.content)
    assert not _warnings(caplog)  # nothing kept from the flow any more
    assert doc.comment_survives(on_edit)  # the person's text is not rewritten
    assert _read(setup, read.content) == google_docs.DocRead(
        read.url, read.content, (), ()
    )
    # The block is the flow's again: its next change is written.
    setup.run(app, read.content.replace("could change", "will change"))
    assert (
        "NORMAL_TEXT",
        None,
        "Google will change named range behavior.",
    ) in doc.lines()


def test_a_heading_a_person_renamed_is_taken_in_without_a_second_copy(
    setup: Setup,
) -> None:
    app = setup.app("rename")
    markdown = "## Plan\n\n### Risks\n\nr1\n\n### Costs\n\nc1\n\n## Next\n\nn1\n"
    setup.run(app, markdown)
    doc = setup.google.only_doc()
    on_c1 = doc.comment("c1")
    doc.person_replace("Plan", "The plan")  # renames the blocks under it too
    read = _read(setup, markdown)
    assert read is not None and read.edited == (("Plan",),)
    assert read.content == markdown.replace("## Plan", "## The plan")

    setup.run(app, read.content)
    assert [text for _, _, text in doc.lines()] == [
        "The plan",
        "Risks",
        "r1",
        "Costs",
        "c1",
        "Next",
        "n1",
    ]
    assert doc.comment_survives(on_c1)  # the blocks under it were not rewritten
    state = _docs.read_document(doc.to_json())
    assert set(state.ranges) == {b.block_id for b in _blocks(read.content)}
    assert _docs.plan_write(state, _blocks(read.content), None).requests == []


_watch_runs: list[str | None] = []


@coco.fn(memo=True)
async def _watch(declared: str) -> None:
    read = await google_docs.read_doc(GDOCS, "Weekly brief", declared)
    _watch_runs.append(None if read is None else read.content)


def test_a_read_reruns_its_reader_on_a_persons_edit_only(setup: Setup) -> None:
    """The read's record survives the target's own writes (a create, an update,
    a person's edit taken in) and a comment, and not a person's edit."""
    content: dict[str, str] = {"content": BRIEF}

    @coco.fn
    async def main() -> None:
        google_docs.declare_doc(GDOCS, "Weekly brief", content["content"])
        await coco.use_mount(coco.component_subpath("watch"), _watch, BRIEF)

    app = coco.App(coco.AppConfig(name="watch", environment=coco_env), main)
    _watch_runs.clear()

    def run(new: str | None = None) -> int:
        if new is not None:
            content["content"] = new
        before = len(_watch_runs)
        app.update_blocking()
        return len(_watch_runs) - before

    assert run() == 1 and _watch_runs[-1] is None  # read before the doc existed
    assert run() == 0  # the target created it: not a change by a person
    assert run(BRIEF.replace("Ship the", "Ship the new")) == 0
    assert run() == 0  # after the target's own update
    doc = setup.google.only_doc()
    doc.comment("Google may")
    doc.revision += 1  # a comment may move the revision
    assert run() == 0
    doc.person_replace("Google may change", "Google could change")
    assert run() == 1 and "Google could change" in str(_watch_runs[-1])
    assert run() == 0
    # The flow takes the person's text in: the target's write, planned on the
    # revision the reader read, makes the blocks the target's again.
    edited = BRIEF.replace("Ship the", "Ship the new").replace(
        "Google may change *named range*", "Google could change _named range_"
    )
    assert run(edited) == 0
    assert run() == 0
    _person_deletes_block(doc, _blocks(BRIEF)[1])  # the preamble
    assert run() == 1
    assert run() == 0


def test_the_doc_is_the_same_for_a_run_with_a_new_state(setup: Setup) -> None:
    """The doc's tag comes from the declaration, so a fresh state store (another
    machine, a deleted database) finds the doc rather than creating a copy."""
    setup.run(setup.app("first_state"), BRIEF)
    (doc_id,) = setup.google.files
    setup.run(setup.app("second_state"), BRIEF.replace("Ship the", "Ship the new"))
    assert list(setup.google.files) == [doc_id]
    assert "Ship the new connector in 1.3." in [
        t for _, _, t in setup.google.only_doc().lines()
    ]


_ROUND_TRIP = [
    "plain words, with punctuation: a; b! (c) [d] e_f *g* `h` <i> 1. 2) #3 \\\\ ~",
    "**bold**, *italic*, ***both***, `code`, `` a ` b ``, and **bold `code`**",
    "a [link](https://e.com/a_(b)), <https://e.com/c>, https://e.com/d. done",
    "snake_case and _italic_ and __bold__ and a*b*c",
    "- item with **bold**\n  - nested *italic*\n    - deeper `code`\n- back",
    "## Heading with *style* and a #tag #",
    "# Top\n\n### Skips a level",
    "\\# not a heading, \\- not an item, \\+ not either, \\~~~ no fence",
    "text ending in a backslash \\\\",
    "Café 🎉 — “quotes” and an em dash",
]


@pytest.mark.parametrize("markdown", _ROUND_TRIP)
def test_rendered_markdown_parses_back_to_the_same_paragraphs(markdown: str) -> None:
    paragraphs = _markdown.parse(markdown)
    rendered = _markdown.render(paragraphs)
    assert _markdown.parse(rendered) == [_markdown.normalized(p) for p in paragraphs]
    assert _markdown.render(_markdown.parse(rendered)) == rendered


def test_whitespace_a_person_adds_is_not_an_edit() -> None:
    run = _markdown.Run
    written = _markdown.parse("a **bold** word")
    typed = [
        _markdown.Paragraph(
            "NORMAL_TEXT",
            None,
            (run(" a "), run("bold ", bold=True), run("word  ")),
        ),
        _markdown.Paragraph("NORMAL_TEXT", None, ()),
    ]
    assert _markdown.fingerprint(typed) == _markdown.fingerprint(written)
    assert _markdown.render(typed) == "a **bold** word"

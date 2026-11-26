"""The web as a source: a search or a set of pages lists pages (DESIGN.md §0, §3).

Two listing kinds, one resource kind:

- ``web_search`` lists the pages a set of queries finds, through a
  ``SearchEngine`` chosen by the environment (never by the model), with a
  **frontier** rule that keeps the set stable under rank jitter: a page
  chosen last run stays while any query still returns it within its top
  ``2k``; new pages enter from a query's top ``k`` while the budget has room.
- ``web_pages`` lists the pages a result (``res:`` handle) or a literal list
  names, minus the pages another web source of the session already listed,
  so a lead that is an alias of a page already read is one resource, not two.

A page is a resource (``WebPage``, a ``FileLike``): its key is the canonical
final URL, its content is the page normalized (title and main content as
Markdown by trafilatura; a PDF as a document), and its content state is the
digest of that normalized content, so a worker over an unchanged digest is
skipped and a timestamp, a nonce or a comment count changes nothing.

Every fetch happens in the listing, in the body of the session that opened
the source, never in a worker: conditional requests from a **response cache**
(per requested URL: final URL, validators, and the digest of the payload the
validators were obtained with), payloads kept by version in a **payload
store** (per final URL and digest), both in the session's ``use_state`` and
pruned to the current listings when the body finishes. A 304 reuses exactly
the version its validators name, or the URL is fetched again. Redirects are
followed hop by hop, and a chain that lands on a final URL already fetched
this run takes that fetch's response snapshot, so two aliases cannot observe
two versions in one run.

What a listing records (``SEARCH_OBSERVATION``, ``PAGES_OBSERVATION``) is
what its validator needs to reproduce it from the record alone: the frontier,
the requested-to-final mapping, each page's validators and digest, and the
unavailable pages with their reasons. Validators never raise for the
environment: an engine outage or an exhausted transient fetch makes the record
invalid, the body re-runs and contains the trouble where it meets it.

Unavailable pages: a permanent outcome (404, 403, robots, a sign-in page, an
unsupported media type, a PDF over the cap) is data — the page is left out and
recorded with its reason; an exhausted transient outcome (429, 503, a timeout,
after retries) is an environment failure contained per page — left out,
marked ``transient``, and recorded as a contained failure of the session so
the fetch is tried again next run. Fetch trouble never reaches a fan-out's
``failed`` count, because workers do not fetch.

Politeness: one ``User-Agent`` naming the project, ``robots.txt`` per host,
``Crawl-delay`` honoured up to ten seconds, one request at a time per host and
at most four in flight, a 20-second timeout, 2 MiB for HTML and 8 MiB for a
PDF, five redirect hops, no cookies, ``GET`` only, and no sign-in: a 401 or
403, a login path, or a page that only asks to sign in is unavailable.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import logging
import re
import time
import urllib.parse
import urllib.robotparser
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import PurePath, PurePosixPath
from typing import TYPE_CHECKING, ClassVar, Protocol, Self

import cocoindex as coco
import httpx
import lxml.etree
import lxml.html
import trafilatura
from cocoindex._internal.api import StateHandle
from cocoindex.connectorkits.fingerprint import fingerprint_bytes
from cocoindex.resources import file as _file

from ..documents import Document
from ..errors import ToolError, TransientError, record_contained_failure, retrying
from ..spec import Json
from ..trace import TraceSink
from ..transcript import canonical_json
from .source import Source, content_of, item_key_arg

if TYPE_CHECKING:
    from . import SessionContext

logger = logging.getLogger(__name__)

USER_AGENT = "cocoindex-agents/0.1 (+https://github.com/cocoindex-io/cocoindex)"

# The frontier: ``K`` results per query are candidates, ``2 * K`` keep an
# incumbent; a search source reads at most SEARCH_BUDGET pages and a page set
# at most PAGES_BUDGET. Both budgets sit inside one widened-count bucket
# (9–15 render ``8–16``, 4–7 render ``4–8``), so a page or two unavailable does
# not move the card (DESIGN.md §5).
K = 4
SEARCH_BUDGET = 12
PAGES_BUDGET = 6

HTML_CAP = 2 * 1024 * 1024
PDF_CAP = 8 * 1024 * 1024
TEXT_CAP = 40_000  # characters of normalized text a worker reads
TIMEOUT_SECONDS = 20.0
MAX_HOPS = 5
MAX_IN_FLIGHT = 4
MAX_CRAWL_DELAY = 10.0
# trafilatura's precision mode drops link-heavy sections; under this many
# characters its default extractor is tried instead.
THIN_PRECISION = 500
# A page whose normalized text is this short and asks to sign in is a sign-in page.
SIGN_IN_TEXT = 200

_SIGN_IN_PATH = re.compile(r"/(login|log-in|signin|sign-in|auth)(/|$)", re.I)
_SIGN_IN_TEXT = re.compile(r"\b(sign in|log in|login|signin)\b", re.I)
_URL = re.compile(r"https?://[^\s<>()\"'\]\[]+")
_TRACKING = {"fbclid", "gclid", "mc_cid", "mc_eid", "igshid", "ref_src"}

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


# --- search engines -----------------------------------------------------------


@dataclass(frozen=True)
class SearchHit:
    url: str
    title: str


class SearchEngine(Protocol):
    """A ranked web search. ``identity`` names the engine and the configuration
    that affects its results (``wikipedia:en``, ``tavily:basic``), never a
    credential; it is tracked through ``AgentsConfig.search_engine`` and named
    in every record."""

    @property
    def identity(self) -> str: ...

    async def search(self, query: str, k: int) -> list[SearchHit]:
        """The top ``k`` hits. Raises ``TransientError`` for trouble worth a
        retry (a 429, a 5xx, a timeout); anything else is an environment
        failure."""
        ...


# The engines a process knows, by identity. Validators run without a component
# context, so they look the engine up here from the identity the record names;
# an identity nobody registered makes the record invalid, not the run fail.
_ENGINES: dict[str, SearchEngine] = {}
# The HTTP transport every client of this module uses: ``None`` is the network;
# a test sets ``httpx.MockTransport``.
_TRANSPORT: httpx.AsyncBaseTransport | None = None
# The backoff ``errors.retrying`` uses here; ``None`` is its default (one second
# to thirty). A test sets a zero backoff so an exhausted retry takes no time.
_backoff: Callable[[int], float] | None = None
# The key under which a session's context remembers that this body run already
# bumped the stores' run index and registered the pruning hook.
_RUN_STARTED = "web:run-started"


def register_engine(engine: SearchEngine) -> SearchEngine:
    _ENGINES[engine.identity] = engine
    return engine


def set_transport(transport: httpx.AsyncBaseTransport | None) -> None:
    global _TRANSPORT
    _TRANSPORT = transport


def http_client() -> httpx.AsyncClient:
    """One client per listing or validation: no cookies, no automatic redirects
    (they are followed hop by hop, below), the project's ``User-Agent``."""
    return httpx.AsyncClient(
        transport=_TRANSPORT,
        follow_redirects=False,
        timeout=TIMEOUT_SECONDS,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/pdf,text/plain,text/markdown;q=0.9,*/*;q=0.5",
        },
    )


def _transient_status(status: int) -> bool:
    return status == 429 or status >= 500


class WikipediaSearch:
    """The MediaWiki search API: no key, free, a descriptive ``User-Agent``
    (Wikimedia's policy) and well under its 200 requests a minute."""

    def __init__(self, language: str = "en") -> None:
        self.language = language

    @property
    def identity(self) -> str:
        return f"wikipedia:{self.language}"

    async def search(self, query: str, k: int) -> list[SearchHit]:
        host = f"https://{self.language}.wikipedia.org"
        params = {
            "action": "query",
            "list": "search",
            "srsearch": query,
            "srlimit": str(k),
            "format": "json",
        }
        async with http_client() as client:
            try:
                response = await client.get(f"{host}/w/api.php", params=params)
            except httpx.TransportError as exc:
                raise TransientError(f"wikipedia search: {exc}") from exc
        if _transient_status(response.status_code):
            raise TransientError(f"wikipedia search: HTTP {response.status_code}")
        response.raise_for_status()
        hits = response.json().get("query", {}).get("search", [])
        return [
            SearchHit(
                f"{host}/wiki/" + urllib.parse.quote(h["title"].replace(" ", "_")),
                h["title"],
            )
            for h in hits
        ]


class TavilySearch:
    """Tavily Search, basic depth: one credit per query on its free tier."""

    def __init__(self, api_key: str) -> None:
        self._api_key = api_key

    @property
    def identity(self) -> str:
        return "tavily:basic"

    async def search(self, query: str, k: int) -> list[SearchHit]:
        async with http_client() as client:
            try:
                response = await client.post(
                    "https://api.tavily.com/search",
                    json={"query": query, "max_results": k, "search_depth": "basic"},
                    headers={"Authorization": f"Bearer {self._api_key}"},
                )
            except httpx.TransportError as exc:
                raise TransientError(f"tavily search: {exc}") from exc
        if _transient_status(response.status_code):
            raise TransientError(f"tavily search: HTTP {response.status_code}")
        response.raise_for_status()
        return [
            SearchHit(r["url"], r.get("title") or r["url"])
            for r in response.json().get("results", [])
        ]


# --- URLs ---------------------------------------------------------------------


def canonical_url(url: str) -> str | None:
    """The canonical form of an ``http(s)`` URL, or ``None`` for anything else.

    Scheme and host lower-cased, the default port and the fragment dropped,
    tracking parameters dropped, query parameters sorted, an empty path made
    ``/``. Two spellings of one address are one key.
    """
    try:
        parts = urllib.parse.urlsplit(url.strip())
    except ValueError:
        return None
    scheme = parts.scheme.lower()
    if scheme not in ("http", "https") or not parts.hostname:
        return None
    host = parts.hostname.lower()
    port = parts.port
    if port is not None and port != (443 if scheme == "https" else 80):
        host = f"{host}:{port}"
    query = [
        (k, v)
        for k, v in urllib.parse.parse_qsl(parts.query, keep_blank_values=True)
        if not (k.startswith("utm_") or k in _TRACKING)
    ]
    return urllib.parse.urlunsplit(
        (scheme, host, parts.path or "/", urllib.parse.urlencode(sorted(query)), "")
    )


def urls_in(text: str) -> list[str]:
    """The canonical ``http(s)`` URLs a text names, deduplicated, in first-seen order."""
    out: list[str] = []
    for raw in _URL.findall(text):
        url = canonical_url(raw.rstrip(".,;:!?"))
        if url is not None and url not in out:
            out.append(url)
    return out


# --- fetching -----------------------------------------------------------------


@dataclass(frozen=True)
class Validators:
    etag: str | None = None
    last_modified: str | None = None

    def headers(self) -> dict[str, str]:
        out = {}
        if self.etag:
            out["If-None-Match"] = self.etag
        if self.last_modified:
            out["If-Modified-Since"] = self.last_modified
        return out

    @classmethod
    def of(cls, response: httpx.Response) -> Validators:
        return cls(response.headers.get("etag"), response.headers.get("last-modified"))


@dataclass(frozen=True)
class Fetched:
    """A final response: a 200 with its body, or a 304 (``body`` is ``None``)."""

    final_url: str
    status: int
    validators: Validators
    content_type: str
    body: bytes | None
    truncated: bool = False  # an HTML body cut at the cap


@dataclass(frozen=True)
class Unavailable:
    """A page that could not be read. ``transient`` marks an exhausted retry
    (an environment failure contained per page); the rest is data."""

    reason: str
    transient: bool = False
    detail: str = ""


class Fetcher:
    """Polite fetching for one run: robots per host, one request at a time per
    host, a global ceiling, hop-by-hop redirects, and one response snapshot per
    final URL so aliases share a fetch."""

    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client
        self._robots: dict[str, urllib.robotparser.RobotFileParser | None] = {}
        self._host_locks: dict[str, asyncio.Lock] = {}
        self._last_request: dict[str, float] = {}
        self._in_flight = asyncio.Semaphore(MAX_IN_FLIGHT)
        self.snapshots: dict[str, Fetched] = {}  # by final URL, this run

    async def fetch(
        self, requested: str, previous: tuple[str, Validators] | None
    ) -> Fetched | Unavailable:
        """Fetch a requested URL; ``previous`` is the final URL and validators of
        the response the caller holds, sent as a conditional request when the
        chain reaches that final URL again. Transient trouble is retried
        (``errors.retrying``); when exhausted it is an ``Unavailable`` marked
        transient, never an exception."""
        try:
            return await retrying(
                lambda: self._fetch_once(requested, previous), backoff=_backoff
            )
        except TransientError as exc:
            return Unavailable("transient", transient=True, detail=str(exc))

    async def _fetch_once(
        self, requested: str, previous: tuple[str, Validators] | None
    ) -> Fetched | Unavailable:
        url = requested
        for _hop in range(MAX_HOPS + 1):
            if url in self.snapshots:
                return self.snapshots[url]
            if _SIGN_IN_PATH.search(urllib.parse.urlsplit(url).path):
                return Unavailable("sign-in", detail=url)
            if not await self._allowed(url):
                return Unavailable("robots", detail=url)
            headers = {}
            if previous is not None and previous[0] == url:
                headers = previous[1].headers()
            response = await self._request(url, headers)
            status = response.status_code
            if 300 <= status < 400 and response.headers.get("location"):
                nxt = canonical_url(
                    urllib.parse.urljoin(url, response.headers["location"])
                )
                if nxt is None:
                    return Unavailable("redirect", detail=response.headers["location"])
                url = nxt
                continue
            if status == 304:
                assert previous is not None
                return Fetched(url, 304, previous[1], "", None)
            if status in (401, 403):
                return Unavailable("sign-in", detail=f"HTTP {status}")
            if status in (404, 410):
                return Unavailable("not-found", detail=f"HTTP {status}")
            if _transient_status(status):
                raise TransientError(f"HTTP {status} for {url}")
            if status != 200:
                return Unavailable(f"http-{status}")
            content_type = (
                response.headers.get("content-type", "").split(";")[0].strip().lower()
            )
            kind = _content_kind(content_type)
            if kind is None:
                return Unavailable("unsupported-media-type", detail=content_type)
            cap = PDF_CAP if kind == "pdf" else HTML_CAP
            body, over = await _read_capped(response, cap)
            if over and kind == "pdf":
                return Unavailable("too-large", detail=f"over {PDF_CAP} bytes")
            fetched = Fetched(
                url, 200, Validators.of(response), content_type, body, over
            )
            self.snapshots[url] = fetched
            return fetched
        return Unavailable("redirect", detail=f"more than {MAX_HOPS} hops")

    async def _request(self, url: str, headers: dict[str, str]) -> httpx.Response:
        host = _host_of(url)
        lock = self._host_locks.setdefault(host, asyncio.Lock())
        async with lock:
            # The crawl delay is waited out under the host's lock alone, so a
            # slow host does not hold one of the four in-flight slots idle.
            wait = self._last_request.get(host, 0.0) + self._crawl_delay(host)
            if wait > time.monotonic():
                await asyncio.sleep(wait - time.monotonic())
            async with self._in_flight:
                try:
                    return await self._client.send(
                        self._client.build_request("GET", url, headers=headers),
                        stream=True,
                    )
                except httpx.TransportError as exc:
                    raise TransientError(f"{type(exc).__name__} for {url}") from exc
                finally:
                    self._last_request[host] = time.monotonic()

    async def _allowed(self, url: str) -> bool:
        host = _host_of(url)
        if host not in self._robots:
            self._robots[host] = await self._load_robots(host)
        rules = self._robots[host]
        return rules is None or rules.can_fetch(USER_AGENT, url)

    async def _load_robots(
        self, host: str
    ) -> urllib.robotparser.RobotFileParser | None:
        """The host's robots.txt, or ``None`` when it has none we can read (then
        everything is allowed, as the protocol says for a missing file)."""
        try:
            response = await self._client.get(f"{host}/robots.txt")
        except httpx.TransportError:
            return None
        if response.status_code != 200:
            return None
        rules = urllib.robotparser.RobotFileParser()
        rules.parse(response.text.splitlines())
        return rules

    def _crawl_delay(self, host: str) -> float:
        rules = self._robots.get(host)
        delay = rules.crawl_delay(USER_AGENT) if rules is not None else None
        return min(float(delay), MAX_CRAWL_DELAY) if delay else 0.0


def _host_of(url: str) -> str:
    """The robots and politeness key: scheme and authority."""
    parts = urllib.parse.urlsplit(url)
    return f"{parts.scheme}://{parts.netloc}"


def _content_kind(content_type: str) -> str | None:
    if content_type in ("text/html", "application/xhtml+xml"):
        return "html"
    if content_type in ("text/plain", "text/markdown", "text/x-markdown"):
        return "text"
    if content_type == "application/pdf":
        return "pdf"
    return None


async def _read_capped(response: httpx.Response, cap: int) -> tuple[bytes, bool]:
    chunks: list[bytes] = []
    size = 0
    over = False
    try:
        async for chunk in response.aiter_bytes():
            chunks.append(chunk)
            size += len(chunk)
            if size > cap:
                over = True
                break
    except httpx.TransportError as exc:
        raise TransientError(
            f"{type(exc).__name__} while reading {response.url}"
        ) from exc
    finally:
        await response.aclose()
    return b"".join(chunks)[:cap], over


# --- normalization ------------------------------------------------------------


@dataclass(frozen=True)
class Normalized:
    """A page's content as the model reads it: a title and Markdown, or a PDF."""

    title: str
    text: str | None  # Markdown; ``None`` for a document
    pdf: bytes | None = None
    truncated: bool = False

    @property
    def media_type(self) -> str | None:
        return "application/pdf" if self.pdf is not None else None

    def digest(self) -> str:
        """The content state: title and text, or the engine's fingerprint of the PDF."""
        if self.pdf is not None:
            return fingerprint_bytes(self.pdf).hex()
        return hashlib.sha256(
            (self.title + "\n\x00\n" + (self.text or "")).encode("utf-8")
        ).hexdigest()


def normalize(fetched: Fetched) -> Normalized:
    """A pure function of the response: trafilatura for HTML (precision first, its
    default extractor when precision is thin), the text itself for text and
    Markdown, the bytes for a PDF. No clock, no metadata extraction."""
    assert fetched.body is not None
    kind = _content_kind(fetched.content_type)
    if kind == "pdf":
        return Normalized(
            title=PurePosixPath(fetched.final_url).name, text=None, pdf=fetched.body
        )
    text = fetched.body.decode("utf-8", errors="replace")
    if kind == "text":
        return Normalized(_first_line(text), _cap_text(text, fetched.truncated))
    title = _html_title(text) or PurePosixPath(fetched.final_url).name

    def extract(favor_precision: bool) -> str | None:
        return trafilatura.extract(
            text,
            url=fetched.final_url,
            output_format="markdown",
            include_links=True,
            include_tables=True,
            include_comments=False,
            deduplicate=False,
            with_metadata=False,
            favor_precision=favor_precision,
        )

    body = extract(favor_precision=True)
    if body is None or len(body) < THIN_PRECISION:
        body = extract(favor_precision=False) or body or ""
    return Normalized(
        title, _cap_text(body, fetched.truncated), truncated=fetched.truncated
    )


def _cap_text(text: str, truncated_body: bool) -> str:
    text = text.strip()
    if len(text) > TEXT_CAP or truncated_body:
        return text[:TEXT_CAP].rstrip() + "\n\n[truncated]"
    return text


def _first_line(text: str) -> str:
    for line in text.splitlines():
        if line.strip():
            return line.strip().lstrip("# ")[:200]
    return ""


def _html_title(html: str) -> str:
    try:
        tree = lxml.html.fromstring(html)
    except (ValueError, lxml.etree.ParserError, lxml.etree.XMLSyntaxError):
        return ""
    for xpath in ('//meta[@property="og:title"]/@content', "//title/text()"):
        found = tree.xpath(xpath)
        if found and str(found[0]).strip():
            return " ".join(str(found[0]).split())[:200]
    return ""


def looks_like_sign_in(normalized: Normalized) -> bool:
    text = normalized.text or ""
    return len(text) < SIGN_IN_TEXT and bool(
        _SIGN_IN_TEXT.search(text + " " + normalized.title)
    )


# --- the resource -------------------------------------------------------------


class PageUrl(_file.FilePath[str]):
    """A page's address as its ``FilePath``: the resolved path is the URL itself."""

    __slots__ = ("_url",)

    def __init__(self, url: str) -> None:
        super().__init__(None, PurePosixPath(urllib.parse.urlsplit(url).path or "/"))
        self._url = url

    def resolve(self) -> str:
        return self._url

    def _with_path(self, path: PurePath) -> Self:
        return type(self)(self._url)  # type: ignore[return-value]

    def __coco_memo_key__(self) -> object:
        return ("web", self._url)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, PageUrl) and other._url == self._url

    def __hash__(self) -> int:
        return hash(self._url)


class WebPage(_file.FileLike[str]):
    """A page as a resource: keyed by its canonical final URL, memoized on the
    digest of its normalized content (``FileMetadata.content_fingerprint``;
    the modification time is fixed, so only the digest decides).

    What ``read()`` returns is what a worker reads: a header the system
    writes (``# <title>``, ``URL: <url>``) and the Markdown, or a PDF's bytes.
    No time is in it (DESIGN.md §3, no timestamps in tool results).
    """

    def __init__(self, url: str, normalized: Normalized, digest: str) -> None:
        self.title = normalized.title
        self.digest = digest
        self.media_type = normalized.media_type
        if normalized.pdf is not None:
            self._data = normalized.pdf
        else:
            header = f"# {normalized.title}\nURL: {url}\n\n"
            self._data = (header + (normalized.text or "")).encode("utf-8")
        super().__init__(
            PageUrl(url),
            _metadata=_file.FileMetadata(
                size=len(self._data),
                modified_time=EPOCH,
                content_fingerprint=bytes.fromhex(digest),
            ),
        )

    @property
    def url(self) -> str:
        return self._file_path.resolve()

    async def _fetch_metadata(self) -> _file.FileMetadata:
        assert self._metadata is not None
        return self._metadata

    async def _read_impl(self, size: int = -1) -> bytes:
        return self._data if size < 0 else self._data[:size]


# --- stores in use_state ------------------------------------------------------


@dataclass(frozen=True)
class ResponseEntry:
    """What a requested URL last answered: where it ended, the validators, and
    the digest of the payload those validators were obtained with."""

    final_url: str
    etag: str | None
    last_modified: str | None
    digest: str
    run: int

    def validators(self) -> Validators:
        return Validators(self.etag, self.last_modified)


@dataclass(frozen=True)
class Payload:
    """One version of a page's content, immutable once written."""

    title: str
    text: str | None
    pdf_b64: str | None
    truncated: bool
    first_fetched: str  # UTC, to the minute: for the trace, never for the model
    run: int

    def normalized(self) -> Normalized:
        pdf = base64.b64decode(self.pdf_b64) if self.pdf_b64 is not None else None
        return Normalized(self.title, self.text, pdf, self.truncated)


def payload_key(final_url: str, digest: str) -> str:
    return f"{final_url}\n{digest}"


@dataclass
class WebStores:
    """The session's response cache, payload store and frontier history."""

    run: int = 0
    responses: dict[str, ResponseEntry] = field(default_factory=dict)
    payloads: dict[str, Payload] = field(default_factory=dict)
    frontiers: dict[str, tuple[str, ...]] = field(default_factory=dict)

    def prune(self, run: int) -> None:
        """Drop response entries no listing of ``run`` named, then payload
        versions no response entry references."""
        self.responses = {u: e for u, e in self.responses.items() if e.run == run}
        live = {payload_key(e.final_url, e.digest) for e in self.responses.values()}
        self.payloads = {k: p for k, p in self.payloads.items() if k in live}


def _now_minute() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ")


# --- records ------------------------------------------------------------------


@dataclass(frozen=True)
class PageRecord:
    """One requested URL in a listing's record: where it ended and what it held,
    or why it was unavailable."""

    requested: str
    final_url: str | None = None
    etag: str | None = None
    last_modified: str | None = None
    digest: str | None = None
    reason: str | None = None
    transient: bool = False

    def validators(self) -> Validators:
        return Validators(self.etag, self.last_modified)

    def summary(self) -> str:
        return self.reason or (self.digest or "?")[:8]


@dataclass(frozen=True)
class ListingRecord:
    """What a web source records, once per run whatever was observed through it:
    enough for its validator to reproduce the listing and compare. Every
    observation of the source (``stats``, ``sample``, ``keys``, the fan-out's
    listing) is a pure function of the listing, so they share one record under
    one key, and an unchanged rerun validates the source once: one search per
    query and one conditional request per page. ``frontier`` is the sorted set
    of requested URLs chosen; ``pages`` is sorted by requested URL."""

    frontier: tuple[str, ...]
    pages: tuple[PageRecord, ...]

    def content(self) -> tuple[object, ...]:
        """What decides validity: the frontier and each page's final URL, digest
        and reason — not its validators, which an origin may rotate (a CDN's
        ETag per edge) while the content stands; a validation that saw new
        validators refreshes the record and stays valid."""
        return (
            self.frontier,
            tuple(
                (p.requested, p.final_url, p.digest, p.reason, p.transient)
                for p in self.pages
            ),
        )

    def differences(self, other: ListingRecord) -> list[str]:
        """What changed between two records, for the log."""
        out = []
        if self.frontier != other.frontier:
            out.append(f"frontier {sorted(set(self.frontier) ^ set(other.frontier))}")
        mine = {p.requested: p for p in self.pages}
        theirs = {p.requested: p for p in other.pages}
        for requested in sorted(set(mine) | set(theirs)):
            a, b = mine.get(requested), theirs.get(requested)
            if a is None or b is None:
                out.append(f"{requested}: {'gone' if b is None else 'new'}")
            elif (a.final_url, a.digest, a.reason) != (b.final_url, b.digest, b.reason):
                out.append(f"{requested}: {a.summary()} -> {b.summary()}")
        return out


@dataclass(frozen=True)
class SearchKey:
    engine: str
    k: int
    queries: tuple[str, ...]


@dataclass(frozen=True)
class PagesKey:
    urls: tuple[str, ...]  # sorted admitted requested URLs
    exclude_requested: tuple[str, ...]
    exclude_final: tuple[str, ...]


# A page read by an open session (a planner): the canonical URL, and what it held.
PageReadState = tuple[str, str | None, str | None, str] | None


# --- the listing computation --------------------------------------------------


def frontier(
    prev: Iterable[str], results: Mapping[str, list[str]], k: int, budget: int
) -> list[str]:
    """The pure frontier rule (DESIGN.md §3): incumbents — URLs of ``prev`` that
    some query still returns within its top ``2k`` — ordered by (best rank,
    URL), then newcomers from the queries' top ``k``, ordered the same, cut at
    ``budget``. No per-query quota."""
    rank: dict[str, int] = {}
    newcomer_ok: set[str] = set()
    for urls in results.values():
        seen: set[str] = set()
        for i, url in enumerate(urls[: 2 * k]):
            if url in seen:
                continue
            seen.add(url)
            rank[url] = min(rank.get(url, i + 1), i + 1)
            if i < k:
                newcomer_ok.add(url)
    prev_set = set(prev)
    incumbents = sorted((u for u in rank if u in prev_set), key=lambda u: (rank[u], u))
    newcomers = sorted(
        (u for u in newcomer_ok if u not in prev_set), key=lambda u: (rank[u], u)
    )
    return (incumbents + newcomers)[:budget]


@dataclass(frozen=True)
class Page:
    """A page a listing yielded this run."""

    final_url: str
    validators: Validators
    normalized: Normalized
    digest: str
    first_fetched: str


@dataclass
class ListingResult:
    frontier: list[str]
    pages: dict[str, Page]  # by final URL
    records: dict[str, PageRecord]  # by requested URL
    transient: dict[str, str]  # requested URL -> detail, for containment

    def record(self) -> ListingRecord:
        return ListingRecord(
            tuple(sorted(self.frontier)),
            tuple(self.records[u] for u in sorted(self.records)),
        )


class _History(Protocol):
    """Where a listing takes its previous validators and payloads from: the
    session's stores in the body, the record alone in a validator."""

    def previous(self, requested: str) -> tuple[str, Validators, str] | None:
        """(final URL, validators, digest) of the last response, if known."""
        ...

    def payload(self, final_url: str, digest: str) -> Normalized | None: ...

    def first_fetched(self, final_url: str, digest: str) -> str | None:
        """When this version was first fetched, if the history holds it."""
        ...

    def frontier(self, identity: str) -> tuple[str, ...]:
        """The requested URLs a search source chose last run."""
        ...

    def remember_frontier(self, identity: str, chosen: Iterable[str]) -> None: ...


class _StoreHistory:
    def __init__(self, stores: WebStores) -> None:
        self._stores = stores

    def frontier(self, identity: str) -> tuple[str, ...]:
        return self._stores.frontiers.get(identity, ())

    def remember_frontier(self, identity: str, chosen: Iterable[str]) -> None:
        self._stores.frontiers[identity] = tuple(sorted(chosen))

    def previous(self, requested: str) -> tuple[str, Validators, str] | None:
        entry = self._stores.responses.get(requested)
        if entry is None:
            return None
        return entry.final_url, entry.validators(), entry.digest

    def payload(self, final_url: str, digest: str) -> Normalized | None:
        payload = self._stores.payloads.get(payload_key(final_url, digest))
        return payload.normalized() if payload is not None else None

    def first_fetched(self, final_url: str, digest: str) -> str | None:
        payload = self._stores.payloads.get(payload_key(final_url, digest))
        return payload.first_fetched if payload is not None else None


# What a validator holds for a page a 304 confirmed: it compares digests and
# never reads content, so any placeholder stands for the payload.
_CONFIRMED = Normalized("", None)


class _RecordHistory:
    """A validator knows validators and digests, never payloads: a 304 confirms
    the recorded digest (the placeholder above stands for the content), a 200 is
    normalized and compared."""

    def __init__(self, record: ListingRecord) -> None:
        self._record = record
        self._pages = {p.requested: p for p in record.pages}

    def frontier(self, identity: str) -> tuple[str, ...]:
        return self._record.frontier

    def remember_frontier(self, identity: str, chosen: Iterable[str]) -> None:
        pass  # a validator records nothing

    def previous(self, requested: str) -> tuple[str, Validators, str] | None:
        page = self._pages.get(requested)
        if page is None or page.final_url is None or page.digest is None:
            return None
        return page.final_url, page.validators(), page.digest

    def payload(self, final_url: str, digest: str) -> Normalized | None:
        return _CONFIRMED

    def first_fetched(self, final_url: str, digest: str) -> str | None:
        return None


async def fetch_listing(
    requested_urls: list[str],
    fetcher: Fetcher,
    history: _History,
    *,
    exclude_final: frozenset[str] = frozenset(),
) -> ListingResult:
    """Fetch every requested URL (conditionally where history allows), normalize,
    and yield the pages by final URL. A requested URL whose final URL is in
    ``exclude_final`` is suppressed after fetching and kept in the records as
    an alias (DESIGN.md §3, ``web_pages``)."""
    result = ListingResult(list(requested_urls), {}, {}, {})

    async def one(requested: str) -> None:
        prev = history.previous(requested)
        outcome = await fetcher.fetch(
            requested, (prev[0], prev[1]) if prev is not None else None
        )
        if isinstance(outcome, Unavailable):
            result.records[requested] = PageRecord(
                requested, reason=outcome.reason, transient=outcome.transient
            )
            if outcome.transient:
                result.transient[requested] = outcome.detail
            return
        if outcome.status == 304:
            assert prev is not None
            final_url, validators, digest = prev
            normalized = history.payload(final_url, digest)
            if normalized is None:
                # The version the validators name is gone: fetch again, unconditionally.
                again = await fetcher.fetch(requested, None)
                if isinstance(again, Unavailable):
                    result.records[requested] = PageRecord(
                        requested, reason=again.reason, transient=again.transient
                    )
                    if again.transient:
                        result.transient[requested] = again.detail
                    return
                outcome = again
            else:
                _add_page(
                    result,
                    requested,
                    final_url,
                    validators,
                    normalized,
                    digest,
                    history,
                    exclude_final,
                )
                return
        assert outcome.body is not None
        normalized = normalize(outcome)
        if looks_like_sign_in(normalized):
            result.records[requested] = PageRecord(requested, reason="sign-in")
            return
        _add_page(
            result,
            requested,
            outcome.final_url,
            outcome.validators,
            normalized,
            normalized.digest(),
            history,
            exclude_final,
        )

    await asyncio.gather(*(one(u) for u in requested_urls))
    return result


def _add_page(
    result: ListingResult,
    requested: str,
    final_url: str,
    validators: Validators,
    normalized: Normalized,
    digest: str,
    history: _History,
    exclude_final: frozenset[str],
) -> None:
    result.records[requested] = PageRecord(
        requested, final_url, validators.etag, validators.last_modified, digest
    )
    if final_url in exclude_final:
        return  # an alias of a page another listing read: suppressed, recorded
    if final_url not in result.pages:
        # The version's first-fetched time: kept when the payload store already
        # holds this version (the body's history), new otherwise.
        first = history.first_fetched(final_url, digest) or _now_minute()
        result.pages[final_url] = Page(final_url, validators, normalized, digest, first)


# --- the two sources ----------------------------------------------------------


class WebSource(Source[WebPage]):
    """What the two web kinds share: the run-local listing, the stores, the
    records and the details for the plan."""

    kind: ClassVar[str]
    observation: ClassVar[coco.DataDep]

    def __init__(self, sink: TraceSink | None, ctx: SessionContext | None) -> None:
        # The session's context gives the stores (``ctx.state``) and the finish
        # hook; a validator builds a source with neither and records nothing.
        self._sink = sink
        self._ctx = ctx
        self._listing: ListingResult | None = None
        self.patterns: tuple[str, ...] = ()

    # -- the listing, once per run --

    async def resources(self) -> dict[str, WebPage]:
        listing = await self.listed()
        return {
            url: WebPage(url, page.normalized, page.digest)
            for url, page in sorted(listing.pages.items())
        }

    async def listed(self) -> ListingResult:
        if self._listing is None:
            self._listing = await self._list_in_body()
        return self._listing

    def _stores(self) -> StateHandle[WebStores]:
        assert self._ctx is not None
        return self._ctx.state("web", WebStores(), WebStores)

    def _begin_run(self) -> StateHandle[WebStores]:
        """The stores for this body run: the first web source to touch them bumps
        the run index and registers the pruning for when the body finishes."""
        assert self._ctx is not None
        handle = self._stores()
        if _RUN_STARTED not in self._ctx.states:
            handle.value.run += 1
            self._ctx.states[_RUN_STARTED] = True
            self._ctx.on_finish.append(lambda: self._finish(handle))
        return handle

    async def _list_in_body(self) -> ListingResult:
        handle = self._begin_run()
        stores = handle.value
        async with http_client() as client:
            result = await self._compute(Fetcher(client), _StoreHistory(stores))
        self._remember(stores, result)
        handle.value = stores
        for requested, detail in sorted(result.transient.items()):
            if self._sink is not None:
                record_contained_failure(
                    self._sink, f"page:{requested}", TransientError(detail)
                )
        return result

    def _finish(self, handle: StateHandle[WebStores]) -> None:
        stores = handle.value
        stores.prune(stores.run)
        handle.value = stores

    def _remember(self, stores: WebStores, result: ListingResult) -> None:
        for page in result.pages.values():
            key = payload_key(page.final_url, page.digest)
            if key not in stores.payloads:
                n = page.normalized
                stores.payloads[key] = Payload(
                    n.title,
                    n.text,
                    base64.b64encode(n.pdf).decode("ascii")
                    if n.pdf is not None
                    else None,
                    n.truncated,
                    page.first_fetched,
                    stores.run,
                )
            else:
                stores.payloads[key] = replace(stores.payloads[key], run=stores.run)
        for requested, record in result.records.items():
            if record.final_url is not None and record.digest is not None:
                stores.responses[requested] = ResponseEntry(
                    record.final_url,
                    record.etag,
                    record.last_modified,
                    record.digest,
                    stores.run,
                )
            elif requested in stores.responses:
                # Unavailable this run: the entry and its version stay, marked
                # live, so the next run can still ask conditionally.
                stores.responses[requested] = replace(
                    stores.responses[requested], run=stores.run
                )

    async def _compute(self, fetcher: Fetcher, history: _History) -> ListingResult:
        """The listing: in the body over the session's stores, in a validator
        over the record alone."""
        raise NotImplementedError

    # -- observations: one record per source, carrying the listing --

    def observation_state(self, method: str, result: Json) -> object:
        assert self._listing is not None
        return self._listing.record()

    async def entries(self, resources: dict[str, WebPage]) -> Json:
        return [(url, page.digest) for url, page in resources.items()]

    # -- what the plan shows --

    def details(self) -> list[str]:
        assert self._listing is not None
        lines = []
        for url, page in sorted(self._listing.pages.items()):
            lines.append(f"{url} · {page.digest[:8]} · fetched {page.first_fetched}")
        for requested in sorted(self._listing.records):
            record = self._listing.records[requested]
            if record.reason is not None:
                lines.append(f"unavailable {requested} · {record.reason}")
        return lines

    # -- a read by an open session --

    async def read(self, args: dict[str, Json]) -> str | Document:
        url = canonical_url(item_key_arg(args))
        if url is None:
            raise ToolError("read needs an http(s) URL as `key`")
        handle = self._begin_run()
        stores = handle.value
        async with http_client() as client:
            result = await fetch_listing([url], Fetcher(client), _StoreHistory(stores))
        self._remember(stores, result)
        handle.value = stores
        record = result.records[url]
        state: PageReadState = None
        if record.final_url is not None and record.digest is not None:
            state = (record.final_url, record.etag, record.last_modified, record.digest)
        coco.use_data_dep(PAGE_READ, key=url, state=state)
        if url in result.transient and self._sink is not None:
            record_contained_failure(
                self._sink, f"page:{url}", TransientError(result.transient[url])
            )
        page = next(iter(result.pages.values()), None)
        if page is None:
            raise ToolError(f"page {url!r} is unavailable: {record.reason}")
        resource = WebPage(page.final_url, page.normalized, page.digest)
        return await content_of(page.final_url, resource, await resource.read())


class WebSearchSource(WebSource):
    """``web_search``: the pages a set of queries finds."""

    kind = "web_search"

    def __init__(
        self,
        engine_identity: str,
        queries: tuple[str, ...],
        sink: TraceSink | None = None,
        ctx: SessionContext | None = None,
    ) -> None:
        super().__init__(sink, ctx)
        self.engine_identity = engine_identity
        self.queries = queries

    def _identity(self) -> tuple[str, str, int, tuple[str, ...]]:
        return ("web_search", self.engine_identity, K, self.queries)

    def __coco_memo_key__(self) -> object:
        return self._identity()

    def identity_json(self) -> str:
        """The memo key as a string: the frontier history's key in the stores."""
        return canonical_json(list(self._identity()))

    def observation_key(self, method: str, args: str) -> object:
        return SearchKey(self.engine_identity, K, self.queries)

    async def _compute(self, fetcher: Fetcher, history: _History) -> ListingResult:
        engine = _ENGINES.get(self.engine_identity)
        if engine is None:
            raise RuntimeError(
                f"no search engine registered for {self.engine_identity!r}"
            )
        prev_frontier = history.frontier(self.identity_json())

        async def search(query: str) -> tuple[str, list[str]]:
            hits = await retrying(lambda: engine.search(query, 2 * K), backoff=_backoff)
            urls = [u for h in hits if (u := canonical_url(h.url)) is not None]
            return query, urls

        results = dict(await asyncio.gather(*(search(q) for q in self.queries)))
        chosen = frontier(prev_frontier, results, K, SEARCH_BUDGET)
        result = await fetch_listing(chosen, fetcher, history)
        history.remember_frontier(self.identity_json(), chosen)
        return result


class WebPagesSource(WebSource):
    """``web_pages``: the pages a list of URLs names, minus the pages other web
    sources of the session listed (the turn's handle-table snapshot)."""

    kind = "web_pages"

    def __init__(
        self,
        urls: tuple[str, ...],
        exclude_requested: frozenset[str],
        exclude_final: frozenset[str],
        sink: TraceSink | None = None,
        ctx: SessionContext | None = None,
    ) -> None:
        super().__init__(sink, ctx)
        self.urls = urls  # sorted admitted requested URLs
        self.exclude_requested = exclude_requested
        self.exclude_final = exclude_final

    @classmethod
    def admit(
        cls,
        named: Iterable[str],
        exclude_requested: frozenset[str],
        exclude_final: frozenset[str],
        sink: TraceSink | None = None,
        ctx: SessionContext | None = None,
    ) -> WebPagesSource:
        """Apply the first exclusion stage and the budget in the order given,
        then freeze the sorted effective list as the identity."""
        admitted: list[str] = []
        for url in named:
            if url in exclude_requested or url in exclude_final or url in admitted:
                continue
            admitted.append(url)
            if len(admitted) == PAGES_BUDGET:
                break
        return cls(tuple(sorted(admitted)), exclude_requested, exclude_final, sink, ctx)

    def __coco_memo_key__(self) -> object:
        return (
            "web_pages",
            self.urls,
            tuple(sorted(self.exclude_requested)),
            tuple(sorted(self.exclude_final)),
        )

    def observation_key(self, method: str, args: str) -> object:
        return PagesKey(
            self.urls,
            tuple(sorted(self.exclude_requested)),
            tuple(sorted(self.exclude_final)),
        )

    async def _compute(self, fetcher: Fetcher, history: _History) -> ListingResult:
        return await fetch_listing(
            list(self.urls), fetcher, history, exclude_final=self.exclude_final
        )


def exclusions(objects: Iterable[object]) -> tuple[frozenset[str], frozenset[str]]:
    """The requested and final URLs every web source among ``objects`` has
    listed: the exclusion sets of a ``web_pages`` source opened in a later turn."""
    requested: set[str] = set()
    final: set[str] = set()
    for obj in objects:
        if isinstance(obj, WebSource) and obj._listing is not None:
            requested.update(obj._listing.records)
            final.update(obj._listing.pages)
            final.update(
                r.final_url for r in obj._listing.records.values() if r.final_url
            )
    return frozenset(requested), frozenset(final)


# --- validators ---------------------------------------------------------------

# A validator here is a search per query or a request per page, so it must run
# once per record per run: the engine probes a session's records once (a
# double probe of the root session's records was observed with the scripted web
# on 2026-11-21 and fixed in the engine, cocoindex PR #2836). No verdict is
# cached here for a time window: it would hand a stale one to a user who edits
# and reruns at once.


async def _validate_listing(
    source: WebSource, prev: ListingRecord
) -> coco.MemoStateOutcome:
    """Reproduce the listing from the record alone and compare. Trouble with
    the environment makes the record invalid; it never raises (the engine
    would fail the run), except for cancellation and deadline expiry."""
    try:
        async with http_client() as client:
            result = await source._compute(Fetcher(client), _RecordHistory(prev))
    except (TransientError, httpx.HTTPError, RuntimeError, ValueError) as exc:
        logger.warning("web listing validation could not run: %s", exc)
        return coco.MemoStateOutcome(state=prev)
    if result.transient:
        return coco.MemoStateOutcome(state=prev)
    record = result.record()
    valid = record.content() == prev.content()
    if not valid:
        logger.debug("web listing changed: %s", "; ".join(prev.differences(record)))
    return coco.MemoStateOutcome(state=record, memo_valid=valid)


async def _validate_search(
    key: SearchKey, prev: ListingRecord
) -> coco.MemoStateOutcome:
    return await _validate_listing(WebSearchSource(key.engine, key.queries), prev)


async def _validate_pages(key: PagesKey, prev: ListingRecord) -> coco.MemoStateOutcome:
    source = WebPagesSource(
        key.urls, frozenset(key.exclude_requested), frozenset(key.exclude_final)
    )
    return await _validate_listing(source, prev)


async def _validate_page_read(key: str, prev: PageReadState) -> coco.MemoStateOutcome:
    """One conditional request; a page that did not exist validates while it
    still does not."""
    try:
        async with http_client() as client:
            fetcher = Fetcher(client)
            previous = (
                (prev[0], Validators(prev[1], prev[2])) if prev is not None else None
            )
            outcome = await fetcher.fetch(key, previous)
    except (TransientError, httpx.HTTPError) as exc:
        logger.warning("page read validation could not run: %s", exc)
        return coco.MemoStateOutcome(state=prev)
    if isinstance(outcome, Unavailable):
        if outcome.transient:
            return coco.MemoStateOutcome(state=prev)
        return coco.MemoStateOutcome(state=None, memo_valid=prev is None)
    if prev is None:
        return coco.MemoStateOutcome(state=prev)
    if outcome.status == 304:
        return coco.MemoStateOutcome(state=prev, memo_valid=True)
    normalized = normalize(outcome)
    digest = normalized.digest()
    state: PageReadState = (
        outcome.final_url,
        outcome.validators.etag,
        outcome.validators.last_modified,
        digest,
    )
    return coco.MemoStateOutcome(state=state, memo_valid=digest == prev[3])


SEARCH_OBSERVATION = coco.DataDep(
    "cocoindex_agents.web_search.observation", validate=_validate_search
)
PAGES_OBSERVATION = coco.DataDep(
    "cocoindex_agents.web_pages.observation", validate=_validate_pages
)
PAGE_READ = coco.DataDep("cocoindex_agents.web.page", validate=_validate_page_read)

WebSearchSource.observation = SEARCH_OBSERVATION
WebPagesSource.observation = PAGES_OBSERVATION


# --- configuration ------------------------------------------------------------


def default_engine(environ: Mapping[str, str]) -> SearchEngine:
    """The engine the environment selects: Tavily with ``TAVILY_API_KEY``, else
    Wikipedia, which needs no key. Registered, so validators find it too."""
    key = environ.get("TAVILY_API_KEY")
    return register_engine(TavilySearch(key) if key else WikipediaSearch())


__all__ = [
    "K",
    "PAGES_BUDGET",
    "SEARCH_BUDGET",
    "ListingRecord",
    "PageRecord",
    "SearchEngine",
    "SearchHit",
    "TavilySearch",
    "WebPage",
    "WebPagesSource",
    "WebSearchSource",
    "WebStores",
    "WikipediaSearch",
    "canonical_url",
    "default_engine",
    "exclusions",
    "frontier",
    "http_client",
    "normalize",
    "register_engine",
    "set_transport",
    "urls_in",
]

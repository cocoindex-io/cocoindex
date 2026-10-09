"""GitHub issues, pull requests, and releases of one repo, over the REST API.

Works against GitHub Enterprise by pointing GITHUB_API_URL at
https://<host>/api/v3. REST listings already carry each item's body, so the
ref keeps the payload for ``fetch`` but fingerprints only the key and version.
"""

from __future__ import annotations

import os
import re
from collections.abc import AsyncIterable, AsyncIterator
from dataclasses import dataclass, field
from typing import Any

import aiohttp

import cocoindex as coco

from records import Document
from sources.base import DocumentSource

GITHUB_API_URL = os.environ.get("GITHUB_API_URL", "https://api.github.com")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
_PAGE_SIZE = 100


@dataclass(frozen=True)
class GitHubRef:
    """A listing entry: only ``key`` and ``version`` take part in memoization,
    so an unchanged item is a memo hit (and the search relevance score, which
    varies between runs, is ignored)."""

    key: str
    version: str
    payload: dict[str, Any] = field(compare=False, hash=False, repr=False)

    def __coco_memo_key__(self) -> object:
        return (self.key, self.version)


def _session() -> aiohttp.ClientSession:
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    return aiohttp.ClientSession(headers=headers)


async def _json(resp: aiohttp.ClientResponse) -> Any:
    payload = await resp.json()
    if resp.status != 200:
        raise RuntimeError(
            f"GitHub request failed ({resp.status}): {payload.get('message')}"
        )
    return payload


async def _search(
    repo: str, qualifier: str, max_items: int
) -> AsyncIterator[tuple[coco.StableKey, GitHubRef]]:
    """Issues or pull requests of ``repo`` via the search endpoint, newest update
    first. The plain issues listing mixes both in; search filters server-side."""
    fetched = 0
    async with _session() as session:
        for page in range(1, max_items // _PAGE_SIZE + 2):
            params: dict[str, str | int] = {
                "q": f"repo:{repo} {qualifier}",
                "sort": "updated",
                "order": "desc",
                "per_page": _PAGE_SIZE,
                "page": page,
            }
            async with session.get(
                f"{GITHUB_API_URL}/search/issues", params=params
            ) as resp:
                items = (await _json(resp))["items"]
            for item in items:
                if fetched >= max_items:
                    return
                fetched += 1
                yield (
                    item["number"],
                    GitHubRef(str(item["number"]), item["updated_at"], item),
                )
            if len(items) < _PAGE_SIZE:
                return


def _item_document(
    source: str, kind: str, ref: GitHubRef, refs: tuple[str, ...] = ()
) -> Document:
    item = ref.payload
    return Document(
        source=source,
        key=ref.key,
        kind=kind,
        title=item["title"],
        text=item.get("body") or "",
        url=item["html_url"],
        author=item["user"]["login"],
        status=item["state"],
        updated_at=item["updated_at"],
        refs=refs,
    )


@dataclass(frozen=True)
class GitHubIssues(DocumentSource[GitHubRef]):
    repo: str
    max_items: int

    def refs(self) -> AsyncIterable[tuple[coco.StableKey, GitHubRef]]:
        return _search(self.repo, "is:issue", self.max_items)

    @coco.fn
    async def fetch(self, ref: GitHubRef) -> Document:
        return _item_document(self.name, "issue", ref)


_CONVENTIONAL_SCOPE_RE = re.compile(r"^\w+\(([^)]+)\)!?:")


@dataclass(frozen=True)
class GitHubPullRequests(DocumentSource[GitHubRef]):
    repo: str
    max_items: int

    def refs(self) -> AsyncIterable[tuple[coco.StableKey, GitHubRef]]:
        return _search(self.repo, "is:pr", self.max_items)

    @coco.fn
    async def fetch(self, ref: GitHubRef) -> Document:
        # Source-specific extraction: a conventional-commit title such as
        # "fix(neo4j): ..." names the entity it touches outright.
        scope = _CONVENTIONAL_SCOPE_RE.match(ref.payload["title"])
        refs = (scope.group(1).strip().lower(),) if scope else ()
        return _item_document(self.name, "pull_request", ref, refs)


@dataclass(frozen=True)
class GitHubReleases(DocumentSource[GitHubRef]):
    repo: str

    async def refs(self) -> AsyncIterator[tuple[coco.StableKey, GitHubRef]]:
        async with _session() as session:
            async with session.get(
                f"{GITHUB_API_URL}/repos/{self.repo}/releases",
                params={"per_page": _PAGE_SIZE},
            ) as resp:
                releases = await _json(resp)
        for release in releases:
            if not release["draft"]:
                tag = release["tag_name"]
                yield tag, GitHubRef(tag, release["published_at"] or "", release)

    @coco.fn
    async def fetch(self, ref: GitHubRef) -> Document:
        release = ref.payload
        return Document(
            source=self.name,
            key=ref.key,
            kind="release",
            title=release["name"] or ref.key,
            text=release.get("body") or "",
            url=release["html_url"],
            author=release["author"]["login"],
            status="prerelease" if release["prerelease"] else "published",
            updated_at=ref.version,
        )

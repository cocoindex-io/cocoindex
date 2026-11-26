"""The web as a source (DESIGN.md §3, *Sources: the web*): shape E over a fake
search engine and fake pages served by ``httpx.MockTransport``.

Every test counts model calls (inference) and HTTP requests: an unchanged
rerun makes no call and only conditional requests; a page that changed in its
boilerplate re-runs nothing; a page that changed materially re-runs its
worker's answer turn and the reducers on its path; the frontier rule, the
exclusion of pages already read, the unavailable classes, the validators'
behaviour under an outage, the engine as a tracked input, the empty outcomes
and the normalizer's guarantees per page type.
"""

from __future__ import annotations

import pathlib
import re
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass, field

import httpx
import pytest

import cocoindex as coco

from cocoindex_agents import (
    AGENTS_CONFIG,
    MODEL_CLIENT,
    AgentsConfig,
    RoleSpec,
    SessionResult,
    run_open_session,
)
from cocoindex_agents.errors import TransientError
from cocoindex_agents.hints import RunHints
from cocoindex_agents.report import RunReport, render_plan, render_stats_line
from cocoindex_agents.spec import SessionSpec
from cocoindex_agents.testing import CountingRegistry, ScriptedModel
from cocoindex_agents.tools import agents, sources, targets, web
from cocoindex_agents.trace import SessionScratch
from cocoindex_agents.transcript import ModelOutput, ToolCall, ToolResult, Transcript

ORCHESTRATOR = RoleSpec(
    "orchestrator",
    "test",
    ("open_source", "open_target", "for_each", "reduce", "declare", "call"),
)
WORKER = RoleSpec("worker", "test", ("read_input",))
REDUCER = RoleSpec("reducer", "test", ("read_input",))
READER = RoleSpec("reader", "test", ("call",), volatile_ok=True)

READ = "read the page and report findings and leads"
LEADS = "list the lead URLs"
REPORT = "write the report"


def call(tool: str, **args: object) -> ToolCall:
    return ToolCall(tool, dict(args))


# --- fake pages and a fake engine ----------------------------------------------


PARAGRAPHS = [
    "Incremental computation re-evaluates a program after a change so that only "
    "the parts affected by the change are recomputed, by recording dependencies "
    "between inputs and outputs and checking them against what changed.",
    "Build systems such as Make, Bazel and Shake are the classic examples: a target "
    "is rebuilt only when one of its inputs has a newer timestamp or a different "
    "content hash than it had the last time the target was built.",
    "In databases, incremental view maintenance keeps a materialized view current "
    "by applying only the rows that changed, instead of recomputing the whole query "
    "from scratch after every write to one of its base tables.",
    "The common thread is a dependency graph and a notion of what counts as a "
    "change; engines differ in how they record the graph and how fine-grained the "
    "change detection is, from whole files down to individual cells.",
]


def html_page(
    title: str,
    paragraphs: list[str],
    *,
    links: tuple[str, ...] = (),
    stamp: str = "2026-11-21 07:00",
    comments: int = 0,
) -> str:
    """An article with the boilerplate a real page carries: navigation, an ad with a
    nonce, a comments section and a footer with a timestamp and a request id."""
    body = "".join(f"<p>{p}</p>\n" for p in paragraphs)
    see = "".join(
        f'<p>See also <a href="{u}">{u}</a> for more on this subject.</p>'
        for u in links
    )
    talk = "".join(
        f'<div class="comment"><p>Comment {i}: nice post, thanks for sharing this '
        "with everyone here today.</p></div>"
        for i in range(comments)
    )
    return (
        f"<!DOCTYPE html><html><head><title>{title}</title>"
        f'<meta property="og:title" content="{title}"></head><body>'
        '<nav><ul><li><a href="/">Home</a></li><li><a href="/about">About</a></li>'
        '<li><a href="/blog">Blog</a></li></ul></nav>'
        f"<header><h1>{title}</h1></header><main><article>{body}{see}</article></main>"
        '<aside><div class="ad">Buy now! Limited offer id=4f9a</div></aside>'
        f'<section id="comments"><h2>Comments ({comments})</h2>{talk}</section>'
        f"<footer><p>Generated at {stamp} · request id 8c1e2</p><p>© Example Inc.</p>"
        "</footer></body></html>"
    )


def article(name: str, links: tuple[str, ...] = (), **kw: object) -> str:
    """A distinct article per name: the first paragraph names the page."""
    paragraphs = [f"This page is about {name}. " + PARAGRAPHS[0], *PARAGRAPHS[1:]]
    return html_page(f"Page {name}", paragraphs, links=links, **kw)  # type: ignore[arg-type]


@dataclass
class Page:
    body: str | bytes
    content_type: str = "text/html"
    etag: str | None = None
    last_modified: str | None = None
    status: int = 200
    redirect: str | None = None


@dataclass
class FakeWeb:
    """Pages by canonical URL, served through ``httpx.MockTransport`` with
    conditional requests honoured, redirects, robots and status codes; every
    request is counted."""

    pages: dict[str, Page] = field(default_factory=dict)
    robots: dict[str, str] = field(default_factory=dict)  # host -> robots.txt
    gets: Counter[str] = field(default_factory=Counter)
    downloads: Counter[str] = field(default_factory=Counter)
    not_modified: Counter[str] = field(default_factory=Counter)

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handle)

    def handle(self, request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            text = self.robots.get(request.url.host)
            return (
                httpx.Response(200, text=text)
                if text is not None
                else httpx.Response(404)
            )
        url = web.canonical_url(str(request.url))
        assert url is not None
        self.gets[url] += 1
        page = self.pages.get(url)
        if page is None:
            return httpx.Response(404)
        if page.redirect is not None:
            return httpx.Response(302, headers={"location": page.redirect})
        if page.status != 200:
            return httpx.Response(page.status)
        if (page.etag and request.headers.get("if-none-match") == page.etag) or (
            page.last_modified
            and request.headers.get("if-modified-since") == page.last_modified
        ):
            self.not_modified[url] += 1
            return httpx.Response(304)
        self.downloads[url] += 1
        headers = {"content-type": page.content_type}
        if page.etag:
            headers["etag"] = page.etag
        if page.last_modified:
            headers["last-modified"] = page.last_modified
        body = page.body.encode() if isinstance(page.body, str) else page.body
        return httpx.Response(200, content=body, headers=headers)

    def reset_counts(self) -> None:
        self.gets.clear()
        self.downloads.clear()
        self.not_modified.clear()


@dataclass
class FakeEngine:
    results: dict[str, list[str]]  # query -> ranked URLs
    identity: str = "fake:test"
    outage: bool = False
    calls: Counter[str] = field(default_factory=Counter)

    async def search(self, query: str, k: int) -> list[web.SearchHit]:
        if self.outage:
            raise TransientError("search engine down")
        self.calls[query] += 1
        return [
            web.SearchHit(u, u.rsplit("/", 1)[-1])
            for u in self.results.get(query, [])[:k]
        ]


# --- the scripted sessions ------------------------------------------------------


_URLS = re.compile(r"https?://[^\s)\]]+")


@dataclass
class Script:
    """Shape E as the recipe prescribes, followed by every role; the orchestrator
    reads cards as a real model would (items 0 → the no-evidence report)."""

    out: pathlib.Path
    queries: list[str]
    leads_task: str = LEADS
    report_task: str = REPORT
    reword_leads: bool = False  # the leads reducer phrases its answer differently
    seed: str | None = None  # a page the task names to start from (recipe 5)

    def __call__(
        self, spec: SessionSpec, prefix: Transcript, hints: RunHints
    ) -> ModelOutput:
        role = spec.role.name
        if role == "orchestrator":
            return self.orchestrate(prefix)
        if role == "worker":
            return self.work(prefix)
        if role == "reducer":
            return self.reduce(spec, prefix)
        raise AssertionError(role)

    def orchestrate(self, prefix: Transcript) -> ModelOutput:
        n = len(prefix.steps)
        results = [s.results for s in prefix.steps]
        if n == 0:
            opens = [
                call(
                    "open_source", kind="web_search", name="web", queries=self.queries
                ),
                call("open_target", kind="localfs_dir", name="out", path=str(self.out)),
            ]
            if self.seed is not None:
                opens.append(
                    call(
                        "open_source", kind="web_pages", name="given", urls=[self.seed]
                    )
                )
            return ModelOutput(calls=tuple(opens))
        card = results[0][0]
        if not card.ok:
            return ModelOutput(answer=f"could not open the web: {card.value}")
        if card.value["items"] == "0":
            if n == 1:
                return ModelOutput(
                    calls=(
                        call(
                            "declare",
                            target="tgt:out",
                            key="report.md",
                            value="# Report\n\nNo evidence could be found or fetched.\n",
                        ),
                    )
                )
            return ModelOutput(answer="no evidence")
        if n == 1:
            fan_outs = [call("for_each", source="src:web", key="pages", task=READ)]
            if self.seed is not None:
                fan_outs.append(
                    call("for_each", source="src:given", key="given", task=READ)
                )
            return ModelOutput(calls=tuple(fan_outs))
        first = ["coll:pages", "coll:given"] if self.seed is not None else "coll:pages"
        if n == 2:
            return ModelOutput(
                calls=(
                    call("reduce", collection=first, key="leads", task=self.leads_task),
                )
            )
        if n == 3:
            return ModelOutput(
                calls=(
                    call(
                        "open_source",
                        kind="web_pages",
                        name="leads",
                        urls=["res:leads"],
                    ),
                )
            )
        leads_card = results[3][0]
        has_leads = leads_card.ok and leads_card.value["items"] != "0"
        if n == 4 and has_leads:
            return ModelOutput(
                calls=(call("for_each", source="src:leads", key="pages2", task=READ),)
            )
        step_report = 5 if has_leads else 4
        if n == step_report:
            collections = ["coll:pages"] + (
                ["coll:given"] if self.seed is not None else []
            )
            if has_leads:
                collections.append("coll:pages2")
            return ModelOutput(
                calls=(
                    call(
                        "reduce",
                        collection=collections,
                        key="report",
                        task=self.report_task,
                    ),
                )
            )
        if n == step_report + 1:
            return ModelOutput(
                calls=(
                    call(
                        "declare", target="tgt:out", key="report.md", value="res:report"
                    ),
                )
            )
        return ModelOutput(answer="report written")

    def work(self, prefix: Transcript) -> ModelOutput:
        if not prefix.steps:
            return ModelOutput(calls=(call("read_input"),))
        value = prefix.steps[0].results[0].value
        if "text" not in value:  # a document reference (a PDF)
            return ModelOutput(
                answer=f"Findings:\n- a PDF of {value['bytes']} bytes, digest "
                f"{value['digest'][:8]} ({value['key']})\nLeads: none"
            )
        key, text = value["key"], value["text"]
        body = text.split("\n\n", 1)[1] if "\n\n" in text else text
        snippet = body.split(". ", 1)[0][:80]
        leads = [u for u in _URLS.findall(body) if u != key][:3]
        lines = [f"Findings:\n- {snippet} ({key})"]
        lines.append(
            "Leads:\n" + "\n".join(f"- {u} — linked from the page" for u in leads)
            if leads
            else "Leads: none"
        )
        return ModelOutput(answer="\n".join(lines))

    def reduce(self, spec: SessionSpec, prefix: Transcript) -> ModelOutput:
        if not prefix.steps:
            return ModelOutput(calls=(call("read_input"),))
        members: list[str] = prefix.steps[0].results[0].value
        if spec.task == self.leads_task or spec.task.startswith(LEADS):
            cited = {u for m in members for u in _URLS.findall(m.split("Leads", 1)[0])}
            leads: list[str] = []
            for m in members:
                for line in m.split("Leads", 1)[-1].splitlines():
                    for u in _URLS.findall(line):
                        if u not in cited and u not in leads:
                            leads.append(u)
            # The same URLs in other words of the same length, so the result's
            # widened size does not move either.
            why = "a link" if self.reword_leads else "a lead"
            return ModelOutput(
                answer="\n".join(f"{u} — {why}" for u in leads) or "none"
            )
        sources_ = sorted(
            {u for m in members for u in _URLS.findall(m.split("Leads", 1)[0])}
        )
        return ModelOutput(
            answer="# Report\n\n"
            + "\n\n".join(members)
            + "\n\nSources:\n"
            + "\n".join(f"- {u}" for u in sources_)
        )


# --- the fixture ----------------------------------------------------------------


@dataclass
class Fixture:
    env: coco.Environment
    out: pathlib.Path
    model: ScriptedModel
    tools: CountingRegistry
    web: FakeWeb
    engine: FakeEngine
    script: Script
    config: AgentsConfig
    scratch: SessionScratch = field(default_factory=SessionScratch)
    result: SessionResult | None = None
    _app: coco.App[..., SessionResult] | None = None

    def app(self) -> coco.App[..., SessionResult]:
        # One App per environment (a name registers once); the scratch is shared
        # and reset per run.
        if self._app is None:
            self.env.context_provider.provide(AGENTS_CONFIG, self.config)
            self._app = coco.App(
                coco.AppConfig(name="research", environment=self.env),
                run_open_session,
                self.config.spec("orchestrator", "research incremental computation"),
                scratch=self.scratch,
            )
        return self._app

    def run(self) -> SessionResult:
        self.model.reset()
        self.tools.reset()
        self.web.reset_counts()
        self.engine.calls.clear()
        self.scratch.ran = False
        result = self.app().update_blocking()
        assert isinstance(result, SessionResult)
        self.result = result
        return result

    def calls(self, role: str) -> int:
        return self.model.calls_for(role)

    def executed(self) -> dict[str, int]:
        return dict(self.tools.executed)

    def report(self) -> str:
        return (self.out / "report.md").read_text()

    def plan(self) -> str:
        assert self.result is not None
        return render_plan(self.run_report())

    def stats(self) -> str:
        return render_stats_line(self.run_report())

    def run_report(self) -> RunReport:
        assert self.result is not None
        return RunReport.from_result(
            "research",
            self.result.trace,
            ran=self.scratch.ran,
            model="scripted",
            wall_seconds=0.0,
            usage=None,
        )

    def switch_engine(self, engine: FakeEngine) -> None:
        """Another engine, re-provided through the config key (its memo key names
        the engine): the same App, a changed tracked input."""
        web.register_engine(engine)
        self.engine = engine
        self.config = AgentsConfig(
            roles=self.config.roles, tools=self.tools, search_engine=engine.identity
        )
        self.env.context_provider.provide(AGENTS_CONFIG, self.config)


def setup(
    coco_env: coco.Environment,
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    fake: FakeWeb,
    engine: FakeEngine,
    queries: list[str],
    *,
    extra_roles: tuple[RoleSpec, ...] = (),
) -> Fixture:
    web.set_transport(fake.transport())
    web.register_engine(engine)
    monkeypatch.setattr(web, "_backoff", lambda attempt: 0.0)
    out = tmp_path / "out"
    script = Script(out, queries)
    model = ScriptedModel(script)
    tools = CountingRegistry(
        [
            sources.open_source,
            sources.call_method,
            sources.read_input,
            targets.open_target,
            targets.declare,
            agents.for_each,
            agents.reduce,
        ]
    )
    config = AgentsConfig(
        roles={r.name: r for r in (ORCHESTRATOR, WORKER, REDUCER, *extra_roles)},
        tools=tools,
        search_engine=engine.identity,
    )
    coco_env.context_provider.provide(MODEL_CLIENT, model)
    return Fixture(coco_env, out, model, tools, fake, engine, script, config)


@pytest.fixture(autouse=True)
def _network_off() -> Iterator[None]:
    yield
    web.set_transport(None)


def site(fake: FakeWeb, **pages: Page) -> None:
    for name, page in pages.items():
        fake.pages[f"https://example.org/{name}"] = page


def url(name: str) -> str:
    return f"https://example.org/{name}"


# --- tests -----------------------------------------------------------------------


def test_a_research_run_reuses_unchanged_pages_and_reruns_only_a_changed_one(
    coco_env: coco.Environment, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeWeb()
    site(
        fake,
        a=Page(article("a", links=(url("l1"), url("l2"))), etag='"a1"'),
        b=Page(article("b", links=(url("l1"),)), etag='"b1"'),
        c=Page(
            article("c", links=(url("a"),)),
            last_modified="Mon, 17 Nov 2026 10:00:00 GMT",
        ),
        d=Page(article("d")),  # no validators: fetched and compared by digest
        l1=Page(article("l1"), etag='"l1"'),
        l2=Page(article("l2")),
    )
    engine = FakeEngine(
        {"q1": [url("a"), url("b"), url("c")], "q2": [url("b"), url("d")]}
    )
    f = setup(coco_env, tmp_path, monkeypatch, fake, engine, ["q1", "q2"])

    # Fresh: four pages from the searches, two leads (a is excluded: already read).
    assert f.run().answer == "report written"
    report = f.report()
    for name in ("a", "b", "c", "d", "l1", "l2"):
        assert f"({url(name)})" in report
    assert f.calls("orchestrator") == 8
    assert f.calls("worker") == 12 and f.calls("reducer") == 4
    assert fake.downloads[url("b")] == 1  # one fetch, although two queries returned it
    assert "fetched 20" in f.plan() and "example.org/a" in f.plan()

    # Unchanged: no model call, no tool, one search per query and one conditional
    # request per page by the validators (one record per source, however many
    # observations were made through it); validators answer 304, d is
    # re-downloaded and compared by digest.
    f.run()
    assert f.model.calls == 0 and f.executed() == {}
    assert dict(engine.calls) == {"q1": 1, "q2": 1}
    assert fake.not_modified[url("a")] == 1 and fake.not_modified[url("c")] == 1
    assert fake.downloads[url("d")] == 1 and fake.downloads[url("a")] == 0
    assert fake.gets[url("l1")] == 1
    assert (f.out / "report.md").read_text() == report

    # A trivial change: d's footer timestamp and a comment count; the digest of
    # the normalized page is unchanged, so nothing runs.
    fake.pages[url("d")] = Page(article("d", stamp="2026-11-22 09:30", comments=3))
    f.run()
    assert f.model.calls == 0 and f.executed() == {}

    # A rotated ETag on an unchanged page (a CDN edge): the conditional request
    # is answered 200, the digest is the same, the record refreshes its
    # validators and the session stays skipped; the next run gets a 304 again.
    fake.pages[url("a")] = Page(article("a", links=(url("l1"), url("l2"))), etag='"a2"')
    f.run()
    assert f.model.calls == 0 and f.executed() == {}
    assert fake.downloads[url("a")] == 1
    f.run()
    assert f.model.calls == 0 and fake.not_modified[url("a")] == 1

    # A material change to b, with a new ETag: b's worker answers again (its
    # read_input turn hits), then the leads reducer and the report reducer.
    fake.pages[url("b")] = Page(
        html_page(
            "Page b",
            ["This page is about b, revised. " + PARAGRAPHS[0], *PARAGRAPHS[1:]],
            links=(url("l1"),),
        ),
        etag='"b2"',
    )
    f.run()
    assert f.calls("worker") == 1 and f.calls("reducer") == 2
    assert f.calls("orchestrator") == 0
    assert "about b, revised" in f.report()
    # The validators fetched every page twice (the engine probes the root's records
    # for its component entry and for its memoized function's entry when the body
    # re-runs) with the refreshed validators and were answered 304; the body's
    # response cache still held a's rotated-away ETag, so its one request was a
    # 200 that normalized to the same digest and refreshed the cache.
    assert fake.gets[url("a")] == 3 and fake.not_modified[url("a")] == 2
    assert fake.downloads[url("a")] == 1


def test_the_frontier_keeps_a_set_under_rank_jitter_and_moves_only_at_its_edges(
    coco_env: coco.Environment, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeWeb()
    names = ["a", "b", "c", "d", "e", "f", "g", "h", "i"]
    site(fake, **{n: Page(article(n), etag=f'"{n}"') for n in names})
    # a2 is an alias: it redirects to a.
    fake.pages[url("a2")] = Page("", redirect=url("a"))
    ranked = [url(n) for n in names]
    engine = FakeEngine({"q": ranked, "q2": [url("a2"), url("b")]})
    f = setup(coco_env, tmp_path, monkeypatch, fake, engine, ["q", "q2"])

    # k = 4: the top four of q and q2's two, deduplicated by final URL → a, b, c, d.
    f.run()
    assert f.calls("worker") == 8
    assert fake.downloads[url("a")] == 1 and fake.gets[url("a2")] == 1

    # Jitter inside the top 2k with no newcomer in a top k: the set is unchanged.
    engine.results["q"] = [url("b"), url("a"), url("d"), url("c"), *ranked[4:]]
    f.run()
    assert f.model.calls == 0 and f.executed() == {}

    # An incumbent (d) slides to rank 5 while e enters the top 4: d stays (it is
    # within the top 2k), e is a newcomer with a free place → one new worker.
    engine.results["q"] = [
        url("a"),
        url("b"),
        url("c"),
        url("e"),
        url("d"),
        *ranked[5:],
    ]
    f.run()
    assert f.calls("worker") == 2 and f.calls("reducer") == 2
    assert f.calls("orchestrator") == 0  # 5 items are still "4–8"

    # d leaves every top 2k: its worker is dropped, no worker runs, the reducers do.
    engine.results["q"] = [
        url("a"),
        url("b"),
        url("c"),
        url("e"),
        *ranked[5:],
        url("d"),
    ]
    f.run()
    assert f.calls("worker") == 0 and f.calls("reducer") == 2
    assert f"({url('d')})" not in f.report()


def test_web_pages_excludes_what_the_search_read_and_is_keyed_on_the_url_set(
    coco_env: coco.Environment, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeWeb()
    site(
        fake,
        f=Page(article("f", links=(url("b"), url("g1"))), etag='"f"'),
        g1=Page(article("g1"), etag='"g1"'),
        g=Page(article("g"), etag='"g"'),
    )
    fake.pages[url("a")] = Page("", redirect=url("f"))  # the search reaches f through a
    fake.pages[url("b")] = Page("", redirect=url("f"))  # a lead that is an alias of f
    engine = FakeEngine({"q": [url("a")]})
    f = setup(coco_env, tmp_path, monkeypatch, fake, engine, ["q"])

    # The lead b redirects to f, already read: suppressed after fetching; g1 is read.
    f.run()
    assert f.calls("worker") == 4
    assert f.report().count(f"({url('f')})") == 1
    assert "unavailable" not in f.plan()

    # f changes materially and the leads reducer, re-run, rewords its answer
    # around the same URLs: the page-set source's identity is the URL set, so
    # g1's worker hits; only f's worker, the leads reducer and the report run.
    fake.pages[url("f")] = Page(
        html_page(
            "Page f",
            ["This page is about f, revised. " + PARAGRAPHS[0], *PARAGRAPHS[1:]],
            links=(url("b"), url("g1")),
        ),
        etag='"f2"',
    )
    f.script.reword_leads = True
    f.run()
    assert f.calls("worker") == 1 and f.calls("reducer") == 2
    assert f.calls("orchestrator") == 0

    # The alias b now redirects to a new page g: the recorded mapping b → f is
    # stale, the listing re-runs, and g is admitted.
    fake.pages[url("b")] = Page("", redirect=url("g"))
    f.run()
    assert f.calls("worker") == 2
    assert f"({url('g')})" in f.report()


def test_unavailable_pages_are_data_and_exhausted_transient_fetches_are_contained(
    coco_env: coco.Environment, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(web, "PDF_CAP", 2000)
    fake = FakeWeb()
    fake.robots["example.org"] = "User-agent: *\nDisallow: /private/\n"
    site(
        fake,
        ok=Page(article("ok"), etag='"ok"'),
        forbidden=Page("", status=403),
        gate=Page("", redirect="https://example.org/login?next=gate"),
        flaky=Page("", status=503),
    )
    fake.pages[url("private/x")] = Page(article("x"))
    fake.pages[url("big.pdf")] = Page(
        b"%PDF-1.4 " + b"0" * 5000, content_type="application/pdf"
    )
    engine = FakeEngine(
        {
            "q": [
                url("ok"),
                url("forbidden"),
                url("gate"),
                url("private/x"),
                url("big.pdf"),
                url("flaky"),
                url("missing"),
            ]
        }
    )
    monkeypatch.setattr(web, "K", 8)
    f = setup(coco_env, tmp_path, monkeypatch, fake, engine, ["q"])

    result = f.run()
    assert result.answer == "report written"
    assert f.calls("worker") == 2  # only ok was read
    assert fake.gets[url("private/x")] == 0  # never fetched
    plan = f.plan()
    for reason in ("sign-in", "robots", "too-large", "transient", "not-found"):
        assert f"· {reason}" in plan, reason
    # The fan-out's failed count stays 0; the transient page is a contained failure.
    assert result.trace.failures == ("page:" + url("flaky"),)
    assert "re-runs next run" in f.stats()

    # Still flaky: the session re-runs (the contained failure never validates), the
    # cards are unchanged, so no model call; still contained.
    f.run()
    assert f.model.calls == 0 and f.executed()["open_source"] == 2
    assert f.result is not None and f.result.trace.failures == ("page:" + url("flaky"),)

    # Fixed: the page appears, its worker runs, the reducers follow, nothing
    # failed. The item count went from 1 to 2, which crosses a bucket ("1–2" to
    # "2–4"), so the orchestrator sees a changed card and re-infers from there.
    fake.pages[url("flaky")] = Page(article("flaky"), etag='"flaky"')
    f.run()
    assert (
        f.calls("worker") == 2
        and f.result is not None
        and f.result.trace.failures == ()
    )
    assert f.calls("orchestrator") > 0
    assert f"({url('flaky')})" in f.report()


def test_an_engine_outage_is_contained_in_the_body_and_invalidates_in_a_validator(
    coco_env: coco.Environment, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeWeb()
    site(fake, a=Page(article("a"), etag='"a"'))
    engine = FakeEngine({"q": [url("a")]})
    f = setup(coco_env, tmp_path, monkeypatch, fake, engine, ["q"])
    f.run()
    assert f.calls("worker") == 2

    # Down during validation: the record is invalid (no exception), the body
    # re-runs, open_source fails as an ok=False result the model reads.
    engine.outage = True
    result = f.run()
    assert result.answer.startswith("could not open the web")
    assert f.result is not None and f.result.trace.failures == ("tool:open_source",)

    # Back: the body re-runs (a contained failure never validates). The run that
    # failed mounted no worker, so the engine dropped the worker's memo with its
    # path: the page is read again (the price of a whole-tool failure), and the
    # report is rewritten.
    engine.outage = False
    result = f.run()
    assert result.answer == "report written"
    assert f.calls("worker") == 2
    f.run()
    assert f.model.calls == 0


def test_switching_the_search_engine_reruns_the_search(
    coco_env: coco.Environment, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeWeb()
    site(fake, a=Page(article("a"), etag='"a"'), b=Page(article("b"), etag='"b"'))
    f = setup(
        coco_env, tmp_path, monkeypatch, fake, FakeEngine({"q": [url("a")]}), ["q"]
    )
    f.run()
    assert f"({url('a')})" in f.report()
    f.run()
    assert f.model.calls == 0

    # Another engine, same state directory: the old frontier is not reused; the
    # new engine's page is read and the report rewritten.
    f.switch_engine(FakeEngine({"q": [url("b")]}, identity="fake:other"))
    f.run()
    assert f.calls("worker") == 2
    report = f.report()
    assert f"({url('b')})" in report and f"({url('a')})" not in report


def test_empty_outcomes_are_reported_honestly_and_recover(
    coco_env: coco.Environment, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeWeb()
    site(fake, a=Page(article("a"), etag='"a"'))  # no links: no leads
    engine = FakeEngine({"q": []})
    f = setup(coco_env, tmp_path, monkeypatch, fake, engine, ["q"])

    # No hit: items 0, the orchestrator declares the no-evidence report.
    assert f.run().answer == "no evidence"
    assert "No evidence" in f.report()
    assert f.calls("worker") == 0

    # A hit appears: the search card changes, the flow runs; a has no leads, so
    # the page set is empty and the report is reduced from the first collection.
    engine.results["q"] = [url("a")]
    assert f.run().answer == "report written"
    assert f.calls("worker") == 2 and f.calls("reducer") == 4
    assert f"({url('a')})" in f.report()
    f.run()
    assert f.model.calls == 0


def test_a_page_the_task_names_is_read_first_and_its_change_reruns_only_its_path(
    coco_env: coco.Environment, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Recipe 5's starting page: a ``web_pages`` source opened in the first turn
    beside the search (same turn: not excluded from each other), read by its
    own fan-out, both collections reduced for leads and for the report. A
    change to that page re-runs its worker's answer and the two reducers; the
    search pages' workers hit."""
    fake = FakeWeb()
    site(
        fake,
        a=Page(article("a"), etag='"a"'),
        seed=Page(article("seed", links=(url("l1"),)), etag='"s1"'),
        l1=Page(article("l1"), etag='"l1"'),
    )
    engine = FakeEngine({"q": [url("a")]})
    f = setup(coco_env, tmp_path, monkeypatch, fake, engine, ["q"])
    f.script.seed = url("seed")

    f.run()
    assert f.calls("orchestrator") == 8 and f.calls("worker") == 6
    report = f.report()
    assert all(f"({url(n)})" in report for n in ("a", "seed", "l1"))

    f.run()
    assert f.model.calls == 0 and f.executed() == {}

    fake.pages[url("seed")] = Page(
        html_page(
            "Page seed",
            ["Seed, revised. " + PARAGRAPHS[0], *PARAGRAPHS[1:]],
            links=(url("l1"),),
        ),
        etag='"s2"',
    )
    f.run()
    assert f.calls("worker") == 1 and f.calls("reducer") == 2
    assert f.calls("orchestrator") == 0
    assert "Seed, revised" in f.report()


def test_a_planner_reading_a_page_is_skipped_while_the_page_is_unchanged(
    coco_env: coco.Environment, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``read(key)`` on a web source by an open session: recorded as a page read,
    validated by one conditional request, re-run on a content change."""
    fake = FakeWeb()
    site(fake, a=Page(article("a"), etag='"a1"'), b=Page(article("b")))
    engine = FakeEngine({"q": [url("a")]})
    reads: list[str] = []

    def script(spec: SessionSpec, prefix: Transcript, hints: RunHints) -> ModelOutput:
        n = len(prefix.steps)
        if spec.role.name == "reader":
            if n == 0:
                return ModelOutput(
                    calls=(
                        call(
                            "call",
                            handle="src:web",
                            method="read",
                            args={"key": url("b")},
                        ),
                    )
                )
            text = prefix.steps[0].results[0].value
            reads.append(text)
            return ModelOutput(answer=text.split("\n\n", 1)[1][:40])
        if n == 0:
            return ModelOutput(
                calls=(
                    call("open_source", kind="web_search", name="web", queries=["q"]),
                )
            )
        if n == 1:
            return ModelOutput(
                calls=(
                    call(
                        "call_agent",
                        key="reader",
                        task="read b",
                        role="reader",
                        inputs=["src:web"],
                    ),
                )
            )
        return ModelOutput(answer="done")

    web.set_transport(fake.transport())
    web.register_engine(engine)
    monkeypatch.setattr(web, "_backoff", lambda attempt: 0.0)
    tools = CountingRegistry(
        [sources.open_source, sources.call_method, agents.call_agent]
    )
    config = AgentsConfig(
        roles={
            r.name: r
            for r in (
                RoleSpec("orchestrator", "test", ("open_source", "call_agent")),
                READER,
            )
        },
        tools=tools,
        search_engine=engine.identity,
    )
    model = ScriptedModel(script)
    coco_env.context_provider.provide(MODEL_CLIENT, model)
    coco_env.context_provider.provide(AGENTS_CONFIG, config)
    app = coco.App(
        coco.AppConfig(name="reader", environment=coco_env),
        run_open_session,
        config.spec("orchestrator", "read one page"),
    )

    def run() -> None:
        model.reset()
        tools.reset()
        fake.reset_counts()
        app.update_blocking()

    run()
    assert model.calls_for("reader") == 2 and reads[0].startswith("# Page b\nURL: ")
    assert fake.downloads[url("b")] == 1

    # Unchanged: the page read validates by one request (b has no validators, so
    # a download compared by digest); nothing runs.
    run()
    assert model.calls == 0 and tools.executed == {}
    assert fake.downloads[url("b")] == 1

    # b changes: the reader re-runs its answer turn; the orchestrator's body runs
    # with no inference because its child's read changed.
    fake.pages[url("b")] = Page(article("b", stamp="x", comments=2))
    run()
    assert model.calls == 0 and tools.executed == {}  # boilerplate only: still skipped
    fake.pages[url("b")] = Page(html_page("Page b", ["Changed. " + PARAGRAPHS[0]]))
    run()
    assert model.calls_for("reader") == 1 and model.calls_for("orchestrator") == 0


def test_a_pdf_on_the_web_is_a_document(
    coco_env: coco.Environment, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeWeb()
    fake.pages[url("paper")] = Page(
        b"%PDF-1.4 fake pdf body", content_type="application/pdf", etag='"p"'
    )
    f = setup(
        coco_env, tmp_path, monkeypatch, fake, FakeEngine({"q": [url("paper")]}), ["q"]
    )
    f.run()
    assert "a PDF of 22 bytes" in f.report()
    f.run()
    assert f.model.calls == 0


def test_open_source_holds_each_kind_to_its_arguments_and_reduce_checks_its_collections(
    coco_env: coco.Environment, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeWeb()
    site(fake, a=Page(article("a")))
    engine = FakeEngine({"q": [url("a")]})
    outcomes: list[ToolResult] = []

    def script(spec: SessionSpec, prefix: Transcript, hints: RunHints) -> ModelOutput:
        n = len(prefix.steps)
        if spec.role.name != "orchestrator":
            return (
                ModelOutput(calls=(call("read_input"),))
                if n == 0
                else ModelOutput(answer="x")
            )
        if n == 0:
            return ModelOutput(
                calls=(
                    call(
                        "open_source",
                        kind="web_search",
                        name="w1",
                        path="x",
                        queries=["q"],
                    ),
                    call("open_source", kind="web_search", name="w2"),
                    call("open_source", kind="localfs", name="d", queries=["q"]),
                    call("open_source", kind="web_pages", name="p", urls=["res:none"]),
                    call("open_source", kind="web_search", name="web", queries=["q"]),
                )
            )
        if n == 1:
            return ModelOutput(
                calls=(call("for_each", source="src:web", key="pages", task=READ),)
            )
        if n == 2:
            return ModelOutput(
                calls=(
                    call(
                        "reduce",
                        collection=["coll:pages", "coll:pages"],
                        key="r1",
                        task=REPORT,
                    ),
                    call("reduce", collection=[], key="r2", task=REPORT),
                    call("reduce", collection="src:web", key="r3", task=REPORT),
                    call("reduce", collection=["coll:pages"], key="r4", task=REPORT),
                )
            )
        outcomes.extend(prefix.steps[0].results)
        outcomes.extend(prefix.steps[2].results)
        return ModelOutput(answer="done")

    web.set_transport(fake.transport())
    web.register_engine(engine)
    monkeypatch.setattr(web, "_backoff", lambda attempt: 0.0)
    tools = CountingRegistry(
        [
            sources.open_source,
            sources.read_input,
            sources.call_method,
            targets.open_target,
            targets.declare,
            agents.for_each,
            agents.reduce,
        ]
    )
    config = AgentsConfig(
        roles={r.name: r for r in (ORCHESTRATOR, WORKER, REDUCER)},
        tools=tools,
        search_engine=engine.identity,
    )
    coco_env.context_provider.provide(MODEL_CLIENT, ScriptedModel(script))
    coco_env.context_provider.provide(AGENTS_CONFIG, config)
    app = coco.App(
        coco.AppConfig(name="args", environment=coco_env),
        run_open_session,
        config.spec("orchestrator", "check arguments"),
    )
    result = app.update_blocking()
    assert isinstance(result, SessionResult) and result.answer == "done"
    texts = [(r.ok, str(r.value)) for r in outcomes]
    assert texts[0] == (
        False,
        "web_search does not take ['path']; it takes ['queries']",
    ), texts
    assert texts[1] == (False, "web_search needs ['queries']; it takes ['queries']")
    assert texts[2] == (
        False,
        "localfs needs ['path'] and does not take ['queries']; it takes ['path', 'patterns']",
    )
    assert texts[3][0] is False and "unknown handle 'res:none'" in texts[3][1]
    assert texts[4][0] is True
    assert texts[5] == (False, "reduce lists the same collection twice")
    assert texts[6] == (
        False,
        "reduce needs a coll: handle or a non-empty list of them",
    )
    assert texts[7] == (False, "'src:web' is a web_search handle, not a collection")
    assert texts[8][0] is True


# --- the normalizer, per page type -----------------------------------------------


def _digest(html: str, url_: str = "https://example.org/page") -> str:
    return web.normalize(
        web.Fetched(url_, 200, web.Validators(), "text/html", html.encode())
    ).digest()


def test_the_normalizer_keeps_content_edits_and_drops_boilerplate_edits_per_page_type() -> (
    None
):
    # An article: the fixture above.
    base = article("x", links=(url("y"),))
    assert _digest(base) == _digest(
        article("x", links=(url("y"),), stamp="2027-01-01", comments=5)
    )
    assert _digest(base) != _digest(
        html_page(
            "Page x",
            ["This page is about x, changed. " + PARAGRAPHS[0], *PARAGRAPHS[1:]],
        )
    )

    # A documentation page: a sidebar of links, a code block, a version footer.
    def docs(body_note: str, version: str) -> str:
        side = "".join(
            f'<li><a href="/docs/{i}">Section {i}</a></li>' for i in range(30)
        )
        return (
            "<html><head><title>Docs</title></head><body>"
            f'<nav class="sidebar"><ul>{side}</ul></nav><main>'
            f"<h1>Using the thing</h1><p>{PARAGRAPHS[0]} {body_note}</p>"
            f"<pre><code>thing --update ./docs</code></pre><p>{PARAGRAPHS[1]}</p></main>"
            f"<footer>Built with docs-tool {version} at 2026-11-21T07:00Z</footer></body></html>"
        )

    assert _digest(docs("", "1.0")) == _digest(docs("", "1.1"))
    assert _digest(docs("", "1.0")) != _digest(
        docs("Now with a new paragraph of guidance.", "1.0")
    )

    # An encyclopedia article: an infobox table, references, a last-edited line.
    def wiki(extra: str, edited: str) -> str:
        return (
            "<html><head><title>Incremental computing</title></head><body>"
            '<div id="mw-navigation"><a href="/wiki/Main_Page">Main page</a> <a href="/wiki/Random">Random</a></div>'
            f'<div id="content"><h1>Incremental computing</h1><table class="infobox"><tr><th>Paradigm</th><td>Reactive</td></tr></table>'
            f"<p>{PARAGRAPHS[0]}</p><p>{PARAGRAPHS[3]} {extra}</p>"
            f'<h2>References</h2><ol><li><a href="https://example.org/ref1">A paper</a></li></ol></div>'
            f'<div id="footer">This page was last edited on {edited}.</div></body></html>'
        )

    assert _digest(wiki("", "21 November 2026")) == _digest(
        wiki("", "22 November 2026")
    )
    assert _digest(wiki("", "x")) != _digest(wiki("A new sentence of content.", "x"))

    # Plain text and Markdown are themselves.
    def text(body: str) -> str:
        return web.normalize(
            web.Fetched(
                "https://example.org/t.md",
                200,
                web.Validators(),
                "text/markdown",
                body.encode(),
            )
        ).digest()

    assert text("# T\n\nbody") == text("# T\n\nbody\n") and text("# T\n\nbody") != text(
        "# T\n\nbody two"
    )

    # A repository page is an accepted limitation: the extractor returns the
    # sidebar-like parts deterministically; whether a README edit changes the
    # digest is not guaranteed and not asserted.
    def repo(readme: str) -> str:
        files = "".join(
            f'<tr><td><a href="/x/blob/f{i}">file{i}.py</a></td><td>fix things</td><td>2 days ago</td></tr>'
            for i in range(20)
        )
        return (
            "<html><head><title>x/repo</title></head><body>"
            f'<nav><a href="/">Code</a><a href="/issues">Issues</a><a href="/pulls">Pull requests</a></nav>'
            f"<table>{files}</table><article><p>{readme}</p></article>"
            "<aside><h2>About</h2><p>No description</p><p>Stars 12</p></aside></body></html>"
        )

    assert _digest(repo(PARAGRAPHS[0])) == _digest(repo(PARAGRAPHS[0]))


def test_frontier_rule_examples() -> None:
    results = {"q": ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]}
    assert web.frontier([], results, 4, 12) == ["a", "b", "c", "d"]
    # Incumbents stay while within the top 2k, newcomers come from the top k.
    assert web.frontier(
        ["d", "h"], {"q": ["a", "b", "c", "e", "d", "f", "g", "h"]}, 4, 12
    ) == [
        "d",
        "h",
        "a",
        "b",
        "c",
        "e",
    ]
    # Rank order then URL; the budget cuts.
    assert web.frontier([], {"q": ["b", "a"], "r": ["a", "c"]}, 4, 2) == ["a", "b"]
    # An incumbent that left every top 2k is gone.
    assert web.frontier(["z"], results, 4, 12) == ["a", "b", "c", "d"]


def test_canonical_urls_and_urls_in_text() -> None:
    assert web.canonical_url(
        "HTTPS://Example.org:443/a/b?utm_source=x&b=2&a=1#frag"
    ) == ("https://example.org/a/b?a=1&b=2")
    assert web.canonical_url("http://example.org:8080/") == "http://example.org:8080/"
    assert web.canonical_url("ftp://example.org/x") is None
    assert web.canonical_url("not a url") is None
    assert web.urls_in(
        "see https://example.org/a — and (https://example.org/b). https://example.org/a"
    ) == [
        "https://example.org/a",
        "https://example.org/b",
    ]

# Survey: searching, fetching and reading the web for a research flow (2026-11-21)

What exists today for an agent that searches the Internet, reads pages and
writes a report, and what it means for a flow whose reruns must reuse every
step whose inputs did not change. Three readings, each verified against the
pages cited on 2026-11-21 (vendor documentation, pricing and terms pages,
GitHub sources, papers), plus two measurements made from this machine. Items
no primary source confirmed are marked *unverified*. The design that follows
from it is `DESIGN-proposal.md`; the raw per-URL measurements are
`survey/results.md`.

## 1. Hosted fetch-and-extract services

| Service | Key | Free tier | Price | Returns | Normalized? | Origin caching headers? | Notes |
|---|---|---|---|---|---|---|---|
| Firecrawl `/v2/scrape` | yes | 1,000 credits/month, no card, 10 req/min | 1 credit/page; $19/5k, $99/100k | Markdown (default), cleaned/raw HTML, links, metadata (title, description, language, status; no date) | yes (`onlyMainContent` default); own cache 2 days (`maxAge`); `changeTracking` diffs against *its* previous scrape | no | `/extract` and the deep-research endpoint are removed 2026-12-10; terms unclear on storing output |
| Jina Reader `r.jina.ai` | **no** (20 req/min per IP); key: 500 req/min, 10M free tokens | tokens on output (≈$0.05/M, *unverified*) | Markdown or JSON (title, description, url, finalUrl, content, metadata, publishedTime when found) | yes; 5-minute cache (`X-No-Cache`); byte-stable on a probe | no (`cache-control: no-transform` only) | `s.jina.ai` search now needs a key (`401` measured); Apache-2.0 self-hostable |
| Tavily Extract | yes | 1,000 credits/month, no card, 100 req/min | 0.2 credit/URL basic, 0.4 advanced | `raw_content` Markdown or text, beta `published_date`; **no title** | yes; no documented cache | no | ≤20 URLs/request; terms: internal business use |
| Exa `/contents` | yes | $10/month credit | $1/1k pages per content type | title, url, publishedDate, author, text, highlights, summary; `source: cached\|crawled` | yes; cache-first (`maxAgeHours`, `snapshotAsOf`) | no | terms bar copying/storing output beyond a browser cache |
| Diffbot Article | yes | 10,000 calls/month, 5/min | 1 credit/page; $299/250k | title, text, normalized HTML, date, author, siteName, language, tags | yes; richest metadata | no | Crawlbot honours robots.txt |
| ScrapingBee | yes | 1,000 trial credits | 1–75 credits/request | raw HTML (optional Markdown/text) | optional | **yes** (`Spb-` prefixed pass-through) | the only service that forwards origin headers |

Observations: no hosted extractor exposes the origin's `ETag`,
`Last-Modified` or `Cache-Control`, or a content hash of its own, except
ScrapingBee's header pass-through; every one of them caches on its side with
a freshness knob; the storage terms differ widely (Jina claims no rights in
output; Exa and Firecrawl are broad enough to be unclear; Tavily is internal
use). Change detection therefore has to be done by the client on normalized
content, which a local extractor does as well and for free.

## 2. Search APIs

| Service | Key | Free tier | Price | Returns | Notes |
|---|---|---|---|---|---|
| Tavily Search | yes | 1,000 credits/month, no card | 1 credit basic, 2 advanced; PAYG $0.008/credit | title, url, snippet, score, optional raw content and published date; ≤20 results | the default engine of GPT Researcher and LangChain's open deep research; terms: internal use, no sharing of API access |
| Exa Search | yes | $10/month | $4–$15 per 1k | title, url, publishedDate, author, optional contents | 10 QPS |
| Brave Search | yes (card) | $5 credit/month (≈1k queries); the free plan was withdrawn for new sign-ups in February 2026 | $5 per 1k | title, url, description, extra snippets | terms forbid storing or caching results beyond transient use and require "Powered by Brave"; the most restrictive surveyed |
| Serper | yes | 2,500 queries once | $0.30–$1.00 per 1k | Google organic results | scrapes Google; terms *unverified* |
| SerpApi | yes | 250/month | from $25 per 1k | organic fields, raw SERP, Markdown | cached searches free for an hour |
| Jina `s.jina.ai` | yes | 10M tokens/key | ≥10,000 tokens/request | 5 results in Reader form | blocked without a key |
| You.com | yes; a free MCP endpoint with 100 queries/day needs none | $10 (+$90 after verification) | $5 per 1k; live pages $1 per 1k | title, url, snippets or page content | terms *unverified* |
| Bing Search APIs | — | — | retired 2025-08-11 | — | replaced by Grounding with Bing ($14 per 1k): no raw results, display rules |
| DuckDuckGo | no | unofficial | free | Instant Answers officially; `html`/`lite` endpoints are what scrapers use | `ddgs` 9.16 scrapes ten engines "for educational purposes only"; a `202` challenge was seen on the HTML endpoint; not for a public example |
| SearXNG | no | free (self-host) | infrastructure | url, title, content, engines, score | JSON output must be enabled; public instances rate-limit scripted clients |
| Wikipedia / Wikidata | no (descriptive `User-Agent` required) | 200 req/min | free | search API; REST `page/html` and `page/summary` | **the only keyless source with origin validators**: `ETag W/"<revision>/<tid>"` and a `304` measured on `page/html`; article pages give `Last-Modified` and `304`; CC BY-SA 4.0 (Wikidata CC0), storage allowed |
| Common Crawl index | no | free | free | CDX rows with a SHA-1 `digest`; WARC/WAT hold origin headers | a lookup by URL in a past crawl, not a search engine |

### What the model providers offer natively

| Tool | Price | What comes back | Content to the developer? |
|---|---|---|---|
| OpenAI `web_search` (Responses API; chat completions only via `gpt-5-search-api`; the `-search-preview` models shut down 2026-07-23) | $10 per 1k calls + search content tokens ($25 per 1k for the preview tool on non-reasoning models) | `url_citation` annotations (url, title, offsets); all URLs consulted on request | no |
| Anthropic `web_search` | $10 per 1k searches + tokens | results with url, title, page_age, `encrypted_content`; citations with up to 150 characters of `cited_text` | no (opaque) |
| Anthropic `web_fetch` | tokens only (≈2,500 per 10 kB page) | full plain text or PDF, title, **`retrieved_at`** | yes; honours robots.txt; only URLs already in the context; server-side cache (`use_cache`) |
| Gemini Google Search grounding | 5,000 free requests/month on Gemini 3+, then $14 per 1k | queries, `url_citation`, search suggestions that must be displayed | no; terms forbid training on grounded results and limit storage |
| Gemini `url_context` | tokens | retrieval status per URL; 20 URLs/request, 34 MB each | no |

LiteLLM 1.110.2 exposes `web_search_options` (`search_context_size`,
`user_location`) on chat completions for openai, xai, vertex_ai, anthropic,
gemini and perplexity, and the `web_search_preview` tool on its Responses
API; Anthropic's `web_fetch` is not documented in LiteLLM, and citation
formats are not normalized across providers. A model-native search is a
prose answer with citations, priced per call on top of tokens, and tied to
the model's provider; it is not a ranked result list a validator can repeat.

## 3. Local extraction libraries

| Library | Task | Deterministic? | Boilerplate quality (F1) | Footprint | License | Last release |
|---|---|---|---|---|---|---|
| trafilatura 2.3.1 | main content + metadata, txt/Markdown/XML/JSON | yes with `include_comments=False`, no dedup, no metadata (dedup uses a process-wide LRU; metadata calls htmldate with today's date) | own eval 0.926; Scrapinghub 0.958; Bevendorff 2023 0.883 (best mean); Sandia 2024 0.937 | lxml + six pure-Python deps | Apache-2.0 | 2026-10-06 |
| readability-lxml 0.9 | main content as an HTML fragment | yes | own-eval 0.853; Scrapinghub 0.922; Bevendorff median 0.970 (best median); multilingual 0.838 (best) | lxml, chardet, cssselect | Apache-2.0 | 2026-08-27 |
| Mozilla Readability.js 0.6 | the reference algorithm | yes, but needs Node | Scrapinghub 0.947 | Node + jsdom | Apache-2.0 | 2025-03 |
| Resiliparse 2.0.1 | fast text, simple main-content filter | yes | 0.811 at 0.3× trafilatura's time; Bevendorff 0.859 | 1.75 MB wheel (Rust) | Apache-2.0 | 2026-11-04 |
| jusText 3.0.2 | paragraph classifier | yes (needs a language) | 0.862; it is trafilatura's fallback | lxml | BSD-2 | 2025-02 |
| boilerpy3 1.0.7 | Boilerpipe port | yes | 0.807 | none | Apache-2.0 | 2023-11 (stale) |
| newspaper4k 0.9.6 | news articles | date scoring reads the clock | 0.803, 6.8× slower | 11 deps | MIT | 2026-11-01 |
| goose3 3.1.22 | articles | langdetect seed risk | 0.810 | 8 deps | Apache-2.0 | 2026-07 |
| markdownify 1.2.4 / html2text 2025.4 / inscriptis 2.7.7 | HTML → Markdown or text, no boilerplate removal | yes | — (0.66–0.69) | bs4 / none / lxml | MIT / **GPL-3.0** / Apache-2.0 | 2026-11 / 2025-04 / 2026-11 |
| crawl4ai 0.9.6 | Playwright crawler + Markdown + pruning/BM25/LLM filters | rendering no; filters yes | unbenchmarked | ≈35 deps + Chromium, hundreds of MB | Apache-2.0 + attribution | 2026-11-20 |
| Playwright 1.65 | JavaScript rendering | no (live DOM) | — | 53 MB wheel + ≈281 MB Chromium (≈78 MiB headless shell); ≈0.7 GB RAM | Apache-2.0 | 2026-11-14 |
| htmldate 1.11 | publication date | only with `max_date` pinned | date F 0.926/0.949 | lxml, dateparser | Apache-2.0 | 2026-10 |
| pypdf 7.1 / pdfminer.six / pdfplumber | PDF text | yes | — | pure Python / MIT deps | BSD-3 / MIT / MIT | 2026-11 |
| PyMuPDF 2.1 | PDF text, fastest | yes | — | 69 MB wheel | **AGPL-3.0** or commercial | 2026-11 |
| docling 2.151 | PDF/Office → Markdown with layout models | not guaranteed (ML) | — | torch + ≈530 MB models, 2.4–6.2 GB RAM | MIT | 2026-11 |
| ReaderLM-v2 / MinerU-HTML | model-based extraction | no (generative, GPU) | WCXB 0.741 / 0.827 at 10 s / 1.6 s per page on an A100 | 1.5B / 0.6B models | CC-BY-NC / Apache-2.0 | 2025-03 / 2026-11 |

What the benchmarks say, read across trafilatura's own evaluation
(2026-10, 990 pages), Scrapinghub's article benchmark (181 pages, archived
2026-06), Bevendorff et al. SIGIR 2023 (3,985 pages, 14 extractors), Sandia
2024, a SIGIR 2025 multilingual study and WCXB 2026 (2,008 pages, seven page
types): on article-like pages trafilatura and readability tie at the top
(differences ≤0.03 F1, not significant in Sandia's test), readability is
more stable on non-English pages, Resiliparse is within 0.02–0.03 at three
times the speed, model-based extractors win only on forums and listings at
a GPU second per page, and nothing heuristic exceeds about 0.80 on forums,
listings and product pages. Every benchmark was built from HTML fetched with
JavaScript off: article pages generally do not need rendering.

## 4. How current deep-research systems work

| System | Planning | Following leads | Citations | Cost bounds | Published cost or length |
|---|---|---|---|---|---|
| OpenAI Deep Research (ChatGPT; the API models `o3-deep-research`/`o4-mini-deep-research` shut down 2026-07-23) | learned end to end; the UI clarifies and lets the plan be edited | internal; since 2026-02 the user can interrupt and restrict sites | `url_citation` spans (url, title, offsets); a "sources used" list | 5–30 minutes; historically $10/$40 per M tokens plus $10 per 1k searches | — |
| Perplexity Deep Research / Agent API (Sonar chat completions ended 2026-09-27) | effort levels bundle model, step budget and tools; `ultra` runs three agents and merges | step budgets 1/5/15/40/300 with reminders; a 10k-token fetch budget; a small model extracts "only the relevant passages, quoted exactly" (5,561 → 293 tokens) | `[n]`/`[web:n]` markers over `search_results` with stable ids | steps, tokens per search, compaction at 150k | 2–4 minutes; `ultra` $21.71 per task on WANDR |
| Gemini Deep Research (app and the Interactions API agent) | returns a plan for approval (`collaborative_planning`) | gap-driven loop; 60-minute hard cap, ≈20 typical | a `citations` field (format undocumented) | time cap; list-rate tokens | ≈80 searches, 250k input and 60k output tokens, **$1–3 per task**; Max ≈160 searches, 900k in, $3–7 |
| Anthropic Research + `web_search`/`web_fetch` | a lead agent decomposes and saves the plan to memory; 1 / 2–4 / 10+ subagents by complexity | subagents reflect after every result; source-quality heuristics; `max_uses` | always-on citations with ≤150 characters of quoted text; a CitationAgent attaches citations after drafting; `web_fetch` returns `retrieved_at` | `max_uses`, `max_content_tokens`, 45-minute cap | 5–15 minutes; agents use ≈4× the tokens of chat, multi-agent ≈15×; token volume explained 80% of quality variance |
| GPT Researcher | one search, then 3 sub-queries (`MAX_ITERATIONS`); deep mode a breadth × depth tree | static fan-out, 5 results per query, a visited-URL set, an embedding filter at 0.42 | `([cite](url))` inline plus an APA list, prompt-enforced | 3 iterations, 1,200 words, 25k-word context | ≈3 minutes; deep mode ≈$0.40 and ≈5 minutes |
| STORM / Co-STORM | perspectives from related articles' tables of contents, simulated expert conversations, outline first | fixed 3×3×3×3 budget; Co-STORM's moderator asks about retrieved-but-unused material | inline `[n]` into one reference table; citation precision and recall ≈85% | the structural knobs | the one open-source system with resumable stage artifacts (`do_research=False`), for the same topic only |
| LangChain open_deep_research | clarify → research brief → a supervisor splits by sub-topic | `think_tool` reflection; stop after 5 calls, 3 sources or 2 similar searches; 6 supervisor rounds; URLs deduplicated across queries; pages cut at 50k characters and summarized | `[n]` plus a `### Sources` list; "preserve sources verbatim" through compression | iterations × units × calls | ≈$0.46–1.87 per report on Deep Research Bench |
| Hugging Face open-deep-research (smolagents) | a code-agent manager and a browser agent, replanning every 4 steps | ReAct within 12/20 steps | none | step caps | GAIA 55% |
| dzhng/deep-research | breadth 4, depth 2; 3 queries → 3 learnings → 3 follow-ups, breadth halving per level | follow-up questions only | a `## Sources` list of visited URLs, no inline citations | breadth, depth, 5 results, 25k characters per page | — |
| Jina node-DeepResearch | no upfront plan; a FIFO queue of gap questions | query deduplication by embeddings, URL ranking with a reranker, two URLs per host, a 1M-token budget with "beast mode" at the end | GFM footnotes with url, title, **exact quote and dateTime** (page date or `Last-Modified`), ≤10 references | the token budget | "hundreds" of queries |
| Firecrawl deep research (removed 2026-12-10) / `/v2/agent` | key research areas, then iterative search | `maxDepth` 7, `timeLimit` 270 s, `maxUrls` 20 | a `sources` list | credits per URL; `maxCredits` 2,500 on the agent | "a few hundred credits" per agent run |
| Tavily `/research` | Planning → WebSearch → ResearchSubtopic (pro) → Generating | opaque | numbered, MLA, APA or Chicago plus `sources` | credit floors and ceilings | mini 4–110 credits, pro 15–250 (≈$0.03–2.00) |

Patterns. Every system is plan → search → read → reflect → repeat → write;
the commercial ones push the loop into a trained model and expose only an
envelope (steps, tokens, minutes); the open-source ones split into static
fan-outs (GPT Researcher, STORM, dzhng), supervisor-plus-reflection
(LangChain, Anthropic's published design) and one budget-forced loop (Jina,
smolagents). What made outputs stable: a fixed fan-out, one final writing
pass over compressed findings (LangChain dropped parallel section writing
because it "produced disjoint reports"), and a dedicated citation pass or a
strict carry-through rule. What made them unstable: recursion per result,
contradictory citation prompts, relevance thresholds that differ between
config and code, and search-engine nondeterminism, which nobody controls.
Only Anthropic's `web_fetch` records a fetch time; only Jina and Anthropic
keep an exact quote; only Perplexity's Agent API uses stable result ids. The
2026 trend is page-level extraction before reading, to control the dominant
cost, and "run several researchers and merge" at $4–20 a task.

**None of them reruns incrementally.** No system reuses a previous run's
sources, findings or report when the question is repeated or a source
changed; the nearest things are STORM's resumable stage files (same topic
only), the server-side page caches of Anthropic's `web_fetch`, OpenAI's
cache-only mode and Exa's index (invisible, unaddressable), and job stores
that re-serve a finished report. Every run pays the full search, fetch and
token bill again. That gap is the point of the flow proposed here.

## 5. Measured on this machine

### Conditional requests and byte stability (51 URLs, two fetches ten minutes apart)

The table is `survey/results.md`, produced by `survey/measure.py` over
`survey/urls.txt`. In short: 45 of 51 fetched; 33 send a validator, 31
answer `304` to it at once and 22 still do ten minutes later with the stored
validators (the GitHub, Hugging Face, Tavily and Exa pages rotate their ETag
per response; four home pages had new content); 20 of 42 HTML pages differed
byte-for-byte although their content had not changed (median line similarity
0.993), and after trafilatura all 41 extractable pages, including every one
of those 19, were identical; readability + markdownify likewise 42 of 42;
every PDF was byte-identical and carried validators; `robots.txt` allowed
the agent on 46 of 51 URLs; Medium, Reuters, AP, the New York Times and
Stack Overflow refused the fetch outright. trafilatura's output was
identical on repeated extraction of the same HTML, and with `include_links`
the Markdown carries 20–120 absolute links per page, which is what a lead
needs.

### Keyless endpoints, probed once each

| Endpoint | Result |
|---|---|
| `s.jina.ai` search | `401 AuthenticationRequiredError` |
| `r.jina.ai` reader | `200`, Markdown with title, URL and published time; `x-ratelimit-limit: 20` per minute; `cache-control: no-transform`; no origin validators |
| `html.duckduckgo.com/html/?q=` | `200` with ten parseable results from one host; the agent survey saw a `202` challenge on another attempt |
| `lite.duckduckgo.com/lite/?q=` | `202` (a challenge page) |
| Wikipedia search API (`list=search`) | `200`, five results with titles, snippets and sizes |
| Wikipedia REST `page/html/<title>` | `200`, `etag: W/"<revision>/<tid>/view"`, `last-modified`; `304` on `If-None-Match` |
| Brave Search API without a token | `422`, token required |
| Tavily without a key | `401` |

## 6. What this decides for the design

1. Reuse rests on conditional requests where the origin honours them and on
   a digest of *normalized* content everywhere: raw bytes change under an
   unchanged page about half the time; normalized text did not change once.
2. Local extraction with trafilatura is the normalizer: deterministic, top
   of every benchmark on articles, Apache-2.0, no key, no cost, and it gives
   the links a lead needs; readability is the fallback of equal quality.
3. Search is behind one protocol: Wikipedia without a key (the only keyless
   engine that is official and storable, and whose pages carry validators),
   Tavily with one (free tier, no card, the engine the open-source research
   agents use). Brave's terms and the loss of its free plan, Jina's new key
   requirement, and DuckDuckGo's unofficial endpoints rule them out.
4. Citations carry URL, fetch time and a short quote, as Jina's references
   and Anthropic's `web_fetch` do; the systems that keep citations through
   synthesis do it by carrying them verbatim through every compression step.
5. Costs: a first run of the proposed flow is of the order of Gemini Deep
   Research's published envelope (tens of searches, ≈250k input tokens); a
   rerun with unchanged sources is zero model calls, which no surveyed system
   offers.

## Sources

Hosted services and search APIs (read 2026-11-21): firecrawl.dev/pricing;
docs.firecrawl.dev (scrape, search, crawl, extract, alpha/deep-research);
firecrawl.dev/terms-of-service; jina.ai/reader, jina.ai/api-dashboard/rate-limit,
jina.ai/legal, github.com/jina-ai/reader; docs.tavily.com (extract, search,
api-credits, rate-limits, search-crawler), tavily.com/terms; docs.exa.ai
(get-contents, search, rate-limits), exa.ai/pricing, crawler.exa.ai, Exa's
terms PDF; diffbot.com/pricing and docs; scrapingbee.com/pricing and
documentation; brave.com/search/api, api-dashboard.search.brave.com
(documentation, terms of service), implicator.ai and alternativeto.net on the
February 2026 plan change, brave.com/blog/most-powerful-search-api-for-ai;
serper.dev; serpapi.com/pricing, /legal, /search-api; you.com/docs (billing,
rate-limits, retrieve-page-content), you.com/plans;
learn.microsoft.com (bing-search-api-retirement, foundry web-search tool),
microsoft.com/bing/apis; pypi.org/project/ddgs and /duckduckgo-search,
github.com/deedy5/ddgs, duckduckgo.com/terms; docs.searxng.org (search_api,
limiter), searx.space; mediawiki.org (Wikimedia_APIs/Rate_limits,
API:Etiquette, Wikimedia_APIs/Conditional_requests), foundation.wikimedia.org
User-Agent policy, en.wikipedia.org/api/rest_v1 spec,
wikidata.org/wiki/Wikidata:Data_access; commoncrawl.org/get-started and
/terms-of-use, index.commoncrawl.org; developers.openai.com/api/docs
(guides/tools-web-search, pricing, deprecations);
platform.claude.com/docs (web-search-tool, web-fetch-tool, pricing);
aistudio.google.com/docs (google-search, url-context, pricing, terms,
deep-research); docs.litellm.ai (completion/web_search, providers/anthropic,
providers/gemini, response_api), github.com/BerriAI/litellm/releases.

Extraction libraries: pypi.org and GitHub pages of trafilatura,
readability-lxml, mozilla/readability, ReadabiliPy, newspaper4k, goose3,
boilerpy3, jusText, inscriptis, Resiliparse, html2text, markdownify,
crawl4ai, playwright, htmldate, docling, pypdf, pdfminer.six, pdfplumber,
PyMuPDF, markitdown, defuddle, dom-to-semantic-markdown, ReaderLM-v2,
mineru-html, magic-html; trafilatura.readthedocs.io (evaluation,
usage-python, settings) and its `core.py`, `deduplication.py`, `metadata.py`,
`settings.py`; readability's `readability.py`; Readability.js source;
htmldate's `settings.py` and `validators.py`; playwright.dev/python/docs/browsers;
datawookie.dev (2025-06-06) on Playwright memory; almanac.httparchive.org 2024
JavaScript chapter. Benchmarks: github.com/scrapinghub/article-extraction-benchmark;
Bevendorff et al., SIGIR 2023 (downloads.webis.de/publications/papers/bevendorff_2023c.pdf);
Sandia SAND2024-10208 (osti.gov/servlets/purl/2429881); Fundus
(arxiv.org/html/2403.15279v1); Bournonville et al., SIGIR 2025
(dias.users.greyc.fr/publications/sigir2025.pdf); WCXB (arxiv.org/html/2605.21097v1);
MainWebBench/AICC (arxiv.org/html/2511.16397v1); Dripper (arxiv.org/html/2511.23119v1).

Deep-research systems: openai.com/index/introducing-deep-research,
openai.com/index/deep-research-system-card, OpenAI's model, deprecation and
pricing pages, the deep-research cookbook, help.openai.com deep-research FAQ,
the-decoder.com (2026-02) on the GPT-5.2 update; perplexity.ai/hub/blog
(introducing-perplexity-deep-research, improvements-to-every-agent-api-effort-level),
docs.perplexity.ai (sonar-deep-research, agent-api/effort, tools/web-search,
models, pricing, changelog), openrouter.ai/perplexity/sonar-deep-research;
blog.google (2024-12-11, 2025-12-11, 2026-04-21), aistudio.google.com/docs/deep-research,
discuss.ai.google.dev thread 112804; claude.com/blog/research,
claude.com/blog/integrations, anthropic.com/engineering/multi-agent-research-system;
x.ai/news/grok-3, the-decoder.com (2025-03-22); gpt-researcher's README,
`config/variables/default.py`, `skills/researcher.py`, `skills/deep_research.py`,
`context/compression.py`, `prompts.py`, docs.gptr.dev (config, deep_research,
building-gpt-researcher); arxiv.org/abs/2402.14207 and /2408.15232,
stanford-oval/storm README and `storm_wiki/engine.py`, `collaborative_storm/engine.py`;
langchain.com/blog/open-deep-research, open_deep_research README,
`configuration.py`, `prompts.py`, `utils.py`; huggingface.co/blog/open-deep-research,
smolagents `examples/open_deep_research`; dzhng/deep-research README and
`src/deep-research.ts`; jina-ai/node-DeepResearch README, `src/agent.ts`,
`src/utils/schemas.ts`, jina.ai/news/a-practical-guide-to-implementing-deepsearch-deepresearch;
docs.firecrawl.dev/features/agent and alpha/deep-research,
firecrawl.dev/blog/deep-research-api; docs.tavily.com research endpoint,
research-streaming tutorial, api-credits; Alibaba-NLP/DeepResearch README and
arxiv.org/abs/2510.24701; MiroMindAI/MiroThinker README;
moonshotai.github.io/Kimi-Researcher; arxiv.org/abs/2506.11763 (DeepResearch Bench).

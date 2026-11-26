# Measured: 51 pages, two fetches ten minutes apart (2026-11-21)

Produced by `measure.py` over `urls.txt` from a Linux host with `User-Agent: cocoindex-agents-survey/0.1 (+https://github.com/cocoindex-io/cocoindex)`. Raw data: `/tmp/research-survey/data/results.json` on the machine that ran it (not checked in: it holds page bodies' digests and headers only, 60 KB).

| Measure | Result |
|---|---|
| URLs tried | 51 (docs 11, blogs 11, news 9, Wikipedia 4, GitHub 5, PDFs 4, papers and vendor pages 7) |
| Fetched (200) in both passes | 45 of 51; refused outright (401/403): 5 (Medium, Reuters, AP, NYT, Stack Overflow); 1 multiple-choice (300) |
| `robots.txt` allows our agent | 46 of 51 (3 hosts had no robots file; 2 disallow) |
| Send `ETag` | 24 of 45 |
| Send `Last-Modified` | 16 of 45 |
| Send either validator | 33 of 45 |
| Answer 304 to a conditional request right away | 31 of 33 pages (21 of 24 to `If-None-Match`; every `Last-Modified` to `If-Modified-Since`) |
| Answer 304 ten minutes later with the stored validators (a rerun) | 22 of 33; the other 11 send a validator that no longer matches: four GitHub repository pages, Hugging Face, Tavily and Exa rotate their ETag per response, and four home pages (BBC, the Guardian, Simon Willison, Joel on Software) had new content |
| `Cache-Control` | 35 of 45 send one; `max-age=0`/`must-revalidate`/`no-cache`/`no-store`/`private` on 25; a positive `max-age` on 12 (60 s to a day) |
| Raw bytes identical between the two fetches | 25 of 45 (HTML 22 of 42; every PDF) |
| Raw similarity of the HTML pages that differed (difflib on lines) | median 0.993, minimum 0.667 (Hacker News front page) |
| Normalized text identical (trafilatura, Markdown, no comments) | 41 of 41 HTML pages, including all 19 of the 19 whose bytes differed |
| Normalized text identical (readability-lxml + markdownify) | 42 of 42 |
| Extracted size, trafilatura | median ≈ 8k characters on articles, docs and Wikipedia; news front pages under 1k; GitHub repository pages 3–15k |

## Per URL

| URL | Status | ETag | Last-Modified | 304 now | 304 after 10 min | Cache-Control | Raw same | Raw sim. | trafilatura same | readability same |
|---|---|---|---|---|---|---|---|---|---|---|
| docs.python.org/3/library/asyncio.html | 200 | yes | yes | yes | yes | `—` | yes | 1.0 | yes | yes |
| developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag | 200 | yes | yes | yes | yes | `public, max-age=3600` | yes | 1.0 | yes | yes |
| docs.astral.sh/uv/ | 200 | no | no | — | — | `public, max-age=0, must-revalidate` | no | 0.9997 | yes | yes |
| www.postgresql.org/docs/current/mvcc-intro.html | 200 | no | no | — | — | `—` | yes | 1.0 | yes | yes |
| kubernetes.io/docs/concepts/overview/ | 200 | yes | no | yes | yes | `public,max-age=0,must-revalidate` | yes | 1.0 | yes | yes |
| react.dev/learn | 200 | yes | no | yes | yes | `public, max-age=0, must-revalidate` | yes | 1.0 | yes | yes |
| docs.rs/tokio/latest/tokio/ | 200 | yes | no | yes | yes | `max-age=0` | yes | 1.0 | yes | yes |
| docs.anthropic.com/en/docs/agents-and-tools/tool-use/web-search-tool | 200 | no | no | — | — | `private, no-cache, no-store, max-age=0, must-revalidate` | no | 0.9892 | yes | yes |
| platform.openai.com/docs/guides/tools-web-search | 200 | yes | yes | yes | yes | `public, max-age=0, must-revalidate` | yes | 1.0 | yes | yes |
| www.rfc-editor.org/rfc/rfc9111.html | 200 | yes | no | yes | yes | `—` | yes | 1.0 | yes | yes |
| docs.github.com/en/rest | 200 | no | no | — | — | `public, max-age=60` | yes | 1.0 | yes | yes |
| simonwillison.net/ | 200 | no | yes | yes | no | `s-maxage=200` | no | 0.9995 | yes | yes |
| www.joelonsoftware.com/2000/08/09/the-joel-test-12-steps-to-better-code/ | 200 | no | yes | yes | no | `max-age=58, must-revalidate` | no | 0.9908 | yes | yes |
| martinfowler.com/articles/microservices.html | 200 | yes | yes | yes | yes | `—` | yes | 1.0 | yes | yes |
| blog.cloudflare.com/ | 200 | no | no | — | — | `—` | no | 0.9459 | yes | yes |
| engineering.fb.com/ | 200 | no | no | — | — | `max-age=300, must-revalidate` | yes | 1.0 | yes | yes |
| cocoindex.io/blogs/ | 200 | no | yes | yes | yes | `max-age=600` | no | 0.9375 | yes | yes |
| huggingface.co/blog | 200 | yes | no | no | no | `—` | no | 0.9814 | yes | yes |
| openai.com/index/introducing-deep-research/ | 200 | no | no | — | — | `public, max-age=0, must-revalidate` | no | 0.9989 | yes | yes |
| www.lesswrong.com/ | 200 | no | no | — | — | `public, max-age=0, must-revalidate` | no | 0.907 | yes | yes |
| medium.com/@trafilatura | 403 | no | no | — | — | `—` | no | 0.0 | — | yes |
| trafilatura.readthedocs.io/en/latest/evaluation.html | 200 | yes | yes | yes | yes | `max-age=1800, stale-if-error=86400, public` | yes | 1.0 | yes | yes |
| www.bbc.com/news | 200 | yes | no | no | no | `private, stale-if-error=90, stale-while-revalidate=30, max-age=0, must-revalidate` | no | 0.9984 | yes | yes |
| www.reuters.com/technology/ | 401 | no | no | — | — | `max-age=0, private, no-cache, no-store, must-revalidate` | no | 0.0 | yes | yes |
| techcrunch.com/ | 200 | no | no | — | — | `max-age=300, must-revalidate` | yes | 1.0 | yes | yes |
| www.theverge.com/ | 200 | yes | no | yes | yes | `no-store,private` | yes | 1.0 | yes | yes |
| arstechnica.com/ | 200 | no | no | — | — | `—` | no | 0.9931 | yes | yes |
| news.ycombinator.com/ | 200 | no | no | — | — | `private; max-age=0` | no | 0.6667 | — | yes |
| apnews.com/ | 403 | no | no | — | — | `—` | no | 0.0 | — | yes |
| www.theguardian.com/technology | 200 | yes | no | yes | no | `max-age=60, stale-while-revalidate=6, stale-if-error=864000, private` | no | 0.9978 | yes | yes |
| www.nytimes.com/section/technology | 403 | no | no | — | — | `max-age=0, private, no-cache, no-store, must-revalidate` | no | 0.0 | yes | yes |
| en.wikipedia.org/wiki/HTTP_ETag | 200 | no | yes | yes | yes | `private, s-maxage=0, max-age=0, must-revalidate, no-transform` | yes | 1.0 | yes | yes |
| en.wikipedia.org/wiki/Web_scraping | 200 | no | yes | yes | yes | `private, s-maxage=0, max-age=0, must-revalidate, no-transform` | yes | 1.0 | yes | yes |
| en.wikipedia.org/wiki/Incremental_computing | 200 | no | yes | yes | yes | `private, s-maxage=0, max-age=0, must-revalidate, no-transform` | yes | 1.0 | yes | yes |
| en.wikipedia.org/wiki/Large_language_model | 200 | no | yes | yes | yes | `private, s-maxage=0, max-age=0, must-revalidate, no-transform` | yes | 1.0 | yes | yes |
| github.com/cocoindex-io/cocoindex | 200 | yes | no | yes | no | `max-age=0, private, must-revalidate` | no | 0.9942 | yes | yes |
| github.com/adbar/trafilatura | 200 | yes | no | yes | no | `max-age=0, private, must-revalidate` | no | 0.9936 | yes | yes |
| github.com/mendableai/firecrawl | 200 | yes | no | yes | no | `max-age=0, private, must-revalidate` | no | 0.9961 | yes | yes |
| github.com/assafelovic/gpt-researcher | 200 | yes | no | yes | no | `max-age=0, private, must-revalidate` | no | 0.9939 | yes | yes |
| raw.githubusercontent.com/cocoindex-io/cocoindex/main/README.md | 200 | yes | no | yes | yes | `max-age=300` | yes | 1.0 | yes | yes |
| arxiv.org/pdf/2402.14207 | 200 | yes | yes | yes | yes | `—` | yes | 1.0 | — | — |
| arxiv.org/pdf/1706.03762 | 200 | yes | yes | yes | yes | `—` | yes | 1.0 | — | — |
| www.rfc-editor.org/rfc/rfc9111.pdf | 200 | yes | no | yes | yes | `public, max-age=86400` | yes | 1.0 | — | — |
| www.w3.org/TR/2008/REC-xml-20081126/REC-xml-20081126.pdf | 300 | no | no | — | — | `max-age=31536000` | yes | 1.0 | yes | yes |
| arxiv.org/abs/2402.14207 | 200 | no | yes | yes | yes | `max-age=3600` | yes | 1.0 | yes | yes |
| pypi.org/project/trafilatura/ | 200 | yes | no | yes | yes | `—` | yes | 1.0 | yes | yes |
| stackoverflow.com/questions/11177548/what-is-the-difference-between-etag-and-last-modified | 403 | no | no | — | — | `—` | no | 0.0 | — | yes |
| jina.ai/reader/ | 200 | no | yes | yes | yes | `max-age=600` | yes | 1.0 | yes | yes |
| www.firecrawl.dev/pricing | 200 | no | no | — | — | `private, no-cache, no-store, max-age=0, must-revalidate` | no | 0.9868 | yes | yes |
| tavily.com/ | 200 | yes | no | yes | no | `s-maxage=300` | no | 0.987 | yes | yes |
| exa.ai/ | 200 | yes | no | yes | no | `public, max-age=0, must-revalidate` | no | 0.9961 | yes | yes |

# mypy: ignore-errors
# A one-off measurement script (flows/research/SURVEY.md), not part of the package.
"""Measure how the pages a research flow hits behave under HTTP caching.

Pass 1: GET each URL (recording ETag, Last-Modified, Cache-Control, Age, Vary,
Content-Type), save the body, then re-request with If-None-Match and with
If-Modified-Since to see whether the origin answers 304. Pass 2, DELAY seconds
later: GET again; compare the raw bytes (identical? line similarity) and the
text after deterministic extraction (trafilatura; readability+markdownify), and
redo the conditional requests with pass 1's validators, as a rerun would.
Also checks robots.txt for each URL.
"""

import difflib
import hashlib
import json
import re
import sys
import time
import urllib.robotparser
from pathlib import Path
import httpx
import trafilatura
from readability import Document as RDoc
from markdownify import markdownify

DATA = Path(sys.argv[1])
DATA.mkdir(parents=True, exist_ok=True)
DELAY = int(sys.argv[2]) if len(sys.argv) > 2 else 600
UA = "cocoindex-agents-survey/0.1 (+https://github.com/cocoindex-io/cocoindex)"
URLS = [
    line.strip()
    for line in (Path(__file__).with_name("urls.txt")).read_text().splitlines()
    if line.strip() and not line.startswith("#")
]

client = httpx.Client(
    follow_redirects=True,
    timeout=30,
    headers={"User-Agent": UA, "Accept": "text/html,application/pdf,*/*;q=0.8"},
)
robots: dict[str, object] = {}


def allowed(url: str):
    host = re.match(r"https?://[^/]+", url).group(0)
    if host not in robots:
        rp = urllib.robotparser.RobotFileParser(host + "/robots.txt")
        try:
            r = client.get(host + "/robots.txt")
            if r.status_code == 200:
                rp.parse(r.text.splitlines())
                robots[host] = rp
            else:
                robots[host] = r.status_code
        except Exception as e:
            robots[host] = f"error {type(e).__name__}"
    rp = robots[host]
    if isinstance(rp, urllib.robotparser.RobotFileParser):
        return {
            "robots": "present",
            "allowed": rp.can_fetch(UA, url),
            "allowed_star": rp.can_fetch("*", url),
            "crawl_delay": rp.crawl_delay("*"),
        }
    return {"robots": str(rp), "allowed": None}


def hdrs(r):
    h = r.headers
    return {
        k: h.get(k)
        for k in (
            "etag",
            "last-modified",
            "cache-control",
            "age",
            "vary",
            "content-type",
            "expires",
            "x-cache",
            "cf-cache-status",
        )
    }


def get(url, extra=None):
    t = time.time()
    try:
        r = client.get(url, headers=extra or {})
        return {
            "status": r.status_code,
            "headers": hdrs(r),
            "final_url": str(r.url),
            "body": r.content,
            "secs": round(time.time() - t, 2),
        }
    except Exception as e:
        return {
            "status": None,
            "error": f"{type(e).__name__}: {e}",
            "secs": round(time.time() - t, 2),
        }


def extract(body: bytes, ctype: str):
    if "pdf" in (ctype or ""):
        return {"trafilatura": None, "readability": None}
    html = body.decode("utf-8", "replace")
    try:
        t = trafilatura.extract(
            html,
            include_comments=False,
            include_tables=True,
            output_format="markdown",
            favor_precision=True,
        )
    except Exception as e:
        t = f"ERR {type(e).__name__}"
    try:
        rd = markdownify(RDoc(html).summary(), heading_style="ATX")
    except Exception as e:
        rd = f"ERR {type(e).__name__}"
    return {"trafilatura": t, "readability": rd}


def sim(a: bytes, b: bytes):
    if a == b:
        return 1.0
    la, lb = (
        a.decode("utf-8", "replace").splitlines(),
        b.decode("utf-8", "replace").splitlines(),
    )
    if len(la) > 20000 or len(lb) > 20000:  # huge: byte-level quick ratio
        return round(
            difflib.SequenceMatcher(None, a[:400000], b[:400000]).quick_ratio(), 4
        )
    return round(difflib.SequenceMatcher(None, la, lb).ratio(), 4)


def cond(url, h):
    out = {}
    if h.get("etag"):
        r = get(url, {"If-None-Match": h["etag"]})
        out["if_none_match"] = r["status"]
    if h.get("last-modified"):
        r = get(url, {"If-Modified-Since": h["last-modified"]})
        out["if_modified_since"] = r["status"]
    return out


def sha(b):
    return hashlib.sha256(b).hexdigest()[:16]


results = {}
print(f"pass 1 over {len(URLS)} urls", file=sys.stderr)
for i, url in enumerate(URLS):
    rec = {"url": url, "robots": allowed(url)}
    r1 = get(url)
    rec["pass1"] = {k: v for k, v in r1.items() if k != "body"}
    if r1.get("body") is not None:
        (DATA / f"{i:02d}.p1").write_bytes(r1["body"])
        rec["pass1"]["bytes"] = len(r1["body"])
        rec["pass1"]["sha"] = sha(r1["body"])
        rec["pass1"]["conditional"] = cond(url, r1["headers"])
        ex = extract(r1["body"], r1["headers"].get("content-type"))
        rec["pass1"]["extract"] = {
            k: (len(v) if isinstance(v, str) else None) for k, v in ex.items()
        }
        (DATA / f"{i:02d}.p1.json").write_text(json.dumps(ex))
    results[url] = rec
    print(
        f"  {i:02d} {r1.get('status')} {rec['pass1'].get('bytes')}B etag={bool(r1.get('headers', {}).get('etag'))} lm={bool(r1.get('headers', {}).get('last-modified'))} cc={r1.get('headers', {}).get('cache-control')!r} {url}",
        file=sys.stderr,
    )
    time.sleep(0.5)
(DATA / "pass1.json").write_text(json.dumps(results, indent=1, default=str))

print(f"sleeping {DELAY}s", file=sys.stderr)
time.sleep(DELAY)

print("pass 2", file=sys.stderr)
for i, url in enumerate(URLS):
    rec = results[url]
    r2 = get(url)
    rec["pass2"] = {k: v for k, v in r2.items() if k != "body"}
    p1 = DATA / f"{i:02d}.p1"
    if r2.get("body") is not None and p1.exists():
        b1 = p1.read_bytes()
        b2 = r2["body"]
        (DATA / f"{i:02d}.p2").write_bytes(b2)
        rec["pass2"]["bytes"] = len(b2)
        rec["pass2"]["sha"] = sha(b2)
        rec["pass2"]["raw_identical"] = b1 == b2
        rec["pass2"]["raw_similarity"] = sim(b1, b2)
        ex1 = json.loads((DATA / f"{i:02d}.p1.json").read_text())
        ex2 = extract(b2, r2["headers"].get("content-type"))
        rec["pass2"]["extract_identical"] = {
            k: (ex1.get(k) == ex2.get(k)) if ex1.get(k) is not None else None
            for k in ex2
        }
        rec["pass2"]["extract_similarity"] = {
            k: (
                sim(ex1[k].encode(), ex2[k].encode())
                if isinstance(ex1.get(k), str) and isinstance(ex2.get(k), str)
                else None
            )
            for k in ex2
        }
        rec["pass2"]["validators_unchanged"] = {
            k: rec["pass1"]["headers"].get(k) == r2["headers"].get(k)
            for k in ("etag", "last-modified")
        }
        rec["pass2"]["conditional_with_pass1_validators"] = cond(
            url, rec["pass1"]["headers"]
        )
    print(
        f"  {i:02d} {r2.get('status')} ident={rec['pass2'].get('raw_identical')} sim={rec['pass2'].get('raw_similarity')} ext={rec['pass2'].get('extract_identical')} cond={rec['pass2'].get('conditional_with_pass1_validators')} {url}",
        file=sys.stderr,
    )
    time.sleep(0.5)
(DATA / "results.json").write_text(json.dumps(results, indent=1, default=str))
print("done", file=sys.stderr)

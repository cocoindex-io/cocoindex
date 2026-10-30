#!/usr/bin/env python3
"""Collect the raw material for a release note: commits in a tag range, each
annotated with its PR number and GitHub author handle, plus a first-pass
new-contributor list.

This does the tedious, mechanical part — running git log, parsing PR numbers
out of the subjects of the default branch's first-parent commits (one per
landed pull request: a squash commit, or the merge commit of an upstream
merge), and batch-querying `gh` for author handles — so
the human/agent can spend their attention on the judgment part (what to keep,
how to group, how to word it).

Usage:
    collect_changes.py <prev_tag> [head_ref]

    prev_tag   The previous release tag, e.g. v1.0.7
    head_ref   What you're releasing (default: HEAD). Prefer the remote
               default branch (e.g. origin/main) after a `git fetch` — a
               stale local branch silently drops the newest merges.

Output: JSON on stdout with shape:
    {
      "range": "v1.0.7..origin/main",
      "commits": [
        {"sha": "1936301a", "subject": "...", "type": "feat", "scope": "py",
         "pr": 2091, "author_handle": "georgeh0", "author_handle_source": "pr",
         "author_name": "Jiangzhou", "author_email": "..."},
        ...
      ],
      "new_contributors": ["hakuuww", "davidmyriel", ...],
      "unresolved_prs": [2244],
      "warnings": ["HEAD is 1 commit(s) behind origin/main ..."]
    }

`author_handle_source` is "pr" (the PR's author — what release notes credit),
"noreply_email" (parsed from a @users.noreply.github.com commit email when the
PR lookup failed — the commit author, usually but not always the PR author),
or null. `unresolved_prs` lists PR numbers that appear in commit subjects but
whose `gh` lookup failed; when they also 404 on GitHub the usual cause is a
deleted/hidden author account, which makes the PR link dead even though the
commit is real.

Requires: git, and `gh` authenticated against the repo's GitHub remote. Runs
on Python 3.9+ (the `python3` macOS ships), so no project environment is needed.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

CONV_RE = re.compile(r"^(?P<type>\w+)(?:\((?P<scope>[^)]*)\))?(?P<bang>!)?:\s")
PR_RE = re.compile(r"\(#(\d+)\)\s*$")
NOREPLY_RE = re.compile(
    r"^(?:\d+\+)?(?P<handle>[A-Za-z0-9][A-Za-z0-9-]*(?:\[bot\])?)"
    r"@users\.noreply\.github\.com$"
)


def sh(args: list[str]) -> str:
    return subprocess.run(
        args, check=True, capture_output=True, text=True
    ).stdout.strip()


def pr_author(pr: int) -> str | None:
    """Look up the GitHub login that authored a PR. None on any failure."""
    try:
        out = subprocess.run(
            ["gh", "pr", "view", str(pr), "--json", "author", "-q", ".author.login"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except subprocess.CalledProcessError:
        return None
    # `gh` reports an app's PR (Dependabot) as `app/<name>`; release notes
    # credit it the way GitHub's generated notes do, as `<name>[bot]`.
    if out.startswith("app/"):
        return out.removeprefix("app/") + "[bot]"
    return out or None


def stale_head_warning(head: str) -> str | None:
    """Warn when `head` lags its upstream — releasing a stale local branch
    silently drops the newest merges. Best-effort fetch so the comparison is
    against the remote's real state, not a stale tracking ref."""
    try:
        upstream = sh(
            [
                "git",
                "rev-parse",
                "--abbrev-ref",
                "--symbolic-full-name",
                f"{head}@{{upstream}}",
            ]
        )
    except subprocess.CalledProcessError:
        return None  # no upstream (already a remote ref, a tag, or a raw SHA)
    remote = upstream.split("/", 1)[0]
    try:
        subprocess.run(
            ["git", "fetch", "--quiet", remote],
            check=False,
            capture_output=True,
            timeout=30,
        )
    except subprocess.TimeoutExpired:
        pass
    try:
        behind = int(sh(["git", "rev-list", "--count", f"{head}..{upstream}"]))
    except subprocess.CalledProcessError:
        return None
    if behind:
        return (
            f"{head} is {behind} commit(s) behind {upstream} — the range is "
            f"missing the newest merges; pass {upstream} as head_ref"
        )
    return None


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2
    prev = sys.argv[1]
    head = sys.argv[2] if len(sys.argv) > 2 else "HEAD"
    rng = f"{prev}..{head}"

    warnings = []
    stale = stale_head_warning(head)
    if stale:
        warnings.append(stale)

    # %x1f is an ASCII unit separator — safe against subjects containing pipes.
    # First parents only: one commit per landed pull request. A merged pull
    # request's own commits, and an upstream merge's upstream commits, sit
    # behind second parents and carry no PR number of this repository.
    raw = sh(["git", "log", rng, "--first-parent", "--format=%h%x1f%s%x1f%an%x1f%ae"])
    commits = []
    for line in raw.splitlines():
        if not line.strip():
            continue
        sha, subject, author_name, author_email = line.split("\x1f")
        m = CONV_RE.match(subject)
        pr_m = PR_RE.search(subject)
        commits.append(
            {
                "sha": sha,
                "subject": subject,
                "type": (m.group("type") if m else None),
                "scope": (m.group("scope") if m else None),
                "breaking": bool(m and m.group("bang")),
                "pr": int(pr_m.group(1)) if pr_m else None,
                "author_name": author_name,
                "author_email": author_email,
            }
        )

    # Batch the gh lookups — one network call per unique PR, run concurrently.
    prs = sorted({c["pr"] for c in commits if c["pr"]})
    with ThreadPoolExecutor(max_workers=8) as pool:
        handles = dict(zip(prs, pool.map(pr_author, prs)))
    unresolved_prs = [pr for pr in prs if handles[pr] is None]

    # Attribute each commit: the PR author when gh resolved it; else fall back
    # to parsing the commit's GitHub noreply email (commit author — usually but
    # not always the PR author, e.g. after a maintainer rebase).
    for c in commits:
        handle = handles.get(c["pr"]) if c["pr"] else None
        source = "pr" if handle else None
        if handle is None:
            noreply = NOREPLY_RE.match(c["author_email"])
            if noreply:
                handle, source = noreply.group("handle"), "noreply_email"
        c["author_handle"] = handle
        c["author_handle_source"] = source

    # New contributors: authors in this range whose name AND email are both
    # unseen before prev_tag. Still approximate (a person may have committed
    # under a different name+email pair before) — treat it as a candidate list
    # to confirm against `git log <prev_tag> --format='%an <%ae>' | sort -u`.
    prior_names, prior_emails = set(), set()
    for line in sh(["git", "log", prev, "--format=%an%x1f%ae"]).splitlines():
        name, email = line.split("\x1f")
        prior_names.add(name)
        prior_emails.add(email)
    new_handles, seen = [], set()
    for c in commits:
        if c["author_name"] in prior_names or c["author_email"] in prior_emails:
            continue
        h = c["author_handle"]
        if h and h not in seen:
            seen.add(h)
            new_handles.append(h)

    json.dump(
        {
            "range": rng,
            "commits": commits,
            "new_contributors": new_handles,
            "unresolved_prs": unresolved_prs,
            "warnings": warnings,
        },
        sys.stdout,
        indent=2,
    )
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())

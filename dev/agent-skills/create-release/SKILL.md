---
name: create-release
description: >-
  Cut a new CocoIndex release end to end: propose the next version by semver
  when the user doesn't give one, draft the release notes from the commits
  since the last release, create the GitHub release (which creates the tag),
  and watch the Release workflow publish the wheels to PyPI and deploy the
  docs. Use whenever the user wants to release, cut, ship or publish a new
  CocoIndex version: "release 1.0.27", "cut a release", "make a new release",
  "publish a new version to PyPI", "draft the release", "what should the next
  version be". A minor is cut from main, a patch from the minor's release
  branch (release/1.1); the skill tells them apart.
---

# Create a CocoIndex Release

## How a release works here

Publishing a GitHub release whose tag doesn't exist yet creates the `v<version>`
tag on the release's target commit. That tag push starts
`.github/workflows/release.yml`, which:

1. stamps the version from the tag into `Cargo.toml`, `pyproject.toml` and
   `python/cocoindex/_version.py` (`.github/scripts/update_version.py`). The
   checked-in versions stay placeholders (`999.0.0`, `dynamic`), so a release
   needs no version-bump commit;
2. builds the wheels (abi3 and free-threaded, every platform) and the sdist,
   and smoke-tests them;
3. uploads them to PyPI, then deploys the docs when the release is the
   repository's "Latest" one (the `Docs Gate` job).

So publishing the GitHub release is the one action that ships everything.
Don't `git tag` and `git push` instead: that ships to PyPI with no release
entry and no notes. And publishing can't be taken back — PyPI never accepts the
same version twice — so the release is drafted first, shown to the user, and
published only on their go-ahead.

A minor or major release is cut from `main`. A patch release is cut from the
minor's release branch, `release/<X.Y>`, which starts at the minor's tag and
takes only backports (`dev/agent-skills/backport/SKILL.md` says what
qualifies and how one lands; `AGENTS.md`, Versioning, is the policy). The docs
site shows the docs of the latest release, so a patch of an older minor is
published with `--latest=false` (step 5) and deploys no docs.

Needs `gh` authenticated with push access to `cocoindex-io/cocoindex`. The
commands below use placeholders: `<branch>` (`main` or `release/1.1`),
`<prev_tag>` (e.g. `v1.0.26`), `<sha>` (full commit SHA), `<version>` (e.g.
`1.1.0`) and `<tag>` (`v<version>`).

## 1. Pin the branch, the commit and the previous release

```bash
git fetch origin --tags
git rev-parse origin/<branch>                              # <sha>
git describe --tags --abbrev=0 --match 'v[0-9]*' <sha>     # <prev_tag>: the newest release tag the commit descends from
git log --oneline <prev_tag>..<sha>
```

`<branch>` is `main` for a minor or major release and `release/<X.Y>` for a
patch of `X.Y` (`git ls-remote --heads origin 'release/*'` lists them; the
backport skill creates one). Release the branch's tip unless the user names
another commit, and use the SHA, not the branch name, from here on: the
branch can move while you work, and the tag must land on the commit the notes
describe. `git describe` finds the previous release on the commit's own line
of history; `gh release view` without a tag returns the release GitHub marks
"Latest", which is the wrong one once releases come from two branches. A
prerelease tag (`v1.2.0-rc.1`) can be what `git describe` finds; for a final
release use the last stable tag instead, so the notes cover everything since
it. If the range is empty, or holds nothing a user would notice (step 3), say
so and stop.

## 2. Check the commit is green

```bash
gh api "repos/cocoindex-io/cocoindex/commits/<sha>/check-runs?per_page=100" --paginate \
  --jq '.check_runs[] | [.name, .status, .conclusion] | @tsv'
```

`ci-gate` sums up build, tests and type checks; every `ci-gate` run on the
commit must be `success`. `skipped` jobs were path-filtered out, which is fine.
Checks still running: wait for them. Any `failure`, `cancelled` or `timed_out`:
stop and show the user — don't release a red commit on your own judgment. (The
combined-status endpoint reports `pending` forever because the repository uses
check runs, not statuses; ignore it.)

CI never runs the release build itself (manylinux containers, free-threaded
wheels, sdist, third-party notices). If the range touches it, or this is the
first release from a new release branch, offer a dry run before publishing:
`gh workflow run release.yml --ref <branch>` builds and tests everything and
publishes nothing (~15 minutes). Check with
`git diff --stat <prev_tag> <sha> -- .github/workflows/release.yml .github/scripts/update_version.py about.toml about.hbs`,
plus `[build-system]` or `[tool.maturin]` changes in `pyproject.toml`.

## 3. Draft the release notes

Listing commits is easy; `git log` does that. The hard part is **judgment**:
deciding what a *user* cares about, dropping the churn, and grouping the rest so
the note reads like a product update, not a diff.

### Match the previous release

A release note's value comes partly from being consistent with the ones before
it, so read the previous one first and mirror its section structure, wording
style and level of detail:

```bash
gh release view <prev_tag> --json body -q .body
```

CocoIndex publishes lightly edited versions of GitHub's **auto-generated
notes**, with no title or `Commit:` header:

- `## What's Changed`, then flat `* <description> by @handle in <full PR URL>`
  bullets. An item spanning several PRs reads `by @handle in <URL> and <URL>`
  (`in <URL>, <URL> and <URL>` for three).
- Related PRs nested under a plain theme bullet (`* Rust SDK fixes`, then
  indented `  * <description> by @handle in <URL>`).
- `## New Contributors`, with `* @handle made their first contribution in <URL>`
  lines; omit the section when there are none.
- A closing `**Full Changelog**: https://github.com/cocoindex-io/cocoindex/compare/<prev_tag>...<tag>`.
  The link goes live once the tag exists.

If the previous release changed shape, follow it rather than this description.

### Gather the raw material

```bash
python3 dev/agent-skills/create-release/scripts/collect_changes.py <prev_tag> <sha>
```

The collector does the mechanical work: it pairs every first-parent commit in
the range (one per landed PR) with its PR number, the PR author's GitHub handle,
and a first-pass new-contributor list, so you don't hand-loop `gh pr view`. It
prints JSON: `commits` (each with `sha`, `subject`, `type`, `scope`,
`breaking`, `pr`, `author_handle`, `author_handle_source`, `author_name`,
`author_email`), plus `new_contributors` (each a `handle` and the `pr` of their
earliest commit in the range), `unresolved_prs` and `warnings`. Read
`warnings` first. If `gh` can't reach the repository, fall back to
`git log --first-parent <prev_tag>..<sha> --oneline` and say that handles and
contributors are missing, rather than inventing them.

A backport lands on a release branch as a squash commit whose body keeps the
`(cherry picked from commit <sha>)` trailer of each commit it carried. The
collector expands such a commit into one entry per trailer, with the original
commit's subject, PR and author and `backport_pr` set to the backport pull
request: credit and link the original, as the entry does. It needs the
original commits in the clone (`git fetch origin main`); a warning names a
squash commit it couldn't expand.

A minor's range starts at the previous minor's tag, so it holds the originals
of fixes a patch release already shipped. List the originals that are on the
release branch (`git log origin/release/<X.Y> --format=%b | grep -o 'cherry picked from commit [0-9a-f]*'`),
leave those items out, and close "What's Changed" with one line that names
the patch releases: `This release also includes the fixes of v1.1.1.`

If `unresolved_prs` is non-empty, those PR numbers appear in commit subjects but
their `gh` lookup failed. When they also 404 through the REST API, the usual
cause is a deleted or spam-flagged author account, which hides their PRs; the
commits are still real. Recover the handle from the commit's noreply email (the
collector already tries this: `author_handle_source: "noreply_email"` marks it,
crediting the commit author rather than the PR author), or from an earlier
release note crediting the same git author name. Surface the dead PR link to the
user rather than silently shipping it, and offer a commit link
(`cocoindex-io/cocoindex@<short-sha>`) instead.

### Filter: keep what a user would notice

This is the core of the step. Read each commit through the eyes of someone who
*uses* CocoIndex, not someone who maintains it. Default to **excluding**:

- **Internal refactors** with no behavior change (`refactor(engine):`,
  `refactor(state_store):`, internal-only `refactor(sdk):` cleanups, dead-code
  removal).
- **Test-only** changes (`test(...)`).
- **CI / build / chore** plumbing (`ci:`, `build:`, `chore:`, lockfile bumps),
  including Dependabot `ci(deps):` bumps of examples, docs or CI dependencies.
  A dependency bump that fixes an advisory in what the wheel ships (Rust crates
  in the engine, Python requirements of the package) stays, grouped into one
  dependency-security item that names the advisories.
- **Example-only** changes (renames, dependency bumps, example READMEs), unless
  the example work is itself the user-visible proof of a feature (e.g. porting
  every example to a new SDK to show parity can headline).
- **Low-signal docs** (typos, link fixes, styling). Keep docs that document a
  real capability users would want to discover.

Default to **keeping**: `feat`, user-facing `fix`, new connector / source /
language support, performance work users would feel, deprecations (users must
act on them), and docs that surface a real feature.

The conventional-commit `type`/`scope` is a strong hint, not the rule; read the
subject and decide by user impact. When a commit is genuinely borderline (a
refactor that doubles as a reliability fix, an example rename that touches
paths users reference), surface it to the user as a judgment call rather than
silently dropping or keeping it.

### Group and word the survivors

- Lead with the biggest items. If several PRs form one coherent theme (a new
  subsystem, a connector family, a run of Rust SDK fixes), nest them under one
  theme bullet; it tells a story the flat list can't.
- Word each item by its **effect on the user**, not its implementation. Rewrite
  the commit subject when it reads like a diff, but don't claim a capability the
  PR doesn't deliver. Name what users must change: a deprecation and its
  replacement, a raised minimum version, a breaking change in so many words.
- Credit one author per PR (the PR author) with `by @handle in <URL>`.

### New contributors

Use the collector's `new_contributors` as a candidate list. It matches authors
by git name and email against history before `<prev_tag>`, which is
approximate in both directions: someone who previously committed under a
different name and email shows up as "new", and one person can appear under
several name spellings. Verify against
`git log <prev_tag> --format='%an <%ae>' | sort -u` before listing anyone.
Each entry's `pr` is the PR for its "made their first contribution in" link.

## 4. Pick the version

**The branch fixes the kind.** A patch (`X.Y.Z+1`) is released from
`release/<X.Y>`, and a minor or major from `main`; a patch from `main` would
ship everything landed since the minor, and a feature never goes to a release
branch. A version that doesn't match the branch, the user's or a proposed
one: say so and stop.

**The user gave one.** Accept `1.2.3` or `v1.2.3`; the tag is `v1.2.3`. Check
that:

- it is `X.Y.Z`, or a prerelease `X.Y.Z-alpha.N`, `X.Y.Z-beta.N` or
  `X.Y.Z-rc.N`, the forms `update_version.py` maps to PyPI's `aN`, `bN`, `rcN`;
- the tag is free: `git ls-remote --tags origin refs/tags/<tag>` prints nothing
  and `gh release view <tag>` fails (no draft holds it either);
- it is greater than `<prev_tag>` by semver precedence.

If their number disagrees with what step 3 found (a patch bump over new
features), say so once, then use theirs.

**Otherwise, propose one by semver**, decided from the items step 3 kept: the
version follows from what users get, not from commit prefixes. The version
labels the `cocoindex` Python package and the engine it ships; the Rust SDK
isn't launched yet and makes no compatibility promise, so its changes don't
drive the bump.

- **Major** (`X+1.0.0`): a change that breaks existing users of a stable public
  API with no compatible path, such as a public Python API, CLI command or
  connector option removed or renamed without a working deprecated alias, a
  new required argument, or a change that makes existing apps fail or need a
  manual migration. The collector's `breaking` flag (a `!` after the commit
  type) marks candidates; confirm each by reading the PR. A deprecation that
  keeps the old form working is not breaking.
- **Minor** (`X.Y+1.0`): any new backward-compatible capability: a new API,
  parameter, connector, source, language, CLI command or option, or a
  deprecation (it adds the replacement).
- **Patch** (`X.Y.Z+1`): only fixes, performance, docs and dependency bumps,
  from the minor's release branch.

For a prerelease, append `-rc.N` (or `-alpha.N` / `-beta.N`) to the proposed
version, with `N` one past the highest existing tag of that series
(`git tag -l 'v<version>-rc.*'`). Keep `<prev_tag>` at the latest stable release
either way, so a final release's notes cover everything since the last stable.

State the proposal in one line with its reason, e.g. "1.1.0 (minor): adds the
node and relationship graph target APIs (#2612) and `coco.LogicDep` (#2590);
nothing breaks a stable API."

## 5. Create the draft, confirm, publish

Write the notes to a file outside the repository, then create a draft:

```bash
gh release create <tag> --draft --target <sha> --title <tag> \
  --notes-file /tmp/cocoindex-<tag>-notes.md   # add --prerelease for a prerelease, --latest=false for a patch of an older minor
```

GitHub marks a published stable release "Latest" unless told otherwise, and
the Release workflow deploys the docs only from the Latest release. When the
version is lower than the highest published stable one, pass `--latest=false`
so the newer release stays Latest and the docs stay on it:

```bash
gh release list --exclude-drafts --exclude-pre-releases --json tagName --jq '.[].tagName' | sort -V | tail -1
```

A draft creates no tag, starts no workflow, and only maintainers can see it.
Then show the user, and wait for their answer:

- the version and its one-line reason, and the branch it comes from;
- the commit `<sha>` and that its CI is green;
- whether the release will be Latest, and so deploy the docs;
- the notes in a fenced markdown block (a longer outer fence if the notes
  contain fences), so they can be read and copied in place;
- the judgment calls: which commits you excluded and why, and the borderline
  ones, so the user can veto or restore anything quickly;
- the draft's URL, where GitHub renders the notes exactly as they'll appear.

Apply requested changes with `gh release edit <tag> --notes-file <file>`. If
the user edited the draft on GitHub instead, re-read it
(`gh release view <tag> --json body -q .body`) rather than overwriting their
edits. Publish only on an explicit go-ahead, or when the request itself said to
publish without a review:

```bash
gh release edit <tag> --draft=false
git fetch origin --tags && git rev-parse <tag>^{commit}   # must print <sha>
```

A stable release becomes "Latest" on its own unless it was created with
`--latest=false`; `gh release list` marks the Latest one once it is
published. If the user drops the release instead, delete the draft:
`gh release delete <tag> --yes`.

## 6. Watch the workflow and verify

```bash
gh run list -w release.yml --branch <tag> -L 1 --json databaseId,status,url   # appears seconds after publishing
gh run watch <run_id> --exit-status                                           # about 15 minutes
```

Watch in the background rather than blocking on it. If no run appears within a
minute, the tag push didn't trigger the workflow: show the user
`gh run list -w release.yml -L 3` and stop.

Once it's green, check that PyPI has the full set:

```bash
curl -fsS https://pypi.org/pypi/cocoindex/<pypi_version>/json \
  | python3 -c 'import json, sys; print(*(u["filename"] for u in json.load(sys.stdin)["urls"]), sep="\n")'
```

`<pypi_version>` is the version with a prerelease suffix mapped
(`1.1.0-rc.1` → `1.1.0rc1`). Expect one wheel per `build` matrix entry in
`release.yml` plus the `.tar.gz` sdist; PyPI's JSON API can lag the upload by a
minute. Report the release URL, `https://pypi.org/project/cocoindex/<pypi_version>/`,
the workflow run URL, and the result of its `Release Docs` job. That job is
skipped when the release isn't Latest (the `Docs Gate` job's log says which
release is); for a patch of an older minor that is the expected outcome, not a
failure.

## If the Release workflow fails

Read the failure first: `gh run view <run_id> --log-failed`.

- **A build or test job** (`generate-3p-notices`, `build`, `test-abi3`,
  `test-free-threaded`, `sdist`): nothing reached PyPI. A flake (runner,
  network): `gh run rerun <run_id> --failed`. A real failure needs a fix on
  the release's branch first (on `main`, then backported, for a release
  branch); then ask the user whether to delete the release and its tag
  (`gh release delete <tag> --cleanup-tag --yes`) and re-cut the same version
  on the fixed commit, or keep it and cut the next patch. Deleting a published
  release is visible to everyone, so it is their call.
- **The `Release` job** (PyPI upload): rerun the failed jobs. The upload passes
  `--skip-existing`, so files already on PyPI are skipped. Once any file is on
  PyPI the version is spent; never delete and re-cut it.
- **`Release Docs`**: rerun the failed job. The `docs_release.yml` workflow also
  deploys the docs on demand, from the branch given as its ref: the latest
  release's branch, never `main` while `main` describes unreleased work.

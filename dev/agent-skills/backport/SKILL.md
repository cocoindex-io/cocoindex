---
name: backport
description: >-
  Backport a merged CocoIndex pull request from main to a release branch
  (release/X.Y) so that it ships in the next patch release: decide whether it
  qualifies, create the branch from the minor's tag when it doesn't exist yet,
  cherry-pick with -x in a scratch worktree, resolve conflicts, and land it
  through a pull request to the release branch. Use whenever the user wants a
  landed change in a patch release: "backport #2673", "backport this to 1.1",
  "cherry-pick #2660 onto the release branch", "get this fix into 1.1.1",
  "create the release branch for 1.1".
---

# Backport a pull request to a release branch

## How release branches work here

A minor release (`1.2.0`) is cut from `main`. A patch release (`1.1.1`) is cut
from the minor's release branch, `release/1.1`, which starts at the minor's tag
(`v1.1.0`) and moves only by backports: squash-merged pull requests that
cherry-pick commits already on `main`. The branch exists once its first
backport needs it, and only the newest minor's branch takes backports; an
older one stays as it is (its ruleset forbids deleting it).

A release branch has no merge queue. A pull request to it runs the full CI
tier on its own, and `ci-gate` must pass before it can be squash-merged.
`AGENTS.md` (Versioning) is the policy; this playbook is the procedure.

**What qualifies.** A change goes to a release branch when a user of the
released minor needs it and it adds no way to call CocoIndex:

- a **fix**: released behavior that didn't match its documentation or intent
  (wrong results, lost or stale data, a crash, a hang, a security problem);
- a **dependency bump** that fixes an advisory in what the wheel ships (Rust
  crates of the engine, Python requirements of the package), not the
  development lockfile;
- a **docs change** that describes released behavior or improves the site
  (design, navigation, examples). Not docs of a feature that isn't released.

Never a new API, parameter, connector, source, CLI option or configuration
key, experimental or not; not a refactor, an example, or CI or dev tooling,
unless the branch's own CI needs it. A fix that is entangled with a feature
is backported as the fix alone, adapted to the branch (step 3), not with the
feature's commits for context.

A fix lands on `main` first. When it doesn't apply to `main` because the code
moved on, say so in the backport's pull request.

Needs `gh` authenticated with push access to `cocoindex-io/cocoindex`.
Placeholders: `<pr>` (the merged pull request's number), `<sha>` (its merge
commit on `main`), `<X.Y>` (the minor, e.g. `1.1`).

## 1. Pick the pull request and the branch

```bash
git fetch origin --tags
gh pr view <pr> --json state,mergeCommit,title,author,files \
  --jq '"\(.state) \(.mergeCommit.oid) \(.title) by \(.author.login)\n\([.files[].path] | join("\n"))"'
```

The state must be `MERGED`, and the merge commit is what you cherry-pick: the
pull request's own commits were squashed. Check the change against "What
qualifies" above; a borderline case (a refactor that doubles as a fix, a fix
that adds a parameter) goes to the user before any work.

The branch is the newest minor's, `release/<X.Y>`, unless the user names
another one:

```bash
git tag -l 'v[0-9]*' | sort -V | tail -1             # the newest release tag
git ls-remote --heads origin 'release/*'             # the branches that exist
```

**No branch yet?** Create it from the minor's tag, and nothing else:

```bash
git push origin v<X.Y>.0:refs/heads/release/<X.Y>
```

The `release-branches` ruleset then applies: no deletion, no force push, pull
requests only, `ci-gate` required.

## 2. Cherry-pick in a scratch worktree

Work in a worktree so the user's checkout stays untouched:

```bash
git worktree add /tmp/backport-<pr> -b backport/<X.Y>/<pr> origin/release/<X.Y>
cd /tmp/backport-<pr>
git cherry-pick -x <sha>
```

`-x` appends `(cherry picked from commit <sha>)` to the message. Keep it: the
repository squashes with the commit messages as the body (its
`squash_merge_commit_message` setting), so the trailer reaches the branch,
and the release-notes collector reads it to credit the original pull request
and its author.

**One pull request per original pull request** is the default; it keeps one
squash commit per change on the branch. A coherent series (a docs redesign
that landed as fifteen pull requests) may go in one pull request:
cherry-pick its merge commits in `main`'s order (`git log --first-parent
--reverse` over the range), and the squash commit keeps every trailer.

## 3. Resolve conflicts, adapt to the branch

A conflict usually means `main` has a refactor the branch lacks. Resolve it
the branch's way: keep the branch's structure and port what the fix does
into it. Two cases this repository has seen:

- `main` replaced explicit `if ctx.preview()` guards at store-write sites
  with a discarding store handle. A fix that adds a write site on `main`
  needs the explicit guard on a branch without that refactor.
- A fix changed a function's return type and a refactor moved the call site.
  Take the fix's type and the branch's call site.

Don't cherry-pick the refactor to make the fix apply cleanly. Describe each
adaptation in the pull request body (step 5), so the reviewer and the release
notes know the branch's version differs from `main`'s. When the resolution is
not obvious, stop and show the user the conflict.

A docs cherry-pick that brings text about an unreleased feature along (a
page that also gained a section for it) is trimmed to the released behavior.

## 4. Check locally

Run the hooks the change needs, as for any pull request:

```bash
prek run --from-ref origin/release/<X.Y> --to-ref HEAD
```

For a code change also run the tests of the touched area, with the branch's
own build (`uv run maturin develop`, then `cargo test` or
`uv run pytest python/tests/<area>`): the branch may build differently from
`main`, and a cherry-pick that compiles can still fail a test the fix's own
run passed. A regression test that came with the fix must fail without it on
the branch too; if it passes without the fix, the branch doesn't have the bug
and the backport isn't needed.

## 5. Open the pull request and land it

Title: the original subject, its `(#<pr>)` included, so the branch's history
reads like `main`'s. Body, written to a file outside the repository:

```markdown
Backport of #<pr> to release/<X.Y>.

<the original pull request's summary, or a one-line description>

Adapted for the branch: <each adaptation from step 3, or "none; clean cherry-pick">
```

```bash
gh pr create --base release/<X.Y> --draft --title "<original subject> (#<pr>)" \
  --body-file /tmp/backport-<pr>-body.md
gh pr ready <backport-pr>
gh pr checks <backport-pr> --watch --fail-fast --interval 30
```

Open it as a draft and mark it ready once the local checks pass (drafts run
no CI). A release branch has no merge queue, so the pull request's own run is
the full tier, and it takes as long as a merge-group run does. When `ci-gate`
is green:

```bash
gh pr merge <backport-pr> --squash --delete-branch
git worktree remove /tmp/backport-<pr>
```

Don't use `--auto`: it waits only for the branch's required checks. If CI
fails on something the backport didn't touch, the branch itself is broken:
fix that in its own backport first (a fix that is already on `main`), or
report it to the user.

## 6. Then

The patch release is `dev/agent-skills/create-release/SKILL.md`, run on the
release branch. A docs-only backport that can't wait for it deploys through
`gh workflow run docs_release.yml --ref release/<X.Y>`; never from `main`,
whose docs describe unreleased work.

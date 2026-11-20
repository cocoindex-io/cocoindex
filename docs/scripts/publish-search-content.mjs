// Publishes the docs pages that site search indexes: runs the export of
// export-search-content.mjs on a docs build and pushes its three folders
// (python/, rust/, examples/) to the branch `search-content` of a repository.
//
//   node scripts/publish-search-content.mjs <build-dir> <repo-url> [<commit-message>]
//
// The branch is shared: the website publishes its own pages to another folder
// of it. So a publish starts from the branch's tip, replaces only the export's
// three folders, and adds one commit on top. Nothing is ever force-pushed.
//
//   - A branch that doesn't exist yet is created with the three folders.
//   - An export that changes nothing makes no commit.
//   - When someone else pushed between the fetch and the push, the publish
//     starts again from the new tip. The export replaces its folders as a
//     whole, so it needs no merge.
//   - Any other failure is an error: a failed export pushes nothing, and a push
//     that fails while the branch stands still is not tried again.
//
// The commit takes its author from git's own settings (GIT_AUTHOR_NAME and the
// like, or the git config).
import { execFileSync } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { exportSearchContent } from './export-search-content.mjs';

const BRANCH = 'search-content';
const REF = `refs/heads/${BRANCH}`;
const ATTEMPTS = 5;

const git = (cwd, ...args) =>
  execFileSync('git', args, { cwd, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim();

// The commit the branch points at in the repository, or null when it has no such branch.
const remoteTip = (cwd) => git(cwd, 'ls-remote', 'origin', REF).split(/\s/)[0] || null;

// One try, in a checkout of its own: export onto the branch's tip, commit, push.
// Returns { moved: true } when the branch moved before the push.
function attempt(build, repo, message, beforePush) {
  const work = mkdtempSync(join(tmpdir(), 'search-content-'));
  try {
    git(work, 'init', '--quiet', `--initial-branch=${BRANCH}`);
    git(work, 'remote', 'add', 'origin', repo);
    let tip = null;
    if (remoteTip(work)) {
      git(work, 'fetch', '--quiet', '--depth', '1', 'origin', REF);
      git(work, 'reset', '--quiet', '--hard', 'FETCH_HEAD');
      tip = git(work, 'rev-parse', 'HEAD');
    }

    const count = exportSearchContent(build, work);
    // The checkout is the branch's tip plus the export, so every change is in
    // the export's folders.
    git(work, 'add', '--all');
    if (!git(work, 'status', '--porcelain')) return { count, commit: null };
    git(work, 'commit', '--quiet', '-m', message);
    const commit = git(work, 'rev-parse', 'HEAD');

    beforePush?.();
    try {
      git(work, 'push', '--quiet', 'origin', `HEAD:${REF}`);
    } catch (error) {
      if (remoteTip(work) === tip) throw error;
      return { moved: true };
    }
    return { count, commit };
  } finally {
    rmSync(work, { recursive: true, force: true });
  }
}

// Returns the export's counts and the pushed commit (null when nothing changed).
// `beforePush` runs between the commit and the push; the tests use it to push
// from elsewhere at that moment.
export function publishSearchContent(buildDir, repo, { message = 'update the docs pages', beforePush } = {}) {
  const build = resolve(buildDir);
  for (let n = 1; n <= ATTEMPTS; n++) {
    const result = attempt(build, repo, message, beforePush);
    if (!result.moved) return result;
  }
  throw new Error(`${BRANCH} moved before each of ${ATTEMPTS} pushes`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const [buildDir, repo, message] = process.argv.slice(2);
  if (!buildDir || !repo) {
    console.error('usage: node scripts/publish-search-content.mjs <build-dir> <repo-url> [<commit-message>]');
    process.exit(2);
  }
  try {
    const { count, commit } = publishSearchContent(buildDir, repo, { message });
    const total = Object.values(count).reduce((a, b) => a + b, 0);
    const parts = Object.entries(count).map(([tree, n]) => `${tree} ${n}`).join(', ');
    const outcome = commit ? `pushed ${commit.slice(0, 7)} to ${BRANCH}` : `${BRANCH} is up to date, nothing pushed`;
    console.log(`publish-search-content: ${total} files (${parts}); ${outcome}`);
  } catch (error) {
    console.error(`publish-search-content: ${error.message}`);
    process.exit(1);
  }
}

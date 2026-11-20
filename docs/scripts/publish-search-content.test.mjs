import assert from 'node:assert/strict';
import { execFileSync, spawnSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { test } from 'node:test';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { publishSearchContent } from './publish-search-content.mjs';

// Every repository here is a folder in the temp directory. Git reads no
// settings of the machine, and the commits get a fixed author.
Object.assign(process.env, {
  GIT_CONFIG_GLOBAL: '/dev/null',
  GIT_CONFIG_NOSYSTEM: '1',
  GIT_AUTHOR_NAME: 'Test',
  GIT_AUTHOR_EMAIL: 'test@example.com',
  GIT_COMMITTER_NAME: 'Test',
  GIT_COMMITTER_EMAIL: 'test@example.com',
});

const SCRIPT = fileURLToPath(new URL('./publish-search-content.mjs', import.meta.url));
const SITE = 'https://example.com/docs';
const REF = 'refs/heads/search-content';
const twin = (title, body) => `# ${title}\n\n> Source: ${SITE}/page/\n\n${body}\n`;

const temp = (name) => mkdtempSync(join(tmpdir(), `publish-search-content-${name}-`));
const git = (cwd, ...args) => execFileSync('git', args, { cwd, encoding: 'utf8' }).trim();
const write = (root, files) => {
  for (const [path, text] of Object.entries(files)) {
    mkdirSync(dirname(join(root, path)), { recursive: true });
    writeFileSync(join(root, path), text);
  }
};

// A build with one page in each tree.
const build = (extra = {}) => {
  const dir = temp('build');
  write(dir, {
    'llms.txt': `- [Guide](${SITE}/guide/): A page.\n- [PDF](${SITE}/examples/pdf/): An example.\n`,
    'rust/llms.txt': `- [Guide](${SITE}/rust/guide/): A page.\n`,
    'guide.md': twin('Guide', 'The Python page.'),
    'rust/guide.md': twin('Guide', 'The Rust page.'),
    'examples/pdf.md': twin('PDF', 'The walkthrough.'),
    ...extra,
  });
  return dir;
};
const EXPORTED = ['examples/pdf.md', 'python/guide.md', 'rust/guide.md'];

// The repository the publish pushes to: its folder, and the URL to reach it.
const remote = () => {
  const dir = temp('remote');
  git(dir, 'init', '--quiet', '--bare', '--initial-branch=main');
  return { dir, url: pathToFileURL(dir).href };
};
// What the website's step does: one commit with `files` on the branch's tip.
const pushFromElsewhere = ({ url }, files, message) => {
  const work = temp('elsewhere');
  git(work, 'init', '--quiet', '--initial-branch=search-content');
  if (git(work, 'ls-remote', url, REF)) {
    git(work, 'fetch', '--quiet', url, REF);
    git(work, 'reset', '--quiet', '--hard', 'FETCH_HEAD');
  }
  write(work, files);
  git(work, 'add', '--all');
  git(work, 'commit', '--quiet', '-m', message);
  git(work, 'push', '--quiet', url, `HEAD:${REF}`);
};
// The branch's commit, or '' when the repository has no such branch.
const tip = ({ dir }) => git(dir, 'for-each-ref', '--format=%(objectname)', REF);
const subjects = ({ dir }) => git(dir, 'log', '--format=%s', REF).split('\n');
const files = ({ dir }) => git(dir, 'ls-tree', '-r', '--name-only', REF).split('\n').sort();
const show = ({ dir }, path) => git(dir, 'show', `${REF}:${path}`);

test('a branch that does not exist is created with only the three folders', () => {
  const repo = remote();
  const { count, commit } = publishSearchContent(build(), repo.url, { message: 'docs pages from abc1234' });
  assert.deepEqual(count, { python: 1, rust: 1, examples: 1 });
  assert.equal(commit, tip(repo));
  assert.deepEqual(files(repo), EXPORTED);
  assert.deepEqual(subjects(repo), ['docs pages from abc1234']);
  assert.equal(show(repo, 'python/guide.md'), '# Guide\n\nThe Python page.');
  assert.equal(git(repo.dir, 'for-each-ref', '--format=%(refname)'), REF);
});

test('the folders of the website stay as they are, and a page that left the build goes', () => {
  const repo = remote();
  pushFromElsewhere(repo, { 'site/blogs/post.md': 'a post', 'python/old.md': 'stale' }, 'the website');
  const before = tip(repo);
  const { commit } = publishSearchContent(build(), repo.url);
  assert.deepEqual(files(repo), [...EXPORTED, 'site/blogs/post.md']);
  assert.equal(show(repo, 'site/blogs/post.md'), 'a post');
  assert.equal(git(repo.dir, 'rev-parse', `${commit}^`), before);
});

test('when the branch moves between the fetch and the push, the publish starts again from the new tip', () => {
  const repo = remote();
  pushFromElsewhere(repo, { 'site/blogs/post.md': 'a post' }, 'the website');
  let pushes = 0;
  const beforePush = () => {
    if (pushes++ === 0) pushFromElsewhere(repo, { 'site/answers/a.md': 'an answer' }, 'the website again');
  };
  const { commit } = publishSearchContent(build(), repo.url, { message: 'the docs', beforePush });
  assert.equal(pushes, 2);
  assert.equal(commit, tip(repo));
  assert.deepEqual(files(repo), [...EXPORTED, 'site/answers/a.md', 'site/blogs/post.md']);
  assert.deepEqual(subjects(repo), ['the docs', 'the website again', 'the website']);
});

test('when someone else creates the branch before the first push, the publish starts again on it', () => {
  const repo = remote();
  let pushes = 0;
  const beforePush = () => {
    if (pushes++ === 0) pushFromElsewhere(repo, { 'site/blogs/post.md': 'a post' }, 'the website');
  };
  publishSearchContent(build(), repo.url, { message: 'the docs', beforePush });
  assert.deepEqual(files(repo), [...EXPORTED, 'site/blogs/post.md']);
  assert.deepEqual(subjects(repo), ['the docs', 'the website']);
});

test('a second publish of the same build makes no commit', () => {
  const repo = remote();
  const dir = build();
  const first = publishSearchContent(dir, repo.url);
  const second = publishSearchContent(dir, repo.url);
  assert.equal(second.commit, null);
  assert.deepEqual(second.count, first.count);
  assert.equal(tip(repo), first.commit);
});

test('a failed export pushes nothing', () => {
  const repo = remote();
  pushFromElsewhere(repo, { 'python/old.md': 'earlier export' }, 'earlier');
  const before = tip(repo);
  assert.throws(() => publishSearchContent(build({ 'stray.md': '# Stray\n' }), repo.url), /no rule covers stray\.md/);
  assert.equal(tip(repo), before);
});

test('a push that fails while the branch stands still is an error, and is not tried again', () => {
  const repo = remote();
  writeFileSync(join(repo.dir, 'hooks/pre-receive'), '#!/bin/sh\necho "pushes are closed" >&2\nexit 1\n', {
    mode: 0o755,
  });
  let pushes = 0;
  assert.throws(
    () => publishSearchContent(build(), repo.url, { beforePush: () => pushes++ }),
    /pushes are closed/,
  );
  assert.equal(pushes, 1);
  assert.equal(tip(repo), '');
});

test('a branch that moves before every push ends the publish with an error', () => {
  const repo = remote();
  let pushes = 0;
  const beforePush = () => pushFromElsewhere(repo, { [`site/${pushes++}.md`]: 'a page' }, 'the website');
  assert.throws(() => publishSearchContent(build(), repo.url, { beforePush }), /search-content moved before each of 5 pushes/);
  assert.equal(pushes, 5);
  assert.deepEqual(files(repo), ['site/0.md', 'site/1.md', 'site/2.md', 'site/3.md', 'site/4.md']);
});

test('the command prints the counts and the commit, and exits 1 on a failure', () => {
  const repo = remote();
  const dir = build();
  const run = (...args) => spawnSync('node', [SCRIPT, ...args], { encoding: 'utf8' });

  const first = run(dir, repo.url, 'docs pages from abc1234');
  assert.equal(first.status, 0, first.stderr);
  assert.match(first.stdout, new RegExp(`3 files \\(python 1, rust 1, examples 1\\); pushed ${tip(repo).slice(0, 7)} to search-content`));
  assert.deepEqual(subjects(repo), ['docs pages from abc1234']);

  const again = run(dir, repo.url);
  assert.equal(again.status, 0, again.stderr);
  assert.match(again.stdout, /search-content is up to date, nothing pushed/);

  const bad = run(build({ 'stray.md': '# Stray\n' }), repo.url);
  assert.equal(bad.status, 1);
  assert.match(bad.stderr, /no rule covers stray\.md/);
  assert.equal(run(dir).status, 2);
});

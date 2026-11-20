import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, relative } from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';
import { exportSearchContent } from './export-search-content.mjs';

const SCRIPT = fileURLToPath(new URL('./export-search-content.mjs', import.meta.url));
const SITE = 'https://example.com/docs';
const BANNER =
  '> **A notice for coding agents.** It opens every page.\n' +
  '>\n' +
  `> Source: ${SITE}/page/ · Docs index: ${SITE}/llms.txt\n` +
  '>\n' +
  '> A map from the old names to the new ones.\n';
const twin = (title, body) => `# ${title}\n\n${BANNER}\n${body}\n`;

const write = (root, files) => {
  for (const [path, text] of Object.entries(files)) {
    mkdirSync(dirname(join(root, path)), { recursive: true });
    writeFileSync(join(root, path), text);
  }
};
const tree = (root, dir = root) =>
  readdirSync(dir)
    .flatMap((name) => {
      const path = join(dir, name);
      return statSync(path).isDirectory() ? tree(root, path) : [relative(root, path)];
    })
    .sort();
const snapshot = (root) => Object.fromEntries(tree(root).map((f) => [f, readFileSync(join(root, f), 'utf8')]));

// A build with pages in all three trees, and the files for coding agents.
const build = (extra = {}) => {
  const dir = mkdtempSync(join(tmpdir(), 'search-content-build-'));
  write(dir, {
    'llms.txt':
      `# Docs\n\n> Full text: ${SITE}/llms-full.txt.\n\n` +
      '[The project](https://project.example.org/source/)\n\n## Guide\n' +
      `- [Guide](${SITE}/guide/): The section overview.\n` +
      `- [App](${SITE}/guide/app/): The App page.\n` +
      `## Examples\n- [Sample](${SITE}/examples/sample/): An example.\n`,
    'rust/llms.txt': `# Rust docs\n\n- [App](${SITE}/rust/guide/app/): The App page.\n`,
    'guide.md': twin('Guide', 'The overview.'),
    'guide/app.md': twin('The App', 'An **App** is the runnable unit.\n\n> A quote of the page.'),
    'rust/guide/app.md': twin('The App', 'The Rust page.'),
    'examples/sample.md': twin('Sample', 'The walkthrough.'),
    'skill.md': '# Skill\n',
    'references/notes.md': '# Notes\n',
    'guide/app/index.html': '<html></html>',
    ...extra,
  });
  return dir;
};
const outDir = () => mkdtempSync(join(tmpdir(), 'search-content-out-'));

test('each listed page lands in its tree, and the agent files are not copied', () => {
  const out = outDir();
  assert.deepEqual(exportSearchContent(build(), out), { python: 2, rust: 1, examples: 1 });
  assert.deepEqual(tree(out), [
    'examples/sample.md',
    'python/guide.md',
    'python/guide/app.md',
    'rust/guide/app.md',
  ]);
});

test('the banner is removed, and the title and the text of the page stay', () => {
  const out = outDir();
  exportSearchContent(build(), out);
  assert.equal(
    readFileSync(join(out, 'python/guide/app.md'), 'utf8'),
    '# The App\n\nAn **App** is the runnable unit.\n\n> A quote of the page.\n',
  );
});

test('a .md file that no rule covers fails the export with its name', () => {
  const out = outDir();
  assert.throws(
    () => exportSearchContent(build({ 'notes/draft.md': '# Draft\n' }), out),
    /no rule covers notes\/draft\.md/,
  );
  assert.deepEqual(tree(out), []);
});

test('a listed page without a twin fails the export with its name', () => {
  const list = `- [Gone](${SITE}/guide/gone/): A page.\n`;
  assert.throws(
    () => exportSearchContent(build({ 'rust/llms.txt': list }), outDir()),
    /a listed page has no \.md twin: guide\/gone\.md/,
  );
});

test('a twin without the banner fails the export, also when it opens with its own quote', () => {
  for (const text of ['# Plain\n\nNo banner here.\n', '# Quote\n\n> The page opens with a quote.\n\nText.\n']) {
    assert.throws(
      () => exportSearchContent(build({ 'guide/app.md': text }), outDir()),
      /no banner under the title of guide\/app\.md/,
    );
  }
});

test('a missing page list fails the export', () => {
  const dir = build();
  rmSync(join(dir, 'rust/llms.txt'));
  assert.throws(() => exportSearchContent(dir, outDir()), /missing page list rust\/llms\.txt/);
});

test('only the three folders are replaced: a page that left the lists goes, other folders stay', () => {
  const out = outDir();
  write(out, { 'python/old/page.md': 'stale', 'site/blogs/post.md': 'kept', 'README.md': 'kept' });
  exportSearchContent(build(), out);
  assert.equal(existsSync(join(out, 'python/old/page.md')), false);
  assert.equal(readFileSync(join(out, 'site/blogs/post.md'), 'utf8'), 'kept');
  assert.equal(readFileSync(join(out, 'README.md'), 'utf8'), 'kept');
});

test('a failed export leaves the output folder as it was', () => {
  const out = outDir();
  write(out, { 'python/old/page.md': 'earlier export' });
  assert.throws(() => exportSearchContent(build({ 'stray.md': '# Stray\n' }), out));
  assert.deepEqual(snapshot(out), { 'python/old/page.md': 'earlier export' });
});

test('a second run on the same build writes the same files', () => {
  const dir = build();
  const out = outDir();
  exportSearchContent(dir, out);
  const first = snapshot(out);
  exportSearchContent(dir, out);
  assert.deepEqual(snapshot(out), first);
});

test('the output folder may not be the build folder or inside it', () => {
  const dir = build();
  assert.throws(() => exportSearchContent(dir, dir), /must not contain each other/);
  assert.throws(() => exportSearchContent(dir, join(dir, 'rust')), /must not contain each other/);
  assert.equal(existsSync(join(dir, 'rust/guide/app.md')), true);
});

test('the command prints the counts, and exits 1 naming the file on a failure', () => {
  const ok = spawnSync('node', [SCRIPT, build(), outDir()], { encoding: 'utf8' });
  assert.equal(ok.status, 0);
  assert.match(ok.stdout, /4 files \(python 2, rust 1, examples 1\)/);
  const bad = spawnSync('node', [SCRIPT, build({ 'stray.md': '# Stray\n' }), outDir()], { encoding: 'utf8' });
  assert.equal(bad.status, 1);
  assert.match(bad.stderr, /no rule covers stray\.md/);
  assert.equal(spawnSync('node', [SCRIPT], { encoding: 'utf8' }).status, 2);
});

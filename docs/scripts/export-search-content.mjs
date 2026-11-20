// Exports the docs pages that site search indexes: the Markdown twin of every
// page in the published page lists (llms.txt and rust/llms.txt), with the
// banner for coding agents removed. The same banner on every page would match
// a query equally on all of them.
//
//   node scripts/export-search-content.mjs <build-dir> <out-dir>
//
// <build-dir> is a docs build (docs/dist, or a checkout of the deployed site).
// <out-dir> receives three folders, and nothing else in it is touched:
//
//   python/<route>.md    the Python page   /docs/<route>/
//   rust/<route>.md      the Rust page     /docs/rust/<route>/
//   examples/<slug>.md   an example page   /docs/examples/<slug>/
//
// The three folders are replaced as a whole, so a page that left the page
// lists also leaves the export. Every .md file in the build must be either a
// listed page's twin or one of the files for coding agents (SKIPPED). Any
// other .md file fails the export with its name: a new kind of file gets a
// decision, not a silent default. A failed export changes nothing in <out-dir>.
// The docs test workflow runs the export on every build, so a change that
// breaks one of these rules fails its own pull request.
import { existsSync, mkdirSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { pathToFileURL } from 'node:url';

const BASE = '/docs/';
const PAGE_LISTS = ['llms.txt', 'rust/llms.txt'];
const TREES = ['python', 'rust', 'examples'];
// The .md files the build writes for coding agents. They are not pages.
const SKIPPED = [/^skill\.md$/, /^references\/[^/]+\.md$/];
// A Markdown link to a page: the URL ends with a slash.
const PAGE_LINK = /\]\((https?:\/\/[^)\s]+\/)\)/g;
// The banner is the blockquote right under the title. It always carries a
// `Source:` line, which tells it apart from a quote the page itself opens with.
const BANNER = /^(# [^\n]*\n)\n((?:>[^\n]*\n)+)\n?/;

const walkMarkdown = (root, dir = root) =>
  readdirSync(dir).flatMap((name) => {
    if (name.startsWith('.')) return [];
    const path = join(dir, name);
    if (statSync(path).isDirectory()) return walkMarkdown(root, path);
    return name.endsWith('.md') ? [relative(root, path).split(sep).join('/')] : [];
  });

// Where a listed page's twin is in the build, and where it goes in the export.
function twinOf(url) {
  const { pathname } = new URL(url);
  if (!pathname.startsWith(BASE)) return undefined;
  const route = pathname.slice(BASE.length).replace(/\/$/, '');
  if (!route) return undefined;
  const source = `${route}.md`;
  const [tree] = route.split('/');
  return { source, target: tree === 'rust' || tree === 'examples' ? source : `python/${source}` };
}

const inside = (parent, child) => {
  const rel = relative(parent, child);
  return rel === '' || (!rel.startsWith('..') && !isAbsolute(rel));
};

export function exportSearchContent(buildDir, outDir) {
  const build = resolve(buildDir);
  const out = resolve(outDir);
  if (inside(build, out) || inside(out, build))
    throw new Error(`the output folder and the build folder must not contain each other: ${out}, ${build}`);

  const problems = [];
  const twins = new Map(); // source path in the build → path in the export
  for (const list of PAGE_LISTS) {
    const path = join(build, list);
    if (!existsSync(path)) {
      problems.push(`missing page list ${list}`);
      continue;
    }
    for (const [, url] of readFileSync(path, 'utf8').matchAll(PAGE_LINK)) {
      // A link that leaves the docs is not a page, so it has no twin.
      const twin = twinOf(url);
      if (twin) twins.set(twin.source, twin.target);
    }
  }

  const files = new Map(); // path in the export → content
  for (const [source, target] of twins) {
    const path = join(build, source);
    if (!existsSync(path)) {
      problems.push(`a listed page has no .md twin: ${source}`);
      continue;
    }
    const text = readFileSync(path, 'utf8').replace(/\r\n/g, '\n');
    const banner = BANNER.exec(text);
    if (!banner || !/^> Source: /m.test(banner[2])) {
      problems.push(`no banner under the title of ${source}`);
      continue;
    }
    files.set(target, `${banner[1]}\n${text.slice(banner[0].length)}`);
  }

  for (const file of walkMarkdown(build)) {
    if (!twins.has(file) && !SKIPPED.some((skip) => skip.test(file)))
      problems.push(`no rule covers ${file}: list its page, or add it to SKIPPED`);
  }

  if (problems.length)
    throw new Error(`${problems.length} problem(s)\n${problems.map((p) => `  - ${p}`).join('\n')}`);

  for (const tree of TREES) rmSync(join(out, tree), { recursive: true, force: true });
  for (const [target, text] of files) {
    const path = join(out, target);
    mkdirSync(dirname(path), { recursive: true });
    writeFileSync(path, text);
  }
  return Object.fromEntries(
    TREES.map((tree) => [tree, [...files.keys()].filter((f) => f.startsWith(`${tree}/`)).length]),
  );
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const [buildDir, outDir] = process.argv.slice(2);
  if (!buildDir || !outDir) {
    console.error('usage: node scripts/export-search-content.mjs <build-dir> <out-dir>');
    process.exit(2);
  }
  try {
    const count = exportSearchContent(buildDir, outDir);
    const total = Object.values(count).reduce((a, b) => a + b, 0);
    const parts = Object.entries(count).map(([tree, n]) => `${tree} ${n}`).join(', ');
    console.log(`export-search-content: ${total} files (${parts}) → ${outDir}`);
  } catch (error) {
    console.error(`export-search-content: ${error.message}`);
    process.exit(1);
  }
}

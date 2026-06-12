// Post-build checks for the agent-facing artifacts in dist/. Catches the
// silent-corruption class no human reads: MDX scaffolding leaking into .md
// output, fenced code rewritten by the converter, and HTML pages missing
// their advertised .md twins. Run after `astro build` (see package.json).
import { readFileSync, readdirSync, existsSync, statSync } from 'node:fs';
import { join, relative } from 'node:path';

const DIST = new URL('../dist/', import.meta.url).pathname;
const DOCS_SRC = new URL('../src/content/docs/', import.meta.url).pathname;
const POSTS_SRC = new URL('../src/content/example-posts/', import.meta.url).pathname;
// Same FENCE regex as src/lib/raw-markdown.ts — keep in sync.
const FENCE = /(^|\n)[ \t]*(`{3,}|~{3,})[^\n]*\n[\s\S]*?\n[ \t]*\2[`~]*[ \t]*(?=\n|$)/g;
const errors = [];

const walk = (dir, ext) =>
  readdirSync(dir).flatMap((f) => {
    const p = join(dir, f);
    return statSync(p).isDirectory() ? walk(p, ext) : p.endsWith(ext) ? [p] : [];
  });
const contentFiles = (dir) => walk(dir, '.mdx').concat(walk(dir, '.md'));

// 1. Required artifacts exist.
for (const f of ['llms.txt', 'llms-full.txt', 'skill.md']) {
  if (!existsSync(join(DIST, f))) errors.push(`missing dist/${f}`);
}
if (!existsSync(join(DIST, 'references')) || walk(join(DIST, 'references'), '.md').length === 0) {
  errors.push('missing dist/references/*.md (skill companions)');
}

// 2. Every content page has its .md twin: docs pages and example walkthroughs.
const docSlug = (src) =>
  relative(DOCS_SRC, src).replace(/\.mdx?$/, '').replace(/\/index$/, '');
const postSlug = (src) => relative(POSTS_SRC, src).replace(/\.mdx?$/, '');
for (const src of contentFiles(DOCS_SRC)) {
  if (!existsSync(join(DIST, `${docSlug(src)}.md`)))
    errors.push(`missing .md twin for docs page: ${docSlug(src)}`);
}
for (const src of contentFiles(POSTS_SRC)) {
  if (!existsSync(join(DIST, 'examples', `${postSlug(src)}.md`)))
    errors.push(`missing .md twin for example walkthrough: examples/${postSlug(src)}`);
}

// 3. No sentinel bytes anywhere; no MDX scaffolding outside fenced code
//    (inside fences it may be a legitimate sample of MDX syntax).
const agentFiles = [join(DIST, 'llms-full.txt'), join(DIST, 'skill.md'), ...walk(DIST, '.md')];
for (const f of agentFiles) {
  const text = readFileSync(f, 'utf8');
  const rel = relative(DIST, f);
  if (text.includes('\x00')) errors.push(`NUL sentinel leaked into ${rel}`);
  const prose = text.replace(FENCE, '\n');
  if (prose.includes('{/*')) errors.push(`MDX comment leaked into ${rel}`);
  if (/^[ \t]*:{3,}[a-z]*[ \t]*$/m.test(prose)) errors.push(`unconverted admonition ::: in ${rel}`);
}

// 4. Fenced code survives conversion verbatim (the converter's #1 contract).
const checkFences = (srcPath, outPath, label) => {
  if (!existsSync(outPath)) return;
  const body = readFileSync(srcPath, 'utf8').replace(/\r\n/g, '\n');
  const out = readFileSync(outPath, 'utf8');
  for (const m of body.matchAll(FENCE)) {
    const fence = m[0].replace(/^\n/, '');
    if (!out.includes(fence)) errors.push(`fenced block corrupted in ${label}`);
  }
};
for (const src of contentFiles(DOCS_SRC)) {
  checkFences(src, join(DIST, `${docSlug(src)}.md`), `${docSlug(src)}.md`);
}
for (const src of contentFiles(POSTS_SRC)) {
  checkFences(src, join(DIST, 'examples', `${postSlug(src)}.md`), `examples/${postSlug(src)}.md`);
}

if (errors.length) {
  console.error(`check-agent-output: ${errors.length} problem(s)`);
  for (const e of errors) console.error(`  - ${e}`);
  process.exit(1);
}
console.log('check-agent-output: all agent-facing artifacts OK');

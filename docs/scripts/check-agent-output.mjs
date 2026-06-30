// Post-build checks for the agent-facing artifacts in dist/. Catches the
// silent-corruption class no human reads: MDX scaffolding leaking into .md
// output, fenced code rewritten by the converter, HTML pages missing their
// advertised .md twins, and drift between the artifacts that must agree
// (catalog ↔ example posts, install commands ↔ hosted references, tracked
// .env ↔ .env.example). Run after `astro build` (see package.json).
import { readFileSync, readdirSync, existsSync, statSync } from 'node:fs';
import { join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';
import { FENCE, MDX_COMMENT } from '../src/lib/fence.mjs';

const here = (p) => fileURLToPath(new URL(p, import.meta.url));
const DIST = here('../dist/');
const DOCS_SRC = here('../src/content/docs/');
const POSTS_SRC = here('../src/content/example-posts/');
const EXAMPLES_DIR = here('../../examples/');
const REFERENCES_DIR = here('../../skills/cocoindex/references/');
const errors = [];

const walk = (dir, re) =>
  readdirSync(dir).flatMap((f) => {
    const p = join(dir, f);
    return statSync(p).isDirectory() ? walk(p, re) : re.test(p) ? [p] : [];
  });
const contentFiles = (dir) => walk(dir, /\.mdx?$/);
const read = (p) => readFileSync(p, 'utf8');

// 1. Required artifacts exist; hosted references match the skill folder.
for (const f of ['llms.txt', 'llms-full.txt', 'skill.md']) {
  if (!existsSync(join(DIST, f))) errors.push(`missing dist/${f}`);
}
const refNames = readdirSync(REFERENCES_DIR)
  .filter((f) => f.endsWith('.md'))
  .map((f) => f.replace(/\.md$/, ''));
for (const name of refNames) {
  if (!existsSync(join(DIST, 'references', `${name}.md`)))
    errors.push(`missing dist/references/${name}.md (skill companion)`);
}

// 1b. The copy-paste install commands must fetch every hosted reference.
// The docs UI derives its list at build time; examples/AGENTS.md is static
// text, so drift there is only caught here.
const agentsMd = read(join(EXAMPLES_DIR, 'AGENTS.md'));
for (const name of refNames) {
  if (!new RegExp(`\\b${name}\\b`).test(agentsMd))
    errors.push(`examples/AGENTS.md install command misses references/${name}.md`);
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
  const slug = postSlug(src);
  const twin = join(DIST, 'examples', `${slug}.md`);
  if (!existsSync(twin)) {
    errors.push(
      `example post "${slug}" has no card in src/data/examples.ts — ` +
        `the HTML page and .md twin are both generated from that catalog`,
    );
  } else if (!read(twin).trimEnd().split('\n').slice(6).join('\n').trim()) {
    errors.push(`examples/${slug}.md has an empty body (catalog slug vs post filename mismatch?)`);
  }
}

// 3. No sentinel bytes anywhere; no MDX scaffolding outside fenced code
//    (inside fences it may be a legitimate sample of MDX syntax).
const agentFiles = new Map(
  [join(DIST, 'llms-full.txt'), join(DIST, 'skill.md'), ...walk(DIST, /\.md$/)].map((f) => [
    f,
    read(f),
  ]),
);
for (const [f, text] of agentFiles) {
  const rel = relative(DIST, f);
  if (text.includes('\x00')) errors.push(`NUL sentinel leaked into ${rel}`);
  const prose = text.replace(FENCE, '\n');
  if (prose.includes('{/*')) errors.push(`MDX comment leaked into ${rel}`);
  if (/^[ \t]*:{3,}[a-z]*[ \t]*$/m.test(prose)) errors.push(`unconverted admonition ::: in ${rel}`);
}

// 4. Fenced code survives conversion verbatim (the converter's #1 contract).
//    Mirror the converter's protect-then-strip order: a fence inside an MDX
//    comment is legitimately removed, so only fences whose sentinel survives
//    the comment strip are required to appear in the output.
const survivingFences = (body) => {
  const fences = [];
  let s = body.replace(FENCE, (m, pre) => {
    const lead = typeof pre === 'string' ? pre : '';
    fences.push(m.slice(lead.length));
    return `${lead}\x00F${fences.length - 1}\x00`;
  });
  s = s.replace(MDX_COMMENT, '');
  const out = [];
  for (const m of s.matchAll(/\x00F(\d+)\x00/g)) out.push(fences[Number(m[1])]);
  return out;
};
const checkFences = (srcPath, outPath, label) => {
  const out = agentFiles.get(outPath);
  if (out === undefined) return;
  for (const fence of survivingFences(read(srcPath).replace(/\r\n/g, '\n'))) {
    if (!out.includes(fence)) errors.push(`fenced block corrupted in ${label}`);
  }
};
for (const src of contentFiles(DOCS_SRC)) {
  checkFences(src, join(DIST, `${docSlug(src)}.md`), `${docSlug(src)}.md`);
}
for (const src of contentFiles(POSTS_SRC)) {
  checkFences(src, join(DIST, 'examples', `${postSlug(src)}.md`), `examples/${postSlug(src)}.md`);
}

// 5. Each example's .env.example is a superset of its tracked .env — the
//    documented `cp .env.example .env` step must never drop a default
//    (COCOINDEX_DB regressions were shipped exactly this way).
// Keys assigned in a .env-style file. For the .env.example template, pass
// `includeCommented` to also count commented-out assignments (e.g.
// `# OCI_STREAMING_TOPIC=...`): a copied template still carries that line, so
// the key is documented even when its default is intentionally left disabled.
// The real .env is read strictly — only keys it actively sets must be documented.
const envKeys = (text, { includeCommented = false } = {}) =>
  new Set(
    text
      .split('\n')
      .map((l) => (includeCommented ? l.replace(/^\s*#+\s*/, '') : l))
      .map((l) => l.replace(/^export\s+/, ''))
      .filter((l) => /^[A-Za-z_][A-Za-z0-9_]*=/.test(l))
      .map((l) => l.split('=')[0]),
  );
for (const dir of readdirSync(EXAMPLES_DIR)) {
  const env = join(EXAMPLES_DIR, dir, '.env');
  const tmpl = join(EXAMPLES_DIR, dir, '.env.example');
  if (!existsSync(env) || !existsSync(tmpl)) continue;
  const tmplKeys = envKeys(read(tmpl), { includeCommented: true });
  for (const key of envKeys(read(env))) {
    if (!tmplKeys.has(key))
      errors.push(
        `examples/${dir}/.env has ${key} but .env.example doesn't — ` +
          `"cp .env.example .env" would drop it`,
      );
  }
}

if (errors.length) {
  console.error(`check-agent-output: ${errors.length} problem(s)`);
  for (const e of errors) console.error(`  - ${e}`);
  process.exit(1);
}
console.log('check-agent-output: all agent-facing artifacts OK');

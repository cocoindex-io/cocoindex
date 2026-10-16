// Post-build checks for the agent-facing artifacts in dist/. Catches the
// silent-corruption class no human reads: MDX scaffolding leaking into .md
// output, fenced code rewritten by the converter, HTML pages missing their
// advertised .md twins, and drift between the artifacts that must agree
// (catalog ↔ example posts, root-relative links ↔ emitted pages, install
// commands ↔ hosted references, tracked .env ↔ .env.example). Run after
// `astro build` (see package.json).
import { readFileSync, readdirSync, existsSync, statSync } from 'node:fs';
import { join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';
import { FENCE, INLINE_CODE, MDX_COMMENT } from '../src/lib/fence.mjs';

const here = (p) => fileURLToPath(new URL(p, import.meta.url));
const REPO_ROOT = here('../../');
const DIST = here('../dist/');
const DOCS_SRC = here('../src/content/docs/');
const POSTS_SRC = here('../src/content/example-posts/');
// Files read outside docs/ must also match the `paths:` filters in
// .github/workflows/docs_test.yml, or PRs touching only them skip these checks.
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

// 8. Section overview pages must link every page in their sidebar section.
//    Catches the drift class where a page ships in the sidebar but the
//    hand-curated overview (tables / card grids) silently omits it — a
//    Snowflake-sized hole in a page that claims to be exhaustive.
{
  const sidebarSrc = read(here('../src/data/docs-sidebar.ts'));
  const slugs = [...sidebarSrc.matchAll(/\{ type: 'doc', slug: '([^']+)'/g)].map((m) => m[1]);
  const sections = slugs.filter((s) => !s.includes('/'));
  for (const section of sections) {
    const pages = slugs.filter((s) => s.startsWith(`${section}/`));
    if (!pages.length) continue;
    const overviewPath = join(DIST, section, 'index.html');
    if (!existsSync(overviewPath)) {
      errors.push(`missing dist/${section}/index.html (section overview)`);
      continue;
    }
    const html = read(overviewPath);
    // scope to the article body so the sidebar's own nav links don't satisfy
    // the check; fall back to the full page if the marker is absent.
    const main = html.slice(html.indexOf('<main'), html.lastIndexOf('</main>') + 1) || html;
    for (const page of pages) {
      if (!main.includes(`/docs/${page}/`) && !main.includes(`/docs/${page}"`))
        errors.push(`section overview /docs/${section}/ does not link /docs/${page}/`);
    }
  }
}

// 9. Root-relative links (`/docs/...`) resolve in this build: those written
//    in docs and example-post Markdown, then every one the build emits.
//    Checked here rather than in remark-link-checker (which covers
//    source-relative links) because their targets include catalog example
//    pages, redirect stubs and endpoint files that only dist/ holds in full
//    (and downstream builds move or drop pages), and because Astro's content
//    cache skips re-rendering unchanged .md posts, so a render-time check
//    misses a post whose target page went away.
{
  const isFile = (p) => existsSync(p) && statSync(p).isFile();
  // Why a root-relative `url` fails to resolve in dist/; null if it resolves.
  const rootLinkProblem = (url) => {
    const [, path, fragment] = url.match(/^([^?#]*)(?:\?[^#]*)?(?:#(.*))?$/);
    if (path !== '/docs' && !path.startsWith('/docs/'))
      return 'points outside /docs/ (link docs pages as /docs/<page>/, other sites by absolute URL)';
    const rel = path.slice('/docs/'.length);
    // A page is `<path>/index.html`, linked with the trailing slash
    // (trailingSlash: 'always'); any other link names an emitted file.
    const page = join(DIST, rel, 'index.html');
    const target = path.endsWith('/') ? page : join(DIST, rel);
    if (!isFile(target)) {
      return !path.endsWith('/') && existsSync(page)
        ? `lacks the trailing slash, so it 404s under astro dev (write ${path}/${url.slice(path.length)})`
        : `does not resolve (no dist/${relative(DIST, target)})`;
    }
    if (!fragment) return null;
    const html = read(target);
    if ([...html.matchAll(/\sid="([^"]*)"/g)].some((m) => m[1] === fragment)) return null;
    const where = `dist/${relative(DIST, target)}`;
    return html.includes('http-equiv="refresh"')
      ? `has fragment #${fragment}, but ${where} is a redirect stub, which drops it (link the destination page)`
      : `has fragment #${fragment}, but no id in ${where} matches it`;
  };
  // Inline link and image targets; `//host/...` is absolute, not root-relative.
  const MD_ROOT_LINK = /(?<=\]\()\/(?!\/)[^)\s]*/g;
  // Blank fenced and inline code and MDX comments, keeping newlines so line
  // numbers hold: link-shaped text there is not a link.
  const blank = (m) => m.replace(/[^\n]/g, ' ');
  // URLs already reported at their source line, which the dist pass skips.
  const reported = new Set();
  for (const src of [...contentFiles(DOCS_SRC), ...contentFiles(POSTS_SRC)]) {
    const prose = read(src).replace(FENCE, blank).replace(INLINE_CODE, blank).replace(MDX_COMMENT, blank);
    prose.split('\n').forEach((line, i) => {
      for (const { 0: url, index } of line.matchAll(MD_ROOT_LINK)) {
        const problem = rootLinkProblem(url);
        if (!problem) continue;
        errors.push(`${relative(REPO_ROOT, src)}:${i + 1}:${index + 1}: link ${url} ${problem}`);
        reported.add(url);
      }
    });
  }
  // Then every root-relative link the build emits: hrefs in the HTML pages
  // (the .astro chrome, such as the sidebar, breadcrumbs, example nav and
  // buttons, and redirect stubs) and links in the .md twins and
  // llms-full.txt (the converter's output). GitHub Pages 301-redirects a
  // page URL that lacks the trailing slash, but astro dev 404s it, so
  // without this a chrome href that drops the slash ships unnoticed. Chrome
  // repeats on every page, so each URL is reported once.
  const emitters = new Map(); // url → the dist files that emit it
  const emit = (url, file) => {
    if (!emitters.has(url)) emitters.set(url, new Set());
    emitters.get(url).add(relative(REPO_ROOT, file));
  };
  for (const f of walk(DIST, /\.html$/)) {
    // Attributes of a real tag only: an escaped `&lt;a href="…"` in a code
    // sample is text.
    for (const m of read(f).matchAll(/<[a-z][^>]*?\shref="(\/(?!\/)[^"]*)"/gi)) emit(m[1], f);
  }
  for (const [f, text] of agentFiles) {
    const prose = text.replace(FENCE, '\n').replace(INLINE_CODE, '');
    for (const { 0: url } of prose.matchAll(MD_ROOT_LINK)) emit(url, f);
  }
  for (const [url, files] of emitters) {
    if (reported.has(url)) continue;
    const problem = rootLinkProblem(url);
    if (!problem) continue;
    const [first, ...more] = [...files].sort();
    errors.push(`${first}${more.length ? ` (and ${more.length} more)` : ''}: link ${url} ${problem}`);
  }
}

if (errors.length) {
  console.error(`check-agent-output: ${errors.length} problem(s)`);
  for (const e of errors) console.error(`  - ${e}`);
  process.exit(1);
}
console.log('check-agent-output: all agent-facing artifacts OK');

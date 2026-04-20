#!/usr/bin/env node
// Port CocoIndex docs from the Docusaurus source (../docs/docs/) into this
// Astro project's content collection (src/content/docs/).
//
// Idempotent: wipes src/content/docs/ before re-porting. The Docusaurus
// tree remains the authoring source until the migration is merged; after
// that, this script can be retired.
//
// Content transforms (applied to every .md + .mdx; every output file is
// .mdx so Astro's MDX pipeline runs our remark plugins consistently):
//   - strip `<!-- … -->` HTML comments at the prose level (MDX chokes on them)
//   - strip `import … from '@docusaurus/…'` / `@theme/…` / `@site/…` /
//     any relative import (they won't resolve in Astro and, on .md files,
//     their presence makes Astro skip the remark pipeline entirely)
//   - strip JSX usages of the Docusaurus custom components we aren't porting
//   - rewrite `/img/foo` → `/docs/img/foo` so links resolve under Astro's
//     `base: '/docs'`
//   - normalize `:::info Title` → `:::info[Title]` (standard remark-directive
//     syntax) so admonitions render via scripts/remark-admonitions.mjs
//   - drop the leading `# Title` body H1 (layout renders the H1 from
//     curated metadata — prevents duplicate headings)
//   - strip Docusaurus-only frontmatter keys (`slug`, `sidebar_*`, `id`,
//     `pagination_*`, `hide_title`, `displayed_sidebar`, `custom_edit_url`,
//     `sidebar_class_name`). Astro uses `slug` to override the route; a
//     lingering `slug: /` on overview.md made that page 404.
//
// Static assets under ../docs/static/ are mirrored into public/, minus a
// SKIP_ASSETS list that drops Docusaurus-specific placeholders.
//
// Alongside content the script bakes src/docs-meta.json with:
//   _version: the most recent clean `vMAJOR.MINOR.PATCH` tag on HEAD
//   files:    { <slug>: { sourcePath, editTs } }
//             where editTs = last-commit unix timestamp for the source
//             file (used to render "Last reviewed · Xd ago" in the TOC).

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const SRC_DOCS_ROOT = path.resolve(ROOT, '../docs');
const SRC_DIR = path.join(SRC_DOCS_ROOT, 'docs');
const SRC_STATIC = path.join(SRC_DOCS_ROOT, 'static');
const DEST_DIR = path.resolve(ROOT, 'src/content/docs');
const DEST_PUBLIC = path.resolve(ROOT, 'public');
const DEST_META = path.resolve(ROOT, 'src/docs-meta.json');
const GIT_ROOT = path.resolve(ROOT, '..');

// Deploy prefix — must match `base` in astro.config.mjs. Kept as a
// constant here (rather than parsed from the config) so the port script
// has no Astro dependency.
const BASE = '/docs-v1';

const STRIPPED_COMPONENTS = new Set([
  'Tabs',
  'TabItem',
  'Card',
  'CardGrid',
  'Badge',
  'Button',
  'ActionButtons',
  'GitHubButton',
  'GitHubStar',
  'YouTubeButton',
  'DocumentationButton',
  'ExampleButton',
  'CliCommands',
  'BorderBox',
  'ImageCard',
  'Inset',
  'LastReviewed',
  'RadioCards',
  'VersionSelector',
  'Color',
  'DocCard',
  'DocCardList',
  'BrowserWindow',
  'Admonition',
  'Details',
]);

const SKIP_ASSETS = new Set([
  'docusaurus.png',
  'icon.svg',
  'logo.svg',
  'logo-dark.svg',
]);

function main() {
  if (!fs.existsSync(SRC_DIR)) {
    console.error(`Source not found: ${SRC_DIR}`);
    process.exit(1);
  }
  cleanDest();

  const stats = { files: 0, assets: 0 };
  const meta = { _version: readLatestVersion(), files: {} };
  walk(SRC_DIR, '', stats, meta.files);
  copyStatic(stats);
  fs.writeFileSync(DEST_META, JSON.stringify(meta, null, 2) + '\n');

  console.log(`Ported ${stats.files} docs files, ${stats.assets} assets. Version: ${meta._version}.`);
}

function readLatestVersion() {
  // Most recent tag reachable from HEAD. On `main` this surfaces the
  // latest stable release; on `v1` it surfaces whichever `v1.0.0-alphaN`
  // the preview branch was cut from.
  try {
    const tag = execFileSync(
      'git',
      ['-C', GIT_ROOT, 'describe', '--tags', '--abbrev=0', 'HEAD'],
      { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] },
    ).trim();
    return tag.replace(/^v/, '') || null;
  } catch {
    return null;
  }
}

function lastCommitTs(relPath) {
  try {
    const out = execFileSync(
      'git',
      ['-C', GIT_ROOT, 'log', '-1', '--format=%ct', '--', relPath],
      { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] },
    ).trim();
    return out ? parseInt(out, 10) : 0;
  } catch {
    return 0;
  }
}

function cleanDest() {
  if (fs.existsSync(DEST_DIR)) fs.rmSync(DEST_DIR, { recursive: true, force: true });
  fs.mkdirSync(DEST_DIR, { recursive: true });
  for (const sub of ['img']) {
    const p = path.join(DEST_PUBLIC, sub);
    if (fs.existsSync(p)) fs.rmSync(p, { recursive: true, force: true });
  }
}

function walk(dir, rel, stats, meta) {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const abs = path.join(dir, entry.name);
    const relPath = path.posix.join(rel, entry.name);
    if (entry.isDirectory()) {
      walk(abs, relPath, stats, meta);
      continue;
    }
    let destAbs = path.join(DEST_DIR, relPath);
    fs.mkdirSync(path.dirname(destAbs), { recursive: true });
    if (/\.mdx?$/.test(entry.name)) {
      const raw = fs.readFileSync(abs, 'utf8');
      // Force every ported file to .mdx so the MDX pipeline (and our
      // remark plugins) applies uniformly — Astro's plain-markdown path
      // skips remarkPlugins on .md files inside a glob-loaded collection.
      if (destAbs.endsWith('.md')) destAbs = destAbs.replace(/\.md$/, '.mdx');
      fs.writeFileSync(destAbs, transformMdx(raw));
      stats.files++;

      const slug = relPath.replace(/\.mdx?$/, '');
      const gitPath = `docs/docs/${relPath}`;
      meta[slug] = {
        sourcePath: relPath,
        editTs: lastCommitTs(gitPath),
      };
    } else {
      fs.copyFileSync(abs, destAbs);
      stats.assets++;
    }
  }
}

function copyStatic(stats) {
  if (!fs.existsSync(SRC_STATIC)) return;
  fs.mkdirSync(DEST_PUBLIC, { recursive: true });
  copyTree(SRC_STATIC, DEST_PUBLIC, stats);
}

function copyTree(src, dst, stats) {
  for (const entry of fs.readdirSync(src, { withFileTypes: true })) {
    if (SKIP_ASSETS.has(entry.name)) continue;
    const sAbs = path.join(src, entry.name);
    const dAbs = path.join(dst, entry.name);
    if (entry.isDirectory()) {
      fs.mkdirSync(dAbs, { recursive: true });
      copyTree(sAbs, dAbs, stats);
    } else {
      fs.copyFileSync(sAbs, dAbs);
      stats.assets++;
    }
  }
}

// ---------------------------------------------------------------------------
// Body transforms.
// ---------------------------------------------------------------------------

function transformMdx(raw) {
  const { frontmatter, body } = splitFrontmatter(raw);
  let out = body;
  out = stripHtmlComments(out);
  out = stripDocusaurusImports(out);
  out = stripStrippedComponentJsx(out);
  out = rebaseStaticAssets(out);
  out = normalizeAdmonitionLabels(out);
  out = stripHeadingIds(out);
  out = stripLeadingBodyH1(out);
  const cleanFm = frontmatter === null ? null : stripDocusaurusFrontmatter(frontmatter);
  return cleanFm === null ? out : `---\n${cleanFm}---\n${out}`;
}

// Docusaurus supports explicit heading IDs via `## Title {#custom-id}`.
// MDX treats `{…}` as a JS expression and fails to parse. Strip the ID
// suffix — Astro computes slugs from the heading text, which is close
// enough for a TOC and anchor links within the same page.
function stripHeadingIds(text) {
  return text.replace(/^(#{1,6}[^\n]*?)\s*\{#[a-zA-Z0-9_-]+\}\s*$/gm, '$1');
}

function splitFrontmatter(raw) {
  const m = raw.match(/^---\s*\n([\s\S]*?)\n---\s*\n?([\s\S]*)$/);
  if (!m) return { frontmatter: null, body: raw };
  return { frontmatter: m[1], body: m[2] };
}

function stripDocusaurusFrontmatter(fm) {
  const drop = /^(slug|sidebar_position|sidebar_label|sidebar_class_name|displayed_sidebar|hide_title|pagination_prev|pagination_next|custom_edit_url|id)\s*:/i;
  return fm
    .split('\n')
    .filter((line) => !drop.test(line))
    .join('\n')
    .replace(/\n{3,}/g, '\n\n') + '\n';
}

function stripHtmlComments(text) {
  const parts = splitOnFences(text);
  for (const p of parts) {
    if (p.kind !== 'prose') continue;
    p.text = p.text.replace(/<!--[\s\S]*?-->\s*/g, '');
  }
  return parts.map((p) => p.text).join('');
}

function stripDocusaurusImports(text) {
  // Strip every ES/TS import from prose. Upstream .md files ship MDX
  // imports via relative paths too (`../../src/components/...`), and a
  // single `import` line at the top of a .md file makes Astro's markdown
  // pipeline skip our remark plugins — so strip them all. Python
  // `import` lines inside fenced code blocks are deliberately left alone.
  const parts = splitOnFences(text);
  const re = /^import\s+[\s\S]*?from\s+['"][^'"]+['"]\s*;?\s*\n/gm;
  for (const p of parts) {
    if (p.kind !== 'prose') continue;
    p.text = p.text.replace(re, '');
  }
  return parts.map((p) => p.text).join('');
}

function stripStrippedComponentJsx(text) {
  // Not fence-aware: Docusaurus <Tabs>…</Tabs> wrappers commonly contain
  // the very code fences we'd otherwise split on, so a fence-split
  // approach can't match the closing tag. None of the stripped component
  // names appear inside realistic code examples, so a global strip is safe.
  // Iterate to peel nested structures (Tabs containing TabItem).
  let out = text;
  for (let pass = 0; pass < 8; pass++) {
    let changed = false;
    for (const name of STRIPPED_COMPONENTS) {
      const selfClose = new RegExp(`<${name}\\b[^>]*?/>`, 'g');
      const paired = new RegExp(`<${name}\\b[^>]*?>[\\s\\S]*?</${name}>`, 'g');
      const before = out;
      out = out.replace(selfClose, '').replace(paired, '');
      if (out !== before) changed = true;
    }
    if (!changed) break;
  }
  return out;
}

function rebaseStaticAssets(text) {
  // Markdown and HTML-attribute static paths pick up the deploy prefix.
  let out = text
    .replace(/(\]\()\/img\//g, `$1${BASE}/img/`)
    .replace(/(\]\()\/static\//g, `$1${BASE}/`)
    .replace(/(src=["'])\/img\//g, `$1${BASE}/img/`);
  // Docusaurus templates images with `{useBaseUrl('/img/foo')}`. The
  // helper doesn't exist in Astro and would fail at MDX-compile time, so
  // replace the JSX expression with a plain string at port time.
  out = out.replace(
    /\{useBaseUrl\(\s*['"]([^'"]+)['"]\s*\)\}/g,
    (_m, p) => {
      const clean = p.startsWith('/') ? p : `/${p}`;
      return `"${BASE}${clean.replace(/^\/img\//, '/img/')}"`;
    },
  );
  return out;
}

// Docusaurus accepts `:::info Prerequisite` (space-separated title) as a
// shorthand. Standard remark-directive only understands `:::info[Prerequisite]`.
// Rewrite the shorthand at port time; preserve blocks that already use the
// bracketed / attribute syntax.
function normalizeAdmonitionLabels(text) {
  const kinds = 'info|tip|note|warning|caution|danger';
  const re = new RegExp(`^(\\s*):::(${kinds})[ \\t]+(.+?)[ \\t]*$`, 'gm');
  return text.replace(re, (full, indent, kind, rest) => {
    if (/^[\[{]/.test(rest)) return full;
    return `${indent}:::${kind}[${rest}]`;
  });
}

function stripLeadingBodyH1(text) {
  return text.replace(/^[\s\n]*#\s[^\n]*\n+/, '');
}

function splitOnFences(text) {
  // Conservative fence parser: opening fence is 3+ backticks or tildes;
  // closing is >= opening length of the same char and column. Matters
  // because some docs use ```` python to embed examples containing ``` —
  // a naive split would misclassify regions between them.
  const lines = text.split('\n');
  const parts = [];
  const push = (kind, start, end) => {
    if (start >= end) return;
    const slice = lines.slice(start, end).join('\n') + (end < lines.length ? '\n' : '');
    parts.push({ kind, text: slice });
  };
  let i = 0;
  let proseStart = 0;
  while (i < lines.length) {
    const m = lines[i].match(/^(\s*)(`{3,}|~{3,})(.*)$/);
    if (!m) { i++; continue; }
    const indent = m[1];
    const fence = m[2];
    const fenceChar = fence[0];
    const fenceLen = fence.length;
    push('prose', proseStart, i);
    const codeStart = i;
    i++;
    while (i < lines.length) {
      const cm = lines[i].match(/^(\s*)(`{3,}|~{3,})\s*$/);
      if (cm && cm[1].length <= indent.length && cm[2][0] === fenceChar && cm[2].length >= fenceLen) {
        i++;
        break;
      }
      i++;
    }
    push('code', codeStart, i);
    proseStart = i;
  }
  push('prose', proseStart, lines.length);
  return parts;
}

main();

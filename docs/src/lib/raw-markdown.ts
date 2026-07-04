// Convert MDX page bodies to clean Markdown for the agent-facing endpoints
// (/docs/<slug>.md, /docs/examples/<slug>.md, and /docs/llms-full.txt). The
// goals, in order:
//
//   1. Never corrupt code. Fenced blocks (incl. indented ```sh under a list)
//      and inline `code` spans are protected verbatim — so literal placeholders
//      like `LIST<FLOAT>` or `<COMMAND>` in examples survive untouched.
//   2. Drop MDX scaffolding (ESM `import … from …` / `export const …` lines
//      and `{/* … */}` comments).
//   3. Flatten Starlight admonitions (`:::note[Title]` … `:::`) to a bold
//      callout lead-in, keeping the body prose.
//   4. Remove leftover JSX component tags so agents never see bare
//      `<AppOverview />` noise: tags are stripped (which drops self-closing
//      components entirely and keeps inner content of paired wrappers like
//      `<Tabs>`).
//
// Component names are matched as PascalCase (`[A-Z][a-z]…`) on purpose: it hits
// real components (AppOverview, Tabs, ComponentWithChunks) while leaving
// ALL-CAPS literals (`<FLOAT>`, `<ANY>`, `<COMMAND>`) alone as a second safety
// net beyond code protection.

import { posix } from 'node:path';
import type { CollectionEntry } from 'astro:content';
import { getCollection, getEntry } from 'astro:content';
import { GITHUB_REPO, docSlug, docTitle, pageUrl, LLMS_TXT_URL, SKILL_MD_URL } from '../consts';
import { findExample } from '../data/examples';
import { FENCE, MDX_COMMENT } from './fence.mjs';

const SENT = '\x00'; // sentinel: cannot occur in source text
const INLINE = /`[^`\n]+`/g;
const RESTORE_INLINE = /\x00C(\d+)\x00/g;
const RESTORE_FENCE = /\x00F(\d+)\x00/g;

const ADMONITION_LABEL: Record<string, string> = {
  note: 'Note',
  tip: 'Tip',
  info: 'Info',
  warning: 'Warning',
  caution: 'Caution',
  danger: 'Danger',
  important: 'Important',
};

// Replace each match with a sentinel, keeping any leading newline captured by
// the regex outside the sentinel so blank-line collapsing sees the real layout.
function protect(text: string, re: RegExp, store: string[], tag: string): string {
  return text.replace(re, (m, pre: string | undefined) => {
    const lead = typeof pre === 'string' ? pre : '';
    const i = store.length;
    store.push(m.slice(lead.length));
    return `${lead}${SENT}${tag}${i}${SENT}`;
  });
}

// Conversion runs for both the per-page .md twin and llms-full.txt — memoize
// per body. Skipped in dev: the middleware converts per request on bodies that
// change with every edit, which would grow the map without bound.
const memo = import.meta.env.DEV ? null : new Map<string, string>();

// `srcDir` is the source file's directory relative to the content root ('' for
// top-level pages, 'connectors' for connectors/*). Source-relative links are
// resolved against it and emitted as absolute /docs/... paths — the HTML
// pipeline gets this from remark-link-checker, but the .md twins serve the
// body verbatim and index pages sit one level shallower than their source.
export function mdxToMarkdown(body: string, srcDir = ''): string {
  const key = `${srcDir}\u0000${body ?? ''}`;
  const cached = memo?.get(key);
  if (cached !== undefined) return cached;

  let s = (body ?? '').replace(/\r\n/g, '\n');

  // 1. Protect fenced code, then inline code, behind sentinels.
  const fences: string[] = [];
  s = protect(s, FENCE, fences, 'F');
  const inlines: string[] = [];
  s = protect(s, INLINE, inlines, 'C');

  // 2. Strip ESM import lines (require `from` so we never eat prose starting
  //    with the word "import"), MDX `export …` scaffolding, and MDX comments.
  s = s.replace(/^[ \t]*import\b.+\bfrom\b.+$/gm, '');
  s = s.replace(/^[ \t]*export\s+(?:const|let|var|default|function|\{).+$/gm, '');
  s = s.replace(MDX_COMMENT, '');

  // 2b. Resolve source-relative links to absolute /docs/... paths (fenced and
  //     inline code are already behind sentinels, so hrefs in code are safe).
  s = s.replace(/\]\((\.{1,2}\/[^)\s]*)\)/g, (m, target: string) => {
    const [rel, frag] = target.split('#');
    const path = posix.normalize(posix.join(srcDir, rel)).replace(/\/$/, '');
    if (path.startsWith('..')) return m;
    return `](/docs/${path}${frag ? `#${frag}` : ''})`;
  });

  // 3. Flatten admonitions to a bold callout lead-in, keeping the body prose.
  //    Handles 3+ colons (nesting) and both title forms: `:::note[Title]` and
  //    `::::note Title`. Closing `:::`/`::::` fences are removed.
  s = s.replace(
    /^[ \t]*:{3,}([a-z]+)(?:\[([^\]]*)\]|[ \t]+(.+?))?[ \t]*$/gim,
    (_m, type: string, bracketTitle?: string, spaceTitle?: string) => {
      const title = bracketTitle ?? spaceTitle;
      const label = ADMONITION_LABEL[type.toLowerCase()] ?? type[0].toUpperCase() + type.slice(1);
      return `**${title ? `${label} — ${title}` : label}**`;
    },
  );
  s = s.replace(/^[ \t]*:{3,}[ \t]*$/gm, '');

  // 4. Remove JSX component tags (inline/fenced code already protected). The
  //    `/?` + `[^>]*` body covers open, close, and self-closing forms.
  s = s.replace(/<\/?[A-Z][a-z][A-Za-z0-9]*\b[^>]*>/g, '');

  // 5. Collapse blank lines left by removals — before restoring code, so
  //    blank-line runs inside fenced blocks survive verbatim. Sentinels sit on
  //    their own lines (leading newlines stayed outside them in protect()).
  s = s.replace(/\n{3,}/g, '\n\n');

  // 6. Restore inline code, then fenced code.
  s = s.replace(RESTORE_INLINE, (_m, i) => inlines[Number(i)]);
  s = s.replace(RESTORE_FENCE, (_m, i) => fences[Number(i)]);

  const out = s.trim();
  memo?.set(key, out);
  return out;
}

// Single source for the docs-page route map, shared by the HTML route
// ([...slug].astro) and the Markdown twin ([...slug].md.ts) so the two can
// never enumerate different page sets.
export async function docStaticPaths() {
  const docs = await getCollection('docs');
  return docs.map((doc) => ({ params: { slug: docSlug(doc.id) }, props: { doc } }));
}

// Shared top-of-page banner for every agent-facing .md twin: v1 guard, then
// source/index/skill pointers. One format for docs pages and example pages.
function buildGuard(slug: string, lead: string, extra = ''): string {
  return (
    `> ${lead}\n` +
    `>\n` +
    `> Source: ${pageUrl(slug)} · Docs index: ${LLMS_TXT_URL} · Agent skill: ${SKILL_MD_URL}${extra}\n` +
    `>\n` +
    `> v0→v1 quick map — if you reach for these v0 symbols, stop and use the v1 form: ` +
    `\`@cocoindex.flow_def\`/\`FlowBuilder\` → \`coco.App\` + a \`@coco.fn\` main function; ` +
    `\`add_collector()\`/\`collect()\`/\`export()\` → declare target states (\`declare_row\`, \`declare_file\`); ` +
    `\`cocoindex.sources/functions/targets.*\` → connector APIs (\`localfs.walk_dir\`, \`coco.ops.*\`, \`postgres.declare_table_target\`). ` +
    `Full mapping + API reference: ${SKILL_MD_URL}.`
  );
}

// Full Markdown body for a docs page's /<slug>.md twin. Shared by the
// [...slug].md endpoint and the dev-only middleware fallback.
// Source dir (relative to the content root) for a docs entry, from filePath —
// doc.id is ambiguous for index pages (the loader strips '/index').
export function docSrcDir(doc: CollectionEntry<'docs'>): string {
  const rel = (doc as unknown as { filePath?: string }).filePath?.split(/^(?:.*\/)?src\/content\/docs\//).pop();
  const dir = rel ? posix.dirname(rel) : '';
  return dir === '.' ? '' : dir;
}

export function buildDocMarkdown(doc: CollectionEntry<'docs'>): string {
  const slug = docSlug(doc.id);
  const title = docTitle(doc.id, doc.data.title);
  const guard = buildGuard(
    slug,
    `**CocoIndex v1.** This page documents CocoIndex **v1** — a ground-up redesign ` +
      `from v0. When writing code, ignore any v0 flow-builder DSL or deprecated decorators.`,
  );
  return `# ${title}\n\n${guard}\n\n${mdxToMarkdown(doc.body, docSrcDir(doc))}\n`;
}

// Full Markdown body for an example walkthrough's /examples/<slug>.md twin.
// Shared by the examples/[slug].md endpoint and the dev-only middleware.
export async function buildExampleMarkdown(slug: string): Promise<string | undefined> {
  const meta = findExample(slug);
  if (!meta) return undefined;
  const entry = await getEntry('examplePosts', slug);
  const title = docTitle(slug, meta.title);
  const sourceDir = meta.sourceSlug ?? slug;
  const guard = buildGuard(
    `examples/${slug}`,
    `**CocoIndex v1 example.** This walkthrough uses the CocoIndex **v1** API — ` +
      `ignore any v0 flow-builder DSL or deprecated decorators.`,
    `\n> Runnable source: ${GITHUB_REPO}/tree/main/examples/${sourceDir}`,
  );
  const body = entry?.body ? mdxToMarkdown(entry.body, 'examples') : '';
  return `# ${title}\n\n${guard}\n\n${body}\n`;
}

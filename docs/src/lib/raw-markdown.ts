// Convert MDX page bodies to clean Markdown for the agent-facing endpoints
// (/docs/<slug>.md and /docs/llms-full.txt). The goals, in order:
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

import type { CollectionEntry } from 'astro:content';
import { getCollection } from 'astro:content';
import { docSlug, docTitle, pageUrl, LLMS_TXT_URL, SKILL_MD_URL } from '../consts';

const SENT = '\x00'; // sentinel: cannot occur in source text
// Fence runs are captured as a whole (`{3,}) so a ```` opener is not closed by
// a bare ``` line inside it; the closer may be longer than the opener, per
// CommonMark. (Kept in sync with docs/scripts/check-agent-output.mjs.)
const FENCE = /(^|\n)[ \t]*(`{3,}|~{3,})[^\n]*\n[\s\S]*?\n[ \t]*\2[`~]*[ \t]*(?=\n|$)/g;
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

function protect(text: string, re: RegExp, store: string[], tag: string): string {
  return text.replace(re, (m) => {
    const i = store.length;
    store.push(m);
    return `${SENT}${tag}${i}${SENT}`;
  });
}

// Conversion runs for both the per-page .md twin and llms-full.txt (and per
// request in the dev middleware) — memoize per body.
const memo = new Map<string, string>();

export function mdxToMarkdown(body: string): string {
  const cached = memo.get(body ?? '');
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
  s = s.replace(/\{\/\*[\s\S]*?\*\/\}/g, '');

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
  //    blank-line runs inside fenced blocks survive verbatim.
  s = s.replace(/\n{3,}/g, '\n\n');

  // 6. Restore inline code, then fenced code.
  s = s.replace(RESTORE_INLINE, (_m, i) => inlines[Number(i)]);
  s = s.replace(RESTORE_FENCE, (_m, i) => fences[Number(i)]);

  const out = s.trim();
  memo.set(body ?? '', out);
  return out;
}

// Single source for the docs-page route map, shared by the HTML route
// ([...slug].astro) and the Markdown twin ([...slug].md.ts) so the two can
// never enumerate different page sets.
export async function docStaticPaths() {
  const docs = await getCollection('docs');
  return docs.map((doc) => ({ params: { slug: docSlug(doc.id) }, props: { doc } }));
}

// Full Markdown body for a docs page's /<slug>.md twin: H1, the top-of-page v1
// guard + index/skill pointers, then the cleaned body. Single source of truth
// shared by the [...slug].md endpoint and the dev-only middleware fallback.
export function buildDocMarkdown(doc: CollectionEntry<'docs'>): string {
  const slug = docSlug(doc.id);
  const title = docTitle(doc.id, doc.data.title);
  const guard =
    `> **CocoIndex v1.** This page documents CocoIndex **v1** — a ground-up redesign ` +
    `from v0. When writing code, ignore any v0 flow-builder DSL or deprecated decorators.\n` +
    `>\n` +
    `> Source: ${pageUrl(slug)} · Docs index: ${LLMS_TXT_URL} · Agent skill: ${SKILL_MD_URL}`;
  return `# ${title}\n\n${guard}\n\n${mdxToMarkdown(doc.body)}\n`;
}

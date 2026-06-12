// Generates /docs/llms.txt — a machine-readable index of the docs for LLMs and
// agents (see https://llmstxt.org/). Built from the same sidebar tree and
// per-page descriptions that drive the site, so it stays in sync automatically.
import type { APIRoute } from 'astro';
import { getCollection } from 'astro:content';
import { SITE_URL, GITHUB_REPO } from '../consts';
import { sidebar, type SidebarDoc } from '../data/docs-sidebar';
import { EXAMPLE_CATALOG } from '../data/examples';

const base = import.meta.env.BASE_URL.replace(/\/$/, '');
const url = (slug: string) => new URL(`${base}/${slug}/`, SITE_URL).toString();
const markdownUrl = (slug: string) => new URL(`${base}/${slug}.md`, SITE_URL).toString();
const oneLine = (s?: string) => (s ?? '').replace(/\s+/g, ' ').trim();

export const GET: APIRoute = async () => {
  const docs = await getCollection('docs');
  const desc = new Map<string, string>();
  for (const d of docs) desc.set(d.id, oneLine(d.data.description));

  const line = (slug: string, label?: string) => {
    const d = desc.get(slug);
    return `- [${label ?? slug}](${url(slug)})${d ? `: ${d}` : ''}`;
  };

  const out: string[] = [
    '# CocoIndex Docs',
    '',
    '> CocoIndex is an ultra-performant framework for building data pipelines for AI, ' +
      'with built-in incremental processing. Declare what your target should look like ' +
      'as a function of your source — the Rust engine keeps it in sync, reprocessing ' +
      'only what changed.',
    '',
    '> Full docs text in one file: ' + new URL(`${base}/llms-full.txt`, SITE_URL).toString() +
      `. Regular docs pages also have raw Markdown twins by replacing the trailing slash with \`.md\`, e.g. ${markdownUrl('programming_guide/core_concepts')}. Example walkthroughs are included in llms-full.txt.`,
    '',
  ];

  // Standalone top-level docs (Core Concepts, CLI, FAQ) surfaced first.
  const standalone = sidebar.filter((i): i is SidebarDoc => i.type === 'doc');
  if (standalone.length) {
    out.push('## Key pages');
    for (const d of standalone) out.push(line(d.slug, d.label));
    out.push('');
  }

  for (const item of sidebar) {
    if (item.type !== 'category') continue;
    out.push(`## ${item.label}`);
    if (item.slug) out.push(line(item.slug, item.label));
    for (const sub of item.items) {
      if (sub.type === 'doc') out.push(line(sub.slug, sub.label));
      else for (const s2 of sub.items) if (s2.type === 'doc') out.push(line(s2.slug, s2.label));
    }
    out.push('');
  }

  // Every runnable example in the monorepo — the on-page listing curates a few,
  // but agents should see the whole set (and how to clone any one) in one fetch.
  // Documented examples link to their walkthrough; the rest link to source.
  out.push('## Examples');
  out.push(
    `> ${EXAMPLE_CATALOG.length} runnable examples in the monorepo. ` +
      'Clone the repo with `git clone ' +
      GITHUB_REPO +
      '`, then `cd cocoindex/examples/<dir>`, copy `.env.example` if present, install with `pip install -e .`, and run the command shown for that example.',
  );
  out.push('');
  for (const ex of EXAMPLE_CATALOG) {
    const href = ex.docs
      ? url(`examples/${ex.docs}`)
      : `${GITHUB_REPO}/tree/main/examples/${ex.dir}`;
    out.push(`- [${ex.title}](${href}): ${oneLine(ex.description)} (examples/${ex.dir}; run: \`${ex.run ?? 'cocoindex update main'}\`)`);
  }
  out.push('');

  return new Response(out.join('\n'), {
    headers: { 'Content-Type': 'text/plain; charset=utf-8' },
  });
};

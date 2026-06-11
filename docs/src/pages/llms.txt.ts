// Generates /docs/llms.txt — a machine-readable index of the docs for LLMs and
// agents (see https://llmstxt.org/). Built from the same sidebar tree and
// per-page descriptions that drive the site, so it stays in sync automatically.
import type { APIRoute } from 'astro';
import { getCollection } from 'astro:content';
import { SITE_URL } from '../consts';
import { sidebar, type SidebarDoc } from '../data/docs-sidebar';

const base = import.meta.env.BASE_URL.replace(/\/$/, '');
const url = (slug: string) => new URL(`${base}/${slug}/`, SITE_URL).toString();
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

  return new Response(out.join('\n'), {
    headers: { 'Content-Type': 'text/plain; charset=utf-8' },
  });
};

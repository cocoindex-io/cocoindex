// Emits a raw-Markdown twin of every content-collection docs page at
// /docs/<slug>.md (Python) and /docs/rust/<slug>.md (Rust) — agents and LLMs
// prefer clean Markdown over scraping rendered HTML. Shares the path map with
// [...slug].astro via docStaticPaths, so the two never enumerate different
// page sets. The Markdown body is built by buildDocMarkdown (shared with the
// dev-only middleware that patches Astro's dev-server 404 on these endpoints).
import type { APIRoute } from 'astro';
import { type CollectionEntry } from 'astro:content';
import { buildDocMarkdown, docStaticPaths } from '../lib/raw-markdown';
import { pageRegistry, type Lang } from '../lib/lang';

export const getStaticPaths = docStaticPaths;

export const GET: APIRoute = async ({ props }) => {
  const { doc, lang, route } = props as { doc: CollectionEntry<'docs'>; lang: Lang; route: string };
  const reg = await pageRegistry();
  return new Response(buildDocMarkdown(doc, lang, route, (s) => reg.resolve(s, lang)?.href), {
    headers: { 'Content-Type': 'text/markdown; charset=utf-8' },
  });
};

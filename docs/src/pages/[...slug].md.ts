// Emits a raw-Markdown twin of every content-collection docs page at
// /docs/<slug>.md — agents and LLMs prefer clean Markdown over scraping rendered
// HTML. Shares the slug map with [...slug].astro via docStaticPaths.
// The Markdown body is built by buildDocMarkdown (shared with the dev-only
// middleware that patches Astro's dev-server 404 on these endpoints).
import type { APIRoute } from 'astro';
import { type CollectionEntry } from 'astro:content';
import { buildDocMarkdown, docStaticPaths } from '../lib/raw-markdown';

export const getStaticPaths = docStaticPaths;

export const GET: APIRoute = ({ props }) => {
  const { doc } = props as { doc: CollectionEntry<'docs'> };
  return new Response(buildDocMarkdown(doc), {
    headers: { 'Content-Type': 'text/markdown; charset=utf-8' },
  });
};

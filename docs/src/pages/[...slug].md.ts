// Emits a raw-Markdown twin of every content-collection docs page at
// /docs/<slug>.md — agents and LLMs prefer clean Markdown over scraping rendered
// HTML. Mirrors the slug map in [...slug].astro for regular docs pages.
// The Markdown body is built by buildDocMarkdown (shared with the dev-only
// middleware that patches Astro's dev-server 404 on these endpoints).
import type { APIRoute } from 'astro';
import { getCollection, type CollectionEntry } from 'astro:content';
import { docSlug } from '../consts';
import { buildDocMarkdown } from '../lib/raw-markdown';

export async function getStaticPaths() {
  const docs = await getCollection('docs');
  return docs.map((doc) => ({ params: { slug: docSlug(doc.id) }, props: { doc } }));
}

export const GET: APIRoute = ({ props }) => {
  const { doc } = props as { doc: CollectionEntry<'docs'> };
  return new Response(buildDocMarkdown(doc), {
    headers: { 'Content-Type': 'text/markdown; charset=utf-8' },
  });
};

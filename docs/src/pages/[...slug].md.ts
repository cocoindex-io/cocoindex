// Emits a raw-Markdown twin of every content-collection docs page at
// /docs/<slug>.md — agents and LLMs prefer clean Markdown over scraping rendered
// HTML. Mirrors the slug map in [...slug].astro for regular docs pages.
import type { APIRoute } from 'astro';
import { getCollection, type CollectionEntry } from 'astro:content';
import { SITE_URL, docSlug, titleText } from '../consts';

const base = import.meta.env.BASE_URL.replace(/\/$/, '');

// Strip MDX ESM import lines (e.g. `import Tabs from '...'`) — noise for agents.
const cleanBody = (s?: string) =>
  (s ?? '').replace(/^import\s.+?from\s.+?;?\s*$/gm, '').replace(/\n{3,}/g, '\n\n').trim();

export async function getStaticPaths() {
  const docs = await getCollection('docs');
  return docs.map((doc) => ({ params: { slug: docSlug(doc.id) }, props: { doc } }));
}

export const GET: APIRoute = ({ props }) => {
  const { doc } = props as { doc: CollectionEntry<'docs'> };
  const slug = docSlug(doc.id);
  const title = titleText(typeof doc.data.title === 'string' ? doc.data.title : slug);
  const url = new URL(`${base}/${slug}/`, SITE_URL).toString();
  const body = `# ${title}\n\n> Source: ${url}\n\n${cleanBody(doc.body)}\n`;
  return new Response(body, {
    headers: { 'Content-Type': 'text/markdown; charset=utf-8' },
  });
};

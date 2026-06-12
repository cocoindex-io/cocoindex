// Raw-Markdown twins for the example walkthrough pages at
// /docs/examples/<slug>.md, matching the .md contract regular docs pages have.
// Enumerates the same catalog as examples/[slug].astro; the body is built by
// buildExampleMarkdown (shared with the dev-only middleware).
import type { APIRoute } from 'astro';
import { examples } from '../../data/examples';
import { buildExampleMarkdown } from '../../lib/raw-markdown';

export function getStaticPaths() {
  return examples.map((e) => ({ params: { slug: e.slug } }));
}

export const GET: APIRoute = async ({ params }) => {
  const body = await buildExampleMarkdown(params.slug!);
  if (body === undefined) return new Response('Not found', { status: 404 });
  return new Response(body, {
    headers: { 'Content-Type': 'text/markdown; charset=utf-8' },
  });
};

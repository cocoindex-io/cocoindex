// Raw-Markdown twins for the example walkthrough pages at
// /docs/examples/<slug>.md, matching the .md contract regular docs pages have.
// Enumerates the same catalog as examples/[slug].astro.
import type { APIRoute } from 'astro';
import { getEntry } from 'astro:content';
import { docTitle, pageUrl, LLMS_TXT_URL, SKILL_MD_URL, GITHUB_REPO } from '../../consts';
import { examples, findExample } from '../../data/examples';
import { mdxToMarkdown } from '../../lib/raw-markdown';

export function getStaticPaths() {
  return examples.map((e) => ({ params: { slug: e.slug } }));
}

export const GET: APIRoute = async ({ params }) => {
  const slug = params.slug!;
  const meta = findExample(slug);
  const entry = await getEntry('examplePosts', slug);
  const title = docTitle(slug, meta?.title);
  const source = meta?.dir ? `\n> Runnable source: ${GITHUB_REPO}/tree/main/examples/${meta.dir}` : '';
  const guard =
    `> **CocoIndex v1 example.** This walkthrough uses the CocoIndex **v1** API — ` +
    `ignore any v0 flow-builder DSL or deprecated decorators.\n` +
    `>\n` +
    `> Source: ${pageUrl(`examples/${slug}`)} · Docs index: ${LLMS_TXT_URL} · Agent skill: ${SKILL_MD_URL}${source}`;
  const body = entry?.body ? mdxToMarkdown(entry.body) : '';
  return new Response(`# ${title}\n\n${guard}\n\n${body}\n`, {
    headers: { 'Content-Type': 'text/markdown; charset=utf-8' },
  });
};

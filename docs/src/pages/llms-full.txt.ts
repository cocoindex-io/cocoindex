// Generates /docs/llms-full.txt — the full text of every docs page and example
// walkthrough concatenated into one file, for LLMs and agents that want the
// programming guide + connector reference + runnable examples in a single fetch
// (see https://llmstxt.org/). /docs/llms.txt is the lighter index.
import type { APIRoute } from 'astro';
import { getCollection } from 'astro:content';
import { SITE_URL, docSlug, titleText } from '../consts';
import { examples, findExample } from '../data/examples';
import { flatten } from '../data/docs-sidebar';
import { mdxToMarkdown } from '../lib/raw-markdown';

const base = import.meta.env.BASE_URL.replace(/\/$/, '');
const pageUrl = (slug: string) => new URL(`${base}/${slug}/`, SITE_URL).toString();
const exampleUrl = (slug: string) => new URL(`${base}/examples/${slug}/`, SITE_URL).toString();

export const GET: APIRoute = async () => {
  const docs = await getCollection('docs');
  const bySlug = new Map(docs.map((d) => [docSlug(d.id), d]));

  const out: string[] = [
    '# CocoIndex Docs — full text',
    '',
    '> The complete CocoIndex documentation and example walkthroughs concatenated ' +
      'into one file for LLMs and agents. Each section below is one docs page or ' +
      'example page, in reading order. ' +
      'For a lighter index of pages and examples, see /docs/llms.txt.',
  ];

  const seen = new Set<string>();
  const emit = (slug: string) => {
    const d = bySlug.get(slug);
    if (!d || seen.has(slug)) return;
    seen.add(slug);
    const title = titleText(typeof d.data.title === 'string' ? d.data.title : slug);
    out.push('', '---', '', `# ${title}`, '', `Source: ${pageUrl(slug)}`, '', mdxToMarkdown(d.body));
  };

  // Sidebar order first, then any stray docs not in the tree.
  for (const e of flatten()) emit(e.slug);
  for (const slug of bySlug.keys()) emit(slug);

  const examplePosts = await getCollection('examplePosts');
  const byExampleSlug = new Map(examplePosts.map((e) => [e.id.replace(/\.mdx?$/, ''), e]));
  const emittedExamples = new Set<string>();
  const emitExample = (slug: string) => {
    const post = byExampleSlug.get(slug);
    if (!post || emittedExamples.has(slug)) return;
    emittedExamples.add(slug);
    const meta = findExample(slug);
    const title = titleText(meta?.title ?? slug);
    out.push('', '---', '', `# Example: ${title}`, '', `Source: ${exampleUrl(slug)}`, '', mdxToMarkdown(post.body));
  };

  out.push('', '---', '', '# Example Walkthroughs');
  for (const e of examples) emitExample(e.slug);
  for (const slug of byExampleSlug.keys()) emitExample(slug);

  return new Response(out.join('\n') + '\n', {
    headers: { 'Content-Type': 'text/plain; charset=utf-8' },
  });
};

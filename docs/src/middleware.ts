import { defineMiddleware } from 'astro:middleware';
import { getCollection } from 'astro:content';
import { docSlug, DOCS_BASE as base } from './consts';
import { buildDocMarkdown, buildExampleMarkdown } from './lib/raw-markdown';
import { readSkillFile, skillReferenceNames } from './lib/skill-files';

// Dev-only workaround for an Astro dev-server bug (withastro/astro#10149,
// #16140): with `trailingSlash: 'always'`, dynamic file endpoints like
// `[...slug].md.ts` 404 in `astro dev`, even though they build and serve fine
// in production. We intercept `/<slug>.md` here and return the exact Markdown
// the endpoints produce, for all three dynamic .md families: docs pages,
// example walkthroughs, and the skill's references. Inert in the production
// build — `import.meta.env.DEV` is false, so it falls straight through to
// `next()` and the prerendered static files serve as usual.
export const onRequest = defineMiddleware(async (context, next) => {
  if (import.meta.env.DEV && context.url.pathname.endsWith('.md')) {
    const path = context.url.pathname.startsWith(base)
      ? context.url.pathname.slice(base.length)
      : context.url.pathname;
    const slug = path.replace(/^\//, '').replace(/\.md$/, '');
    const markdown = (body: string) =>
      new Response(body, { headers: { 'Content-Type': 'text/markdown; charset=utf-8' } });

    const refName = slug.startsWith('references/') ? slug.slice('references/'.length) : null;
    if (refName && skillReferenceNames().includes(refName)) {
      return markdown(readSkillFile(`references/${refName}.md`));
    }
    if (slug.startsWith('examples/')) {
      const body = await buildExampleMarkdown(slug.slice('examples/'.length));
      if (body !== undefined) return markdown(body);
    }
    const doc = (await getCollection('docs')).find((d) => docSlug(d.id) === slug);
    if (doc) return markdown(buildDocMarkdown(doc));
  }
  return next();
});

import { defineMiddleware } from 'astro:middleware';
import { getCollection } from 'astro:content';
import { docSlug, DOCS_BASE as base } from './consts';
import { buildDocMarkdown } from './lib/raw-markdown';

// Dev-only workaround for an Astro dev-server bug (withastro/astro#10149,
// #16140): with `trailingSlash: 'always'`, dynamic file endpoints like
// `[...slug].md.ts` 404 in `astro dev`, even though they build and serve fine
// in production. We intercept `/<slug>.md` here and return the exact Markdown
// the endpoint produces (via the shared buildDocMarkdown). Inert in the
// production build — `import.meta.env.DEV` is false, so it falls straight
// through to `next()` and the prerendered static files serve as usual.
export const onRequest = defineMiddleware(async (context, next) => {
  if (import.meta.env.DEV && context.url.pathname.endsWith('.md')) {
    const path = context.url.pathname.startsWith(base)
      ? context.url.pathname.slice(base.length)
      : context.url.pathname;
    const slug = path.replace(/^\//, '').replace(/\.md$/, '');
    const doc = (await getCollection('docs')).find((d) => docSlug(d.id) === slug);
    if (doc) {
      return new Response(buildDocMarkdown(doc), {
        headers: { 'Content-Type': 'text/markdown; charset=utf-8' },
      });
    }
  }
  return next();
});

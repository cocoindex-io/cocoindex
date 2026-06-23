// @ts-check
import { defineConfig } from 'astro/config';
import mdx from '@astrojs/mdx';
import sitemap from '@astrojs/sitemap';
import remarkDirective from 'remark-directive';
import remarkAdmonitions from './scripts/remark-admonitions.mjs';
import remarkCodeTitles from './scripts/remark-code-titles.mjs';
import remarkMermaid from './scripts/remark-mermaid.mjs';
import remarkLinkChecker from './scripts/remark-link-checker.mjs';
import { redirects } from './src/data/docs-sidebar.ts';
import docsMeta from './src/data/docs-meta.json';
// One shared Shiki theme (the readability-tuned --code-* palette) so docs and
// blog highlight code identically — single source of truth (GUIDELINE §5.5).
import { cocoindexCodeTheme } from '@cocoindex/brand/code-theme';

// V1 docs are served from https://cocoindex.io/docs/, matching the
// Docusaurus URLs on the v1 branch (`baseUrl: '/docs/'` in
// docs/docusaurus.config.ts). `base` handles the prefix.
const BASE = '/docs';
// `remark-link-checker` both validates *and* rewrites relative links: under
// `build.format: 'directory'` (the default), source-relative `./foo` links
// resolve incorrectly in the browser (a page at `/programming_guide/x/`
// makes `./foo` mean `/programming_guide/x/foo`). The plugin emits absolute
// hrefs (`/docs/<slug>`) so links work regardless of trailing-slash quirks.
// `[plugin, options]` tuples need an explicit type — TypeScript otherwise
// widens the array literal to `(Plugin | Options)[]` and Astro rejects it.
/** @type {any[]} */
const remarkPlugins = [
  remarkDirective,
  remarkAdmonitions,
  remarkMermaid,
  remarkCodeTitles,
  [remarkLinkChecker, { base: BASE }],
];

export default defineConfig({
  site: 'https://cocoindex.io',
  base: BASE,
  // `trailingSlash: 'always'` matches `build.format: 'directory'`: every
  // page lives at `<slug>/index.html` and is canonical at `<slug>/`. In
  // dev, requests without the trailing slash 404 — that strictness is the
  // point: the link-checker plugin catches no-slash hrefs in markdown/MDX,
  // and `'always'` catches no-slash hrefs in `.astro` components (sidebar,
  // breadcrumb, pager, future pieces) before they ship. External / legacy
  // links without the slash still resolve in production via GitHub Pages's
  // own 301 redirect, so this doesn't break inbound traffic.
  trailingSlash: 'always',
  integrations: [
    mdx({
      // MDX's own remark pipeline doesn't inherit `markdown.remarkPlugins`
      // reliably across Astro versions — wire admonitions + code titles
      // explicitly so .mdx content collection pages get them for sure.
      remarkPlugins,
    }),
    sitemap({
      // Emit a real per-page `lastmod` from the human-set "Last reviewed"
      // timestamp in docs-meta.json (the same value the on-page stamp uses),
      // plus a higher priority for the entry pages. Without this every URL
      // ships bare `<loc>`, so Google has no per-page freshness signal to
      // decide what to re-crawl. Slug is the path between `/docs/` and the
      // trailing slash; pages with no recorded review date (examples, index)
      // simply omit lastmod rather than claiming a bogus build-time date.
      serialize(item) {
        const m = item.url.match(/\/docs\/(.*?)\/?$/);
        const slug = m ? m[1] : '';
        const ts = docsMeta.files?.[slug]?.reviewedTs;
        if (ts) item.lastmod = new Date(ts * 1000).toISOString();
        if (slug === '') {
          item.priority = 1.0;
          item.changefreq = 'weekly';
        } else if (slug.startsWith('getting_started')) {
          item.priority = 0.9;
          item.changefreq = 'weekly';
        } else {
          item.changefreq = 'weekly';
        }
        return item;
      },
    }),
  ],
  markdown: {
    remarkPlugins,
    shikiConfig: { theme: cocoindexCodeTheme, wrap: false },
  },
  redirects,
  // Vite's default envPrefix is `VITE_`; Astro adds `PUBLIC_`. We also
  // want unprefixed `COCOINDEX_DOCS_ALGOLIA_*` names exposed to
  // import.meta.env in `.astro` frontmatter — those come from the
  // GitHub Actions vars (see .github/workflows/_docs_release.yml) and
  // are matched by the same names in docs/.env locally. The Algolia
  // search-only API key is public by design; it's safe to inline.
  vite: {
    envPrefix: ['VITE_', 'PUBLIC_', 'COCOINDEX_'],
  },
});

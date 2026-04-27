// @ts-check
import { defineConfig } from 'astro/config';
import mdx from '@astrojs/mdx';
import sitemap from '@astrojs/sitemap';
import remarkDirective from 'remark-directive';
import remarkAdmonitions from './scripts/remark-admonitions.mjs';
import remarkLinkChecker from './scripts/remark-link-checker.mjs';
import { redirects } from './src/data/docs-sidebar.ts';

// Shiki theme that matches design_guidelines/CocoIndex Docs.html:
//   bg #2A121B (maroon-ink), fg cream, coral keywords, pink function names,
//   palm-green numbers, muted-cream comments, purple types.
const cocoindexCodeTheme = {
  name: 'cocoindex-dark',
  type: 'dark',
  colors: {
    'editor.background': '#2A121B',
    'editor.foreground': '#FCF3D8',
  },
  tokenColors: [
    {
      scope: ['comment', 'punctuation.definition.comment', 'string.comment'],
      settings: { foreground: '#978A74', fontStyle: 'italic' }
    },
    {
      scope: ['keyword', 'keyword.control', 'keyword.operator.new',
        'storage', 'storage.type', 'storage.modifier'],
      settings: { foreground: '#E59A63' }
    },
    {
      scope: ['entity.name.function', 'meta.function-call', 'support.function',
        'variable.function'],
      settings: { foreground: '#FB6A76' }
    },
    {
      scope: ['string', 'string.quoted', 'string.template',
        'punctuation.definition.string'],
      settings: { foreground: '#8ef09e' }
    },
    {
      scope: ['constant.numeric', 'constant.language',
        'constant.language.boolean', 'constant.language.null'],
      settings: { foreground: '#27E62B' }
    },
    {
      scope: ['entity.name.type', 'entity.name.class', 'support.type',
        'support.class', 'meta.type.annotation'],
      settings: { foreground: '#c9a0ff' }
    },
    {
      scope: ['meta.decorator', 'variable.other.decorator', 'entity.name.decorator',
        'punctuation.definition.decorator'],
      settings: { foreground: '#E59A63' }
    },
    {
      scope: ['variable', 'variable.other', 'variable.parameter'],
      settings: { foreground: '#FCF3D8' }
    },
  ],
};

// V0 docs are served from https://cocoindex.io/docs-v0/ as the legacy
// location (v1 takes over /docs). `base` handles the prefix.
const BASE = '/docs-v0';
// `remark-link-checker` both validates *and* rewrites relative links: under
// `build.format: 'directory'` (the default), source-relative `./foo` links
// resolve incorrectly in the browser (a page at `/x/` makes `./foo` mean
// `/x/foo`). The plugin emits absolute hrefs (`<base>/<slug>/`) so links
// work regardless of trailing-slash quirks.
/** @type {any[]} */
const remarkPlugins = [
  remarkDirective,
  remarkAdmonitions,
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
  // breadcrumb, pager) before they ship. External / legacy links without
  // the slash still resolve in production via GitHub Pages's own 301
  // redirect, so this doesn't break inbound traffic.
  trailingSlash: 'always',
  integrations: [
    mdx({
      // MDX's own remark pipeline doesn't inherit `markdown.remarkPlugins`
      // reliably across Astro versions — wire admonitions explicitly so
      // .mdx content collection pages get them for sure.
      remarkPlugins,
    }),
    sitemap(),
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

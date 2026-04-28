// @ts-check
import { defineConfig } from 'astro/config';
import mdx from '@astrojs/mdx';
import sitemap from '@astrojs/sitemap';
import remarkDirective from 'remark-directive';
import remarkAdmonitions from './scripts/remark-admonitions.mjs';
import remarkCodeTitles from './scripts/remark-code-titles.mjs';
import remarkLinkChecker from './scripts/remark-link-checker.mjs';
import { redirects } from './src/data/docs-sidebar.ts';

// Shiki theme — canonical token palette from
// design_guidelines/ui/color.html §04 (.code-showcase .tk-*). Saturated brand
// accents are softened for ten-line snippets: pink→salmon for fn names,
// gold→muted-amber for numbers/booleans. Background is maroon-ink.
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
      settings: { foreground: '#FF9B8A' }
    },
    {
      scope: ['string', 'string.quoted', 'string.template',
        'punctuation.definition.string'],
      settings: { foreground: '#8EF09E' }
    },
    {
      scope: ['constant.numeric', 'constant.language',
        'constant.language.boolean', 'constant.language.null'],
      settings: { foreground: '#D4B86A' }
    },
    {
      scope: ['entity.name.type', 'entity.name.class', 'support.type',
        'support.class', 'meta.type.annotation'],
      settings: { foreground: '#C9A0FF' }
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

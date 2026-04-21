// @ts-check
import { defineConfig } from 'astro/config';
import mdx from '@astrojs/mdx';
import sitemap from '@astrojs/sitemap';
import remarkDirective from 'remark-directive';
import remarkAdmonitions from './scripts/remark-admonitions.mjs';
import remarkCodeTitles from './scripts/remark-code-titles.mjs';
import { redirects } from './src/docs-sidebar.ts';

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

// V1 docs are served from https://cocoindex.io/docs-v1/, matching the
// Docusaurus URLs on the v1 branch (`baseUrl: '/docs-v1/'` in
// docs/docusaurus.config.ts). `base` handles the prefix; `trailingSlash:
// 'never'` keeps /docs-v1/core/basics shaped exactly like Docusaurus.
export default defineConfig({
  site: 'https://cocoindex.io',
  base: '/docs-v1',
  integrations: [
    mdx({
      // MDX's own remark pipeline doesn't inherit `markdown.remarkPlugins`
      // reliably across Astro versions — wire admonitions + code titles
      // explicitly so .mdx content collection pages get them for sure.
      remarkPlugins: [remarkDirective, remarkAdmonitions, remarkCodeTitles],
    }),
    sitemap(),
  ],
  markdown: {
    remarkPlugins: [remarkDirective, remarkAdmonitions, remarkCodeTitles],
    shikiConfig: { theme: cocoindexCodeTheme, wrap: false },
  },
  redirects,
});

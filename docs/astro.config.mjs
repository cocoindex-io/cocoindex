// @ts-check
import { defineConfig } from 'astro/config';
import mdx from '@astrojs/mdx';
import sitemap from '@astrojs/sitemap';
import remarkDirective from 'remark-directive';
import remarkAdmonitions from './scripts/remark-admonitions.mjs';
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

// Docs are served from https://cocoindex.io/docs/, matching the Docusaurus
// URLs we're replacing. `base` handles the prefix; `trailingSlash: 'never'`
// keeps /docs/core/basics shaped exactly like Docusaurus did.
export default defineConfig({
  site: 'https://cocoindex.io',
  base: '/docs',
  integrations: [
    mdx({
      // MDX's own remark pipeline doesn't inherit `markdown.remarkPlugins`
      // reliably across Astro versions — wire admonitions explicitly so
      // .mdx content collection pages get them for sure.
      remarkPlugins: [remarkDirective, remarkAdmonitions],
    }),
    sitemap(),
  ],
  markdown: {
    remarkPlugins: [remarkDirective, remarkAdmonitions],
    shikiConfig: { theme: cocoindexCodeTheme, wrap: false },
  },
  redirects,
});

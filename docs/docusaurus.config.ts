import webpack from 'webpack';
import { themes as prismThemes } from 'prism-react-renderer';
import type { Config } from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

// This runs in Node.js - Don't use client-side code here (browser APIs, JSX...)

const config: Config = {
  title: 'CocoIndex',
  tagline: 'Indexing infra for AI with exceptional velocity',
  favicon: 'img/favicon.ico',

  // Set the production url of your site here
  url: 'https://cocoindex.io',
  // Set the /<baseUrl>/ pathname under which your site is served
  // For GitHub pages deployment, it is often '/<projectName>/'
  baseUrl: '/docs/',

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'cocoindex-io', // Usually your GitHub org/user name.
  projectName: 'docs', // Usually your repo name.
  trailingSlash: false,

  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',

  // Even if you don't use internationalization, you can use this field to set
  // useful metadata like html lang. For example, if your site is Chinese, you
  // may want to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  markdown: {
    mermaid: true,
  },

  plugins: [
    () => ({
      name: 'load-env-vars',
      configureWebpack: () => ({
        mergeStrategy: { plugins: "append", resolve: "merge" },
        plugins: [
          new webpack.DefinePlugin({
            'process.env.COCOINDEX_DOCS_MIXPANEL_API_KEY': JSON.stringify(process.env.COCOINDEX_DOCS_MIXPANEL_API_KEY),
          })
        ],
      }),
    }),
    [
      '@docusaurus/plugin-client-redirects',
      {
        redirects: [
          {
            from: '/core/initialization',
            to: '/core/settings',
          },
          {
            from: '/ops/storages',
            to: '/ops/targets',
          },
        ],
      },
    ],
  ],

  presets: [
    [
      'classic',
      {
        docs: {
          routeBasePath: '/',
          sidebarPath: './sidebars.ts',
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          editUrl: 'https://github.com/cocoindex-io/cocoindex/tree/main/docs',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themes: ['@docusaurus/theme-mermaid'],
  themeConfig: {
    // Replace with your project's social card
    image: 'img/social-card.jpg',
    metadata: [{ name: 'description', content: 'Official documentation for CocoIndex - Learn how to use CocoIndex to build robust data indexing pipelines for AI applications. Comprehensive guides, API references, and best practices for implementing efficient data processing workflows.' }],
    colorMode: {
      defaultMode: 'light',
      disableSwitch: false,
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'CocoIndex',
      logo: {
        alt: 'CocoIndex Logo',
        src: 'img/icon.svg',
        href: 'https://cocoindex.io',
        target: '_self' // This makes the logo click follow the link in the same window
      },
      items: [
        { to: '/docs/', label: 'Documentation', position: 'left', target: '_self' },
        { to: 'https://cocoindex.io/blogs/', label: 'Blog', position: 'left', target: '_self' },
        {
          type: 'html',
          position: 'right',
          value: '<iframe src="https://ghbtns.com/github-btn.html?user=cocoindex-io&repo=cocoindex&type=star&count=true" frameborder="0" scrolling="0" width="120" height="20" title="GitHub" style="vertical-align: middle;"></iframe>',
          className: 'navbar-github-link',
        },
      ],
    },
    footer: {
      style: 'light',
      links: [
        {
          title: 'CocoIndex',
          items: [
            {
              label: 'support@cocoindex.io',
              href: 'mailto:support@cocoindex.io',
            },
          ],
        },
        {
          title: 'Resources',
          items: [
            {
              label: 'Blog',
              to: 'https://cocoindex.io/blogs',
              target: '_self',
            },
            {
              label: 'Documentation',
              to: 'https://cocoindex.io/docs',
              target: '_self',
            },
            {
              label: 'YouTube',
              href: 'https://www.youtube.com/@cocoindex-io',
            },
          ],
        },
        {
          title: 'Community',
          items: [
            {
              label: 'GitHub',
              href: 'https://github.com/cocoindex-io/cocoindex',
            },
            {
              label: 'Discord Community',
              href: 'https://discord.com/invite/zpA9S2DR7s',
            },
            {
              label: 'Twitter',
              href: 'https://x.com/cocoindex_io',
            },
            {
              label: 'LinkedIn',
              href: 'https://www.linkedin.com/company/cocoindex/about/',
            },
          ],
        },
      ],
      copyright: `© ${new Date().getFullYear()} CocoIndex. All rights reserved.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['diff', 'json', 'bash', 'docker'],
    },
  } satisfies Preset.ThemeConfig,
};


if (!!process.env.COCOINDEX_DOCS_POSTHOG_API_KEY) {
  config.plugins.push([
    "posthog-docusaurus",
    {
      apiKey: process.env.COCOINDEX_DOCS_POSTHOG_API_KEY,
      appUrl: "https://us.i.posthog.com",
      enableInDevelopment: false,
    },
  ]);
}


if (!!process.env.COCOINDEX_DOCS_ALGOLIA_API_KEY && !!process.env.COCOINDEX_DOCS_ALGOLIA_APP_ID) {
  config.themeConfig.algolia = {
    appId: process.env.COCOINDEX_DOCS_ALGOLIA_APP_ID,
    apiKey: process.env.COCOINDEX_DOCS_ALGOLIA_API_KEY,
    indexName: 'cocoindex',
    contextualSearch: true,
    searchPagePath: 'search',
  };
}

export default config;

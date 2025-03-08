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
  themes: ['@docusaurus/theme-mermaid'],
  // In order for Mermaid code blocks in Markdown to work,
  // you also need to enable the Remark plugin with this option
  markdown: {
    mermaid: true,
  },

  // Even if you don't use internationalization, you can use this field to set
  // useful metadata like html lang. For example, if your site is Chinese, you
  // may want to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  plugins: [
    [
      "posthog-docusaurus",
      {
        apiKey: "phc_SgKiQafwZjHu4jQW2q402gbz6FYQ2NJRkcgooZMNNcy",
        appUrl: "https://us.i.posthog.com",
        enableInDevelopment: false,
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

  themeConfig: {
    // Replace with your project's social card
    image: 'img/social-card.jpg',
    metadata: [{ name: 'description', content: 'Official documentation for CocoIndex - Learn how to use CocoIndex to build robust data indexing pipelines for AI applications. Comprehensive guides, API references, and best practices for implementing efficient data processing workflows.' }],
    navbar: {
      title: 'CocoIndex',
      logo: {
        alt: 'CocoIndex Logo',
        src: 'img/icon.svg',
        href: 'https://cocoindex.io',
        target: '_self' // This makes the logo click follow the link in the same window
      },
      items: [
        { to: '/docs/', label: 'Documentation', position: 'right', target: '_self' },
        { to: 'https://cocoindex.io/blogs/', label: 'Blog', position: 'right', target: '_self' },
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

export default config;

import type { SidebarsConfig } from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  tutorialSidebar: [
    {
      type: 'category',
      label: 'Getting Started',
      collapsed: false,
      items: [
        'getting_started/overview',
        'getting_started/quickstart',
        'getting_started/installation',
      ],
    },
    {
      type: 'category',
      label: 'CocoIndex Core',
      collapsed: false,
      items: [
        'core/basics',
        'core/data_types',
        'core/initialization',
        'core/flow_def',
        'core/flow_methods',
        'core/cli',
        'core/custom_function',
      ],
    },
    {
      type: 'category',
      label: 'Built-in Operations',
      collapsed: false,
      items: [
        'ops/sources',
        'ops/functions',
        'ops/storages',
      ],
    },
    {
      type: 'category',
      label: 'AI Support',
      collapsed: false,
      items: [
        'ai/llm',
      ],
    },
    {
      type: 'category',
      label: 'About',
      collapsed: false,
      items: [
        'about/community',
        'about/contributing',
      ],
    },
  ],
};

export default sidebars;
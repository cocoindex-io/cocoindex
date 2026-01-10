import type { SidebarsConfig } from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  docs: [
    {
      type: 'category',
      label: 'Getting Started',
      collapsed: false,
      items: [
        'getting_started/overview',
        {
          type: 'doc',
          id: 'getting_started/concept',
          label: 'Concept',
        },
        'getting_started/quickstart',
        'getting_started/installation',
      ],
    },
    {
      type: 'category',
      label: 'CocoIndex Core',
      collapsed: false,
      items: [
        'core/cli',
        'core/memoization_keys',
      ],
    },
    {
      type: 'category',
      label: 'Built-in Connectors',
      collapsed: false,
      items: [
        'built_in_connectors/localfile',
        'built_in_connectors/postgres',
      ],
    },
    {
      type: 'category',
      label: 'Custom Connectors',
      link: { type: 'doc', id: 'custom_connectors/index' },
      collapsed: false,
      items: [],
    },
    {
      type: 'category',
      label: 'Extra Utilities',
      link: { type: 'doc', id: 'extras/index' },
      collapsed: false,
      items: [
        'extras/sentence-transformers',
        'extras/text',
      ],
    },
    {
      type: 'category',
      label: 'Contributing',
      collapsed: false,
      items: [
        'contributing/setup_dev_environment',
        'contributing/guide',
        'contributing/new_built_in_target',
      ],
    },
    {
      type: 'category',
      label: 'About',
      collapsed: false,
      items: [
        'about/community',
      ],
    },
  ],

};

export default sidebars;

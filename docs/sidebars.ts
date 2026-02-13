import type { SidebarsConfig } from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  docs: [
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
      label: 'Programming Guide',
      collapsed: false,
      items: [
        'programming_guide/core_concepts',
        'programming_guide/sdk_overview',
        'programming_guide/processing_component',
        'programming_guide/target_state',
        'programming_guide/function',
        'programming_guide/processing_helpers',
        'programming_guide/app',
        'programming_guide/context',
      ],
    },
    {
      type: 'doc',
      id: 'resource_types',
      label: 'Common Resource Types',
    },
    {
      type: 'category',
      label: 'Connectors',
      collapsed: false,
      items: [
        'connectors/lancedb',
        'connectors/localfs',
        'connectors/postgres',
        'connectors/qdrant',
        'connectors/sqlite',
      ],
    },
    {
      type: 'category',
      label: 'Built-in Operations',
      collapsed: false,
      items: [
        'ops/litellm',
        'ops/sentence_transformers',
        'ops/text',
      ],
    },
    {
      type: 'category',
      label: 'Advanced Topics',
      collapsed: false,
      items: [
        'advanced_topics/memoization_keys',
        'advanced_topics/multiple_environments',
        'advanced_topics/custom_target_connector',
      ],
    },
    {
      type: 'doc',
      id: 'cli',
      label: 'CLI Reference',
    },
    {
      type: 'category',
      label: 'Contributing',
      collapsed: false,
      items: [
        'contributing/setup_dev_environment',
        'contributing/guide',
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

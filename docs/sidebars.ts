import type { SidebarsConfig } from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  docs: [
    {
      type: 'category',
      label: 'Getting Started',
      collapsed: false,
      items: [
        'getting_started/overview',
        'getting_started/installation',
        'getting_started/quickstart',
      ],
    },
    {
      type: 'doc',
      id: 'programming_guide/core_concepts',
      label: 'Core Concepts',
    },
    {
      type: 'category',
      label: 'Programming Guide',
      collapsed: false,
      items: [
        'programming_guide/app',
        'programming_guide/function',
        'programming_guide/processing_component',
        'programming_guide/target_state',
        'programming_guide/context',
        'programming_guide/live_mode',
        'programming_guide/serialization',
        'programming_guide/sdk_overview',
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
        'connectors/amazon_s3',
        'connectors/kafka',
        'connectors/lancedb',
        'connectors/localfs',
        'connectors/postgres',
        'connectors/qdrant',
        'connectors/sqlite',
        'connectors/surrealdb',
      ],
    },
    {
      type: 'category',
      label: 'Built-in Operations',
      collapsed: false,
      items: [
        'ops/entity_resolution',
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
        {
          type: 'doc',
          id: 'advanced_topics/exception_handlers',
          label: 'Error Handling',
        },
        'advanced_topics/internal_storage',
        'advanced_topics/multiple_environments',
        'advanced_topics/live_component',
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

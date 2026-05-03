// V1 docs sidebar — mirrors ../docs/sidebars.ts from the Docusaurus source
// on the v1 branch. Hand-maintained; update alongside `npm run port` when
// the upstream tree changes. `slug` values must match Astro's URL shape
// for each file under src/content/docs/ (no extension; `/index` stripped).

export interface SidebarDoc {
  type: 'doc';
  slug: string;
  label?: string;
}

export interface SidebarCategory {
  type: 'category';
  label: string;
  /** Optional doc the category title links to (e.g. sources/index). */
  slug?: string;
  items: SidebarItem[];
}

export type SidebarItem = SidebarDoc | SidebarCategory;

export const sidebar: SidebarItem[] = [
  {
    type: 'category',
    label: 'Getting Started',
    items: [
      { type: 'doc', slug: 'getting_started/overview', label: 'Overview' },
      { type: 'doc', slug: 'getting_started/installation', label: 'Installation' },
      { type: 'doc', slug: 'getting_started/quickstart', label: 'Quickstart' },
      { type: 'doc', slug: 'getting_started/ai_coding_agents', label: 'Use with AI coding agents' },
    ],
  },
  { type: 'doc', slug: 'programming_guide/core_concepts', label: 'Core Concepts' },
  {
    type: 'category',
    label: 'Programming Guide',
    items: [
      { type: 'doc', slug: 'programming_guide/app', label: 'App' },
      { type: 'doc', slug: 'programming_guide/target_state', label: 'Target state' },
      { type: 'doc', slug: 'programming_guide/processing_component', label: 'Processing component' },
      { type: 'doc', slug: 'programming_guide/function', label: 'Functions' },
      { type: 'doc', slug: 'programming_guide/context', label: 'Context' },
      { type: 'doc', slug: 'programming_guide/live_mode', label: 'Live mode' },
      { type: 'doc', slug: 'programming_guide/serialization', label: 'Serialization' },
      { type: 'doc', slug: 'programming_guide/sdk_overview', label: 'SDK overview' },
    ],
  },
  {
    type: 'category',
    label: 'Common Resources',
    items: [
      { type: 'doc', slug: 'common_resources/data_types', label: 'Data types' },
      { type: 'doc', slug: 'common_resources/vector_schema', label: 'Vector schema' },
      { type: 'doc', slug: 'common_resources/id_generation', label: 'ID generation' },
    ],
  },
  {
    type: 'category',
    label: 'Connectors',
    items: [
      { type: 'doc', slug: 'connectors/amazon_s3', label: 'Amazon S3' },
      { type: 'doc', slug: 'connectors/doris', label: 'Apache Doris' },
      { type: 'doc', slug: 'connectors/falkordb', label: 'FalkorDB' },
      { type: 'doc', slug: 'connectors/google_drive', label: 'Google Drive' },
      { type: 'doc', slug: 'connectors/kafka', label: 'Kafka' },
      { type: 'doc', slug: 'connectors/lancedb', label: 'LanceDB' },
      { type: 'doc', slug: 'connectors/localfs', label: 'Local filesystem' },
      { type: 'doc', slug: 'connectors/neo4j', label: 'Neo4j' },
      { type: 'doc', slug: 'connectors/oci_object_storage', label: 'OCI Object Storage' },
      { type: 'doc', slug: 'connectors/postgres', label: 'Postgres' },
      { type: 'doc', slug: 'connectors/qdrant', label: 'Qdrant' },
      { type: 'doc', slug: 'connectors/sqlite', label: 'SQLite' },
      { type: 'doc', slug: 'connectors/surrealdb', label: 'SurrealDB' },
    ],
  },
  {
    type: 'category',
    label: 'Built-in Operations',
    items: [
      { type: 'doc', slug: 'ops/entity_resolution', label: 'Entity resolution' },
      { type: 'doc', slug: 'ops/litellm', label: 'LiteLLM' },
      { type: 'doc', slug: 'ops/sentence_transformers', label: 'Sentence transformers' },
      { type: 'doc', slug: 'ops/text', label: 'Text ops' },
    ],
  },
  {
    type: 'category',
    label: 'Advanced Topics',
    items: [
      { type: 'doc', slug: 'advanced_topics/concurrency_control', label: 'Concurrency control' },
      { type: 'doc', slug: 'advanced_topics/memoization_keys', label: 'Memoization keys' },
      { type: 'doc', slug: 'advanced_topics/exception_handlers', label: 'Error handling' },
      { type: 'doc', slug: 'advanced_topics/internal_storage', label: 'Internal storage' },
      { type: 'doc', slug: 'advanced_topics/multiple_environments', label: 'Multiple environments' },
      { type: 'doc', slug: 'advanced_topics/live_component', label: 'Live components' },
      { type: 'doc', slug: 'advanced_topics/custom_target_connector', label: 'Custom target connector' },
    ],
  },
  { type: 'doc', slug: 'cli', label: 'CLI Reference' },
  { type: 'doc', slug: 'faq', label: 'FAQ' },
  {
    type: 'category',
    label: 'Contributing',
    items: [
      { type: 'doc', slug: 'contributing/setup_dev_environment', label: 'Setup dev environment' },
      { type: 'doc', slug: 'contributing/guide', label: 'Contributing guide' },
    ],
  },
  {
    type: 'category',
    label: 'About',
    items: [
      { type: 'doc', slug: 'about/community', label: 'Community' },
      { type: 'doc', slug: 'about/telemetry', label: 'Anonymous usage telemetry' },
    ],
  },
];

// V1 is a fresh URL namespace; no legacy Docusaurus redirects carried over.
export const redirects: Record<string, string> = {};

// Flatten for prev/next pager.
export function flatten(items: SidebarItem[] = sidebar): Array<{ slug: string; label?: string }> {
  const out: Array<{ slug: string; label?: string }> = [];
  const visit = (list: SidebarItem[]) => {
    for (const item of list) {
      if (item.type === 'doc') {
        out.push({ slug: item.slug, label: item.label });
      } else {
        if (item.slug) out.push({ slug: item.slug, label: item.label });
        visit(item.items);
      }
    }
  };
  visit(items);
  return out;
}

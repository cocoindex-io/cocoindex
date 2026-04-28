// Docs sidebar — mirrors ../docs/sidebars.ts in the Docusaurus source.
// Hand-maintained; update alongside `npm run port` when upstream changes
// the tree. `slug` must match the URL shape Astro's content collection
// produces for a file under src/content/docs/ (e.g. `sources/index.md` →
// slug `sources`).

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
      { type: 'doc', slug: 'getting_started/quickstart', label: 'Quickstart' },
      { type: 'doc', slug: 'getting_started/installation', label: 'Installation' },
    ],
  },
  {
    type: 'category',
    label: 'CocoIndex Core',
    items: [
      { type: 'doc', slug: 'core/basics', label: 'Indexing basics' },
      { type: 'doc', slug: 'core/data_types', label: 'Data types' },
      { type: 'doc', slug: 'core/flow_def', label: 'Flow definition' },
      { type: 'doc', slug: 'core/settings', label: 'Settings' },
      { type: 'doc', slug: 'core/flow_methods', label: 'Flow methods' },
      { type: 'doc', slug: 'core/cli', label: 'CLI' },
    ],
  },
  {
    type: 'category',
    label: 'Tutorials',
    items: [
      { type: 'doc', slug: 'tutorials/control_flow', label: 'Concurrency control' },
      { type: 'doc', slug: 'tutorials/docker_pgvector_setup', label: 'Docker + pgvector setup' },
      { type: 'doc', slug: 'tutorials/live_updates', label: 'Live updates' },
      { type: 'doc', slug: 'tutorials/manage_flow_dynamically', label: 'Manage flows dynamically' },
    ],
  },
  { type: 'doc', slug: 'query', label: 'Query Support' },
  {
    type: 'category',
    label: 'Built-in Sources',
    slug: 'sources',
    items: [
      { type: 'doc', slug: 'sources/amazons3', label: 'Amazon S3' },
      { type: 'doc', slug: 'sources/azureblob', label: 'Azure Blob' },
      { type: 'doc', slug: 'sources/googledrive', label: 'Google Drive' },
      { type: 'doc', slug: 'sources/localfile', label: 'LocalFile' },
      { type: 'doc', slug: 'sources/postgres', label: 'Postgres' },
    ],
  },
  { type: 'doc', slug: 'ops/functions', label: 'Built-in Functions' },
  {
    type: 'category',
    label: 'Built-in Targets',
    slug: 'targets',
    items: [
      { type: 'doc', slug: 'targets/postgres', label: 'Postgres (pgvector)' },
      { type: 'doc', slug: 'targets/qdrant', label: 'Qdrant' },
      { type: 'doc', slug: 'targets/pinecone', label: 'Pinecone' },
      { type: 'doc', slug: 'targets/lancedb', label: 'LanceDB' },
      { type: 'doc', slug: 'targets/chromadb', label: 'ChromaDB' },
      { type: 'doc', slug: 'targets/neo4j', label: 'Neo4j' },
      { type: 'doc', slug: 'targets/ladybug', label: 'Ladybug' },
      { type: 'doc', slug: 'targets/kuzu', label: 'Kuzu' },
      { type: 'doc', slug: 'targets/doris', label: 'Doris' },
    ],
  },
  {
    type: 'category',
    label: 'Custom Operations',
    items: [
      { type: 'doc', slug: 'custom_ops/custom_sources', label: 'Custom sources' },
      { type: 'doc', slug: 'custom_ops/custom_functions', label: 'Custom functions' },
      { type: 'doc', slug: 'custom_ops/custom_targets', label: 'Custom targets' },
    ],
  },
  {
    type: 'category',
    label: 'AI Support',
    items: [{ type: 'doc', slug: 'ai/llm', label: 'LLM' }],
  },
  {
    type: 'category',
    label: 'CocoInsight',
    items: [
      { type: 'doc', slug: 'cocoinsight_access', label: 'CocoInsight access' },
    ],
  },
  {
    type: 'category',
    label: 'Contributing',
    items: [
      { type: 'doc', slug: 'contributing/setup_dev_environment', label: 'Setup dev environment' },
      { type: 'doc', slug: 'contributing/guide', label: 'Contributing guide' },
      { type: 'doc', slug: 'contributing/new_built_in_target', label: 'New built-in target' },
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

// Legacy Docusaurus URLs → new slugs. Fed into astro.config.mjs `redirects`.
// Astro expects entries keyed by full path (`base` is auto-applied).
export const redirects: Record<string, string> = {
  '/core/initialization': '/core/settings',
  '/core/custom_function': '/custom_ops/custom_functions',
  '/ops/storages': '/targets',
  '/about/contributing': '/contributing/guide',
  '/ops/targets': '/targets',
  '/ops/sources': '/sources',
  '/http_server': '/cocoinsight_access',
};

// Flatten the tree for prev/next pager computation.
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

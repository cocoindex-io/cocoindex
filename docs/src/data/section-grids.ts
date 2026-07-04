// Card data for the section overview pages (programming_guide,
// common_resources, advanced_topics). Rendered by @cocoindex/brand's
// DocCardGrid, imported directly in each section's index.mdx. Hrefs are
// absolute (`/docs/...`) so they work both from the rendered page and
// anywhere the data is reused.
import type { Props as DocCardGridProps } from '@cocoindex/brand/DocCardGrid.astro';

type SectionGridGroups = DocCardGridProps['groups'];

export const programmingGuideGroups: SectionGridGroups = [
  {
    label: 'Core building blocks',
    blurb: 'The pieces you compose into a pipeline, from the App down to a single function.',
    accent: 'coral',
    cards: [
      {
        href: '/docs/programming_guide/app/',
        title: 'App',
        glyph: 'app',
        body: 'The top-level runnable unit: create one, trigger updates, set the db path, and manage lifespan resources.',
      },
      {
        href: '/docs/programming_guide/target_state/',
        title: 'Target state',
        glyph: 'target',
        body: 'Declare what should exist in an external system and let CocoIndex sync minimal creates, updates, and deletes.',
      },
      {
        href: '/docs/programming_guide/processing_component/',
        title: 'Processing component',
        glyph: 'component',
        body: 'The unit of incremental execution and the sync boundary for target states. Covers the mounting APIs and paths.',
      },
      {
        href: '/docs/programming_guide/function/',
        title: 'Functions',
        glyph: 'function',
        body: 'Decorate Python with @coco.fn so it joins change detection and memoization, with async, batching, and GPU runners.',
      },
    ],
  },
  {
    label: 'Resources & runtime',
    blurb: 'Share what components need, and keep the pipeline reacting after the first run.',
    accent: 'palm',
    cards: [
      {
        href: '/docs/programming_guide/context/',
        title: 'Context',
        glyph: 'context',
        body: 'Share connections, models, and config across components with ContextKey, provide(), and use_context().',
      },
      {
        href: '/docs/programming_guide/live_mode/',
        title: 'Live mode',
        glyph: 'livemode',
        body: 'Keep an App running after its first sweep so it reacts to source changes continuously.',
      },
    ],
  },
  {
    label: 'Under the hood',
    blurb: 'The mechanics underneath: how values are stored, and how the SDK is laid out.',
    accent: 'peach',
    cards: [
      {
        href: '/docs/programming_guide/serialization/',
        title: 'Serialization',
        glyph: 'serialization',
        body: 'How memoized returns serialize with msgspec: supported types, required annotations, and custom registration.',
      },
      {
        href: '/docs/programming_guide/sdk_overview/',
        title: 'SDK overview',
        glyph: 'sdk',
        body: 'A tour of the Python SDK: package layout, common types, and how async orchestration composes with sync leaves.',
      },
    ],
  },
];

export const commonResourcesGroups: SectionGridGroup[] = [
  {
    label: 'Types & schema',
    blurb: 'The exact shapes connectors and operations exchange.',
    accent: 'coral',
    cards: [
      {
        href: '/docs/common_resources/data_types/',
        title: 'Data types',
        glyph: 'types',
        body: 'Shared models across connectors and ops: FileLike, FilePath, FilePathMatcher, Chunk, and the Embedder protocol.',
      },
      {
        href: '/docs/common_resources/vector_schema/',
        title: 'Vector schema',
        glyph: 'vector',
        body: 'Describe vector columns with VectorSchema and VectorSchemaProvider, including MultiVectorSchema for ColBERT-style models.',
      },
    ],
  },
  {
    label: 'Identity & state',
    blurb: 'Keep IDs stable and bridge producing and consuming logic.',
    accent: 'palm',
    cards: [
      {
        href: '/docs/common_resources/id_generation/',
        title: 'Stable ID generation',
        glyph: 'id',
        body: 'Generate IDs and UUIDs that stay stable across incremental runs, from pure derivation to stateful sequences.',
      },
      {
        href: '/docs/common_resources/live_map/',
        title: 'LiveMap',
        glyph: 'map',
        body: 'An in-memory keyed collection that bridges producing and consuming logic via target states and LiveMapView.',
      },
    ],
  },
];

export const advancedTopicsGroups: SectionGridGroup[] = [
  {
    label: 'Tuning & resources',
    blurb: 'Control how much work runs at once and how engine state lands on disk.',
    accent: 'coral',
    cards: [
      {
        href: '/docs/advanced_topics/concurrency_control/',
        title: 'Concurrency control',
        glyph: 'concurrency',
        body: 'Cap how many processing components run at once to protect rate-limited APIs and GPUs.',
      },
      {
        href: '/docs/advanced_topics/internal_storage/',
        title: 'Internal storage',
        glyph: 'storage',
        body: 'Tune the LMDB store behind target states and memo results: map size and max dbs.',
      },
    ],
  },
  {
    label: 'Reliability & observability',
    blurb: 'See what the engine is doing, and decide what happens when something breaks.',
    accent: 'palm',
    cards: [
      {
        href: '/docs/advanced_topics/exception_handlers/',
        title: 'Error handling',
        glyph: 'shield',
        body: 'Isolate component failures, recover interrupted updates, and observe background errors.',
      },
      {
        href: '/docs/advanced_topics/progress_monitoring/',
        title: 'Progress monitoring',
        glyph: 'progress',
        body: 'Read structured update stats beyond stdout, and split a run into reported scopes.',
      },
    ],
  },
  {
    label: 'Incremental engine',
    blurb: 'Go a level deeper on change detection and continuous, low-latency updates.',
    accent: 'peach',
    cards: [
      {
        href: '/docs/advanced_topics/memoization_keys/',
        title: 'Memoization keys & states',
        glyph: 'key',
        body: 'Customize how inputs are fingerprinted, and layer state validation like mtime or content hash.',
      },
      {
        href: '/docs/advanced_topics/live_component/',
        title: 'Live components',
        glyph: 'live',
        body: 'Implement process() and process_live() to push incremental updates from event sources.',
      },
    ],
  },
  {
    label: 'Extending & isolating',
    blurb: 'Reach beyond the built-ins and run independent environments side by side.',
    accent: 'gold',
    cards: [
      {
        href: '/docs/advanced_topics/custom_target_connector/',
        title: 'Custom target connector',
        glyph: 'connector',
        body: 'Wire the declarative target-state system into any external store you need to sync.',
      },
      {
        href: '/docs/advanced_topics/multiple_environments/',
        title: 'Multiple environments',
        glyph: 'layers',
        body: 'Run isolated environments side by side for multi-tenancy, library dev, and testing.',
      },
    ],
  },
];

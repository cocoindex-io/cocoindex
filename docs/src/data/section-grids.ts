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
    accent: 'coral',
    cards: [
      {
        href: '/docs/programming_guide/app/',
        title: 'App',
        glyph: 'app',
        body: 'Run your pipeline. An App starts the main function and manages updates and shared resources.',
      },
      {
        href: '/docs/programming_guide/target_state/',
        title: 'Target state',
        glyph: 'target',
        body: 'Describe the output you want, such as a file or database row. CocoIndex keeps that output up to date.',
      },
      {
        href: '/docs/programming_guide/processing_component/',
        title: 'Processing component',
        glyph: 'component',
        body: 'Run one part of a pipeline and own its target states. Learn how to create and identify components.',
      },
      {
        href: '/docs/programming_guide/function/',
        title: 'Functions',
        glyph: 'function',
        body: 'Write processing logic with @coco.fn. Enable memoization to reuse results when inputs and code stay unchanged.',
      },
    ],
  },
  {
    label: 'Resources & runtime',
    blurb: 'Share resources and respond to source changes.',
    accent: 'palm',
    cards: [
      {
        href: '/docs/programming_guide/context/',
        title: 'Context',
        glyph: 'context',
        body: 'Share database connections, models, and configuration across processing components.',
      },
      {
        href: '/docs/programming_guide/live_mode/',
        title: 'Live mode',
        glyph: 'livemode',
        body: 'Keep an App running so it processes source changes after the first update.',
      },
    ],
  },
  {
    label: 'Storage and SDK',
    blurb: 'Learn how CocoIndex stores results and organizes its Python API.',
    accent: 'peach',
    cards: [
      {
        href: '/docs/programming_guide/serialization/',
        title: 'Serialization',
        glyph: 'serialization',
        body: 'Store function results for reuse. Learn which Python types CocoIndex supports and how to register custom types.',
      },
      {
        href: '/docs/programming_guide/sdk_overview/',
        title: 'SDK overview',
        glyph: 'sdk',
        body: 'Find Python packages and common types. Learn how synchronous and asynchronous functions work together.',
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
  {
    label: 'Runtime',
    blurb: 'Control how a pipeline talks to external services.',
    accent: 'gold',
    cards: [
      {
        href: '/docs/common_resources/rate_limiting/',
        title: 'Rate limiting',
        glyph: 'shield',
        body: 'A token-bucket RateLimiter that throttles outbound work, shared by reference so concurrent components run under one budget.',
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
        href: '/docs/advanced_topics/timeouts/',
        title: 'Timeouts',
        glyph: 'progress',
        body: 'Put a cooperative deadline around foreground CocoIndex work without cancelling shared writes or background components.',
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

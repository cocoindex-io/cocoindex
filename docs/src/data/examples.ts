// Metadata for the /docs/examples listing and per-example pages.
//
// Data is mirrored from github.com/cocoindex-io/examples-v1 — the markdown
// bodies live in src/content/example-posts/<slug>.md and are rendered by
// src/pages/examples/[slug].astro beneath the shared hero. Titles may use
// *asterisks* to mark the italic-coral accent — see consts.titleMarkup.
//
// The v1 examples repo currently ships four walkthroughs; this file grows
// as more land there.

export type Category = 'search' | 'ingest' | 'llm' | 'agents' | 'image';

export const CATEGORY_META: Record<Category, { label: string; em?: string; lead: string; thumbClass: string }> = {
  search: { label: 'Vector ', em: 'Indexes', lead: 'Embed your documents, store vectors, answer by meaning.', thumbClass: 'search' },
  ingest: { label: 'Custom ', em: 'Building Blocks', lead: 'Bring your own source, target, or parser. Same declarative flow.', thumbClass: 'ingest' },
  llm: { label: 'Structured ', em: 'Extraction', lead: 'Turn loose prose into structured data with LLMs, BAML, DSPy, or Ollama.', thumbClass: 'llm' },
  agents: { label: 'Knowledge ', em: 'Graphs', lead: 'Give agents a persistent, graph-shaped memory from conversations, meetings, products.', thumbClass: 'agents' },
  image: { label: 'Multimodal', lead: 'Images, PDFs, slides, faces — same flow, different encoder.', thumbClass: 'pink' },
};

export type ExampleCard = {
  slug: string;                      // becomes /docs/examples/<slug>
  title: string;                     // asterisks → italic-coral
  index: string;                     // e.g. '01 / 02' — shown top-right of thumb
  category: Category;
  thumbClass?: string;
  thumbLabel: string;
  motif?: string;                    // raw inline SVG markup for the thumb
  description: string;
  tags: Array<{ kind: 'src' | 'tgt' | 'llm' | 'ops' | 'lvl'; label: string }>;
  footMeta: string;
  sourceSlug?: string;               // override GitHub path when the listing slug differs from the repo dir
  featured?: boolean;
};

const MOTIFS = {
  repos: `<svg viewBox="0 0 120 70" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="14" y="18" width="20" height="34" rx="2"/><rect x="40" y="18" width="20" height="34" rx="2"/><rect x="66" y="18" width="20" height="34" rx="2"/><path d="M92 35 L106 35 M98 30 L106 35 L98 40" stroke-linecap="round" stroke-linejoin="round"/></svg>`,
  pdfToMd: `<svg viewBox="0 0 120 70" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="18" y="14" width="30" height="42" rx="2"/><path d="M54 35 L70 35 M62 29 L70 35 L62 41" stroke-linecap="round" stroke-linejoin="round"/><rect x="74" y="14" width="30" height="42" rx="2"/><path d="M80 24 L98 24 M80 32 L94 32 M80 40 L98 40 M80 48 L88 48"/></svg>`,
  codeChunks: `<svg viewBox="0 0 120 70" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="14" y="12" width="34" height="46" rx="2"/><path d="M20 22 L42 22 M20 30 L36 30 M20 38 L42 38 M20 46 L32 46" stroke-width="1.2"/><path d="M54 24 L70 24 M62 18 L70 24 L62 30" stroke-linecap="round" stroke-linejoin="round"/><path d="M54 46 L70 46 M62 40 L70 46 L62 52" stroke-linecap="round" stroke-linejoin="round"/><rect x="76" y="14" width="30" height="18" rx="2" fill="currentColor" opacity="0.14"/><rect x="76" y="38" width="30" height="18" rx="2" fill="currentColor" opacity="0.14"/><circle cx="91" cy="23" r="2.5" fill="currentColor"/><circle cx="91" cy="47" r="2.5" fill="currentColor"/></svg>`,
  graph: `<svg viewBox="0 0 120 70" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M34 20 L58 36 M86 19 L64 36 M34 50 L56 41 M86 51 L66 40 M36 19 L84 18"/><circle cx="30" cy="18" r="6"/><circle cx="90" cy="17" r="6"/><circle cx="28" cy="52" r="6"/><circle cx="92" cy="52" r="6"/><circle cx="61" cy="38" r="8" fill="currentColor" opacity="0.14"/><circle cx="61" cy="38" r="8"/></svg>`,
} as const;

export const examples: ExampleCard[] = [
  {
    slug: 'index-codebase',
    title: 'Index Your *Codebase*',
    index: '01 / 04',
    category: 'search',
    thumbLabel: 'Code · Tree-sitter',
    motif: MOTIFS.codeChunks,
    description: 'Walk a repo, split by syntax with Tree-sitter, embed, and query your codebase in English. A live vector index for AI coding agents, in ~100 lines.',
    tags: [
      { kind: 'src', label: 'Local FS' },
      { kind: 'tgt', label: 'Postgres' },
      { kind: 'ops', label: 'Tree-sitter' },
      { kind: 'lvl', label: 'Starter' },
    ],
    footMeta: '~10 min · starter',
    sourceSlug: 'code_embedding',
  },
  {
    slug: 'multi-codebase-summarization',
    title: 'Multi-codebase *Summarization*',
    index: '02 / 04',
    category: 'llm',
    thumbLabel: 'Code · LLM summaries',
    motif: MOTIFS.repos,
    description: 'Walk many Python repos, chunk by syntax, ask an LLM to write a searchable summary per file. The flagship v1 walkthrough.',
    tags: [
      { kind: 'src', label: 'Multi-repo' },
      { kind: 'llm', label: 'Any LLM' },
      { kind: 'lvl', label: 'Advanced' },
    ],
    footMeta: '~35 min · featured',
    sourceSlug: 'multi_codebase_summarization',
    featured: true,
  },
  {
    slug: 'pdf-to-markdown',
    title: 'PDF → *Markdown*',
    index: '03 / 04',
    category: 'ingest',
    thumbLabel: 'PDF · custom blocks',
    motif: MOTIFS.pdfToMd,
    description: 'Incremental PDF → Markdown conversion pipeline. Custom building blocks over a folder of PDFs.',
    tags: [
      { kind: 'src', label: 'PDF' },
      { kind: 'tgt', label: 'Local FS' },
      { kind: 'ops', label: 'Custom blocks' },
    ],
    footMeta: '~18 min',
    sourceSlug: 'pdf_to_markdown',
  },
  {
    slug: 'podcast-to-knowledge-graph',
    title: 'Podcasts → *Knowledge Graph*',
    index: '04 / 04',
    category: 'agents',
    thumbLabel: 'YouTube · LLM + graph',
    motif: MOTIFS.graph,
    description: 'Turn YouTube podcasts into a queryable knowledge graph — diarized transcription, two-step LLM extraction, embedding-based entity resolution, and a SurrealDB graph.',
    tags: [
      { kind: 'src', label: 'YouTube' },
      { kind: 'llm', label: 'OpenAI' },
      { kind: 'ops', label: 'Entity resolution' },
      { kind: 'tgt', label: 'SurrealDB' },
      { kind: 'lvl', label: 'Advanced' },
    ],
    footMeta: '~40 min · advanced',
    sourceSlug: 'conversation_to_knowledge',
  },
];

export const featuredSlug = 'multi-codebase-summarization';

// Featured examples still show up in their home category grid — when the
// catalog is small, hiding them leaves categories visibly empty.
export const byCategory = (cat: Category): ExampleCard[] =>
  examples.filter((e) => e.category === cat);

export const findExample = (slug: string): ExampleCard | undefined =>
  examples.find((e) => e.slug === slug);

// Sidebar facets shown on the listing. Short in v1 — expands as more
// examples land.
export const SIDEBAR_TARGETS = ['Local FS', 'Postgres', 'SurrealDB'];
export const SIDEBAR_SOURCES = ['Local FS', 'PDF', 'Multi-repo', 'YouTube'];
export const SIDEBAR_LLMS = ['OpenAI', 'Gemini', 'Anthropic'];
export const POPULAR: Array<{ slug: string; label: string; count: string }> = [
  { slug: 'index-codebase', label: 'Index Your Codebase', count: '★' },
  { slug: 'multi-codebase-summarization', label: 'Multi-codebase Summarization', count: '★' },
  { slug: 'pdf-to-markdown', label: 'PDF → Markdown', count: '★' },
  { slug: 'podcast-to-knowledge-graph', label: 'Podcasts → Knowledge Graph', count: '★' },
];

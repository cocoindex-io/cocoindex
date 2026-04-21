// Metadata for the /docs/examples listing and per-example pages.
//
// Data is mirrored from github.com/cocoindex-io/examples — the markdown
// bodies live in src/content/example-posts/<slug>.md and are rendered by
// src/pages/examples/[slug].astro beneath the shared hero. Titles may use
// *asterisks* to mark the italic-coral accent — see consts.titleMarkup.

export type Category = 'search' | 'ingest' | 'llm' | 'agents' | 'image';

export const CATEGORY_META: Record<Category, { label: string; em?: string; lead: string; thumbClass: string }> = {
  search:  { label: 'Vector ',        em: 'Indexes',     lead: 'Embed your documents, store vectors, answer by meaning.',         thumbClass: 'search' },
  ingest:  { label: 'Custom ',        em: 'Building Blocks', lead: 'Bring your own source, target, or parser. Same declarative flow.', thumbClass: 'ingest' },
  llm:     { label: 'Structured ',    em: 'Extraction',  lead: 'Turn loose prose into structured data with LLMs, BAML, DSPy, or Ollama.', thumbClass: 'llm' },
  agents:  { label: 'Knowledge ',     em: 'Graphs',      lead: 'Give agents a persistent, graph-shaped memory from conversations, meetings, products.', thumbClass: 'agents' },
  image:   { label: 'Multimodal',                         lead: 'Images, PDFs, slides, faces — same flow, different encoder.',     thumbClass: 'pink' },
};

export type ExampleCard = {
  slug: string;                      // becomes /docs/examples/<slug>
  title: string;                     // asterisks → italic-coral, e.g. 'HN Trending *Topics*'
  index: string;                     // e.g. '07 / 20' — shown top-right of thumb
  category: Category;
  thumbClass?: string;               // override (e.g. agents shown as multimodal pink card)
  thumbLabel: string;                // small pill top-left of the thumb
  motif?: string;                    // raw inner SVG markup for the thumb illustration
  description: string;
  tags: Array<{ kind: 'src' | 'tgt' | 'llm' | 'ops' | 'lvl'; label: string }>;
  footMeta: string;                  // e.g. '~10 min · ~80 loc'
  sourceSlug?: string;               // override GitHub path when the slug differs from the repo dir
  featured?: boolean;
};

const MOTIFS = {
  nodeGraph: `<svg class="m-node" viewBox="0 0 120 70"><rect x="10" y="20" width="28" height="28" rx="4"/><rect x="82" y="6" width="28" height="28" rx="4"/><rect x="82" y="34" width="28" height="28" rx="4"/><path d="M38 34 L82 20 M38 34 L82 48" stroke="currentColor" stroke-width="1.5" fill="none"/><circle cx="24" cy="34" r="3"/><circle cx="96" cy="20" r="3"/><circle cx="96" cy="48" r="3"/></svg>`,
  dots: `<svg class="m-dots" viewBox="0 0 120 70"><circle cx="16" cy="20" r="4"/><circle cx="36" cy="20" r="4"/><circle cx="56" cy="20" r="4"/><circle cx="76" cy="20" r="4" opacity="0.6"/><circle cx="96" cy="20" r="4" opacity="0.35"/><circle cx="16" cy="40" r="4"/><circle cx="36" cy="40" r="4"/><circle cx="56" cy="40" r="4" opacity="0.6"/><circle cx="16" cy="60" r="4"/><circle cx="36" cy="60" r="4" opacity="0.35"/></svg>`,
  pdf: `<svg viewBox="0 0 120 70" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="32" y="10" width="42" height="52" rx="2"/><path d="M32 20 L74 20 M40 30 L66 30 M40 38 L66 38 M40 46 L56 46"/><rect x="78" y="14" width="22" height="28" rx="1" opacity="0.5"/><circle cx="89" cy="56" r="8" fill="currentColor"/></svg>`,
  hnArrow: `<svg viewBox="0 0 120 70" fill="none"><rect x="36" y="14" width="48" height="42" fill="currentColor" opacity="0.12"/><path d="M46 26 L60 40 L60 48 M60 40 L74 26" stroke="currentColor" stroke-width="2.4" fill="none" stroke-linecap="round" stroke-linejoin="round"/></svg>`,
  image: `<svg viewBox="0 0 120 70" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="22" y="14" width="76" height="42" rx="3"/><circle cx="38" cy="26" r="4" fill="currentColor"/><path d="M22 46 L44 34 L64 44 L80 32 L98 40" stroke-linejoin="round"/></svg>`,
  postgres: `<svg viewBox="0 0 120 70" fill="none" stroke="currentColor" stroke-width="1.5"><ellipse cx="60" cy="18" rx="28" ry="8"/><path d="M32 18 L32 52 Q32 60 60 60 T88 52 L88 18 M32 35 Q32 43 60 43 T88 35"/></svg>`,
  filesTransform: `<svg viewBox="0 0 120 70" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="16" y="18" width="32" height="34" rx="2"/><path d="M52 35 L72 35 M64 29 L72 35 L64 41" stroke-linecap="round" stroke-linejoin="round"/><rect x="76" y="18" width="32" height="34" rx="2"/><path d="M22 28 L42 28 M22 36 L42 36 M82 28 L102 28 M82 36 L96 36 M82 44 L102 44"/></svg>`,
  forms: `<svg viewBox="0 0 120 70" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="28" y="12" width="40" height="50" rx="2"/><path d="M36 22 L60 22 M36 30 L60 30 M36 38 L54 38"/><rect x="72" y="22" width="28" height="40" rx="2" fill="currentColor" opacity="0.14" stroke="none"/><path d="M76 32 L96 32 M76 40 L96 40 M76 48 L90 48" stroke="currentColor" stroke-width="1.2"/></svg>`,
  cross: `<svg viewBox="0 0 120 70" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M60 18 L60 52 M43 35 L77 35" stroke-linecap="round"/><circle cx="60" cy="35" r="22"/></svg>`,
  dspy: `<svg viewBox="0 0 120 70" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="30" cy="35" r="10"/><circle cx="60" cy="20" r="8"/><circle cx="60" cy="50" r="8"/><circle cx="90" cy="35" r="10"/><path d="M40 35 L52 22 M40 35 L52 48 M68 22 L80 35 M68 48 L80 35"/></svg>`,
  slides: `<svg viewBox="0 0 120 70" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="14" y="12" width="22" height="46" rx="2"/><rect x="40" y="12" width="22" height="46" rx="2"/><rect x="66" y="12" width="22" height="46" rx="2"/><rect x="92" y="12" width="14" height="46" rx="2" opacity="0.5"/></svg>`,
  face: `<svg viewBox="0 0 120 70" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="60" cy="35" r="22"/><circle cx="52" cy="30" r="2" fill="currentColor"/><circle cx="68" cy="30" r="2" fill="currentColor"/><path d="M52 42 Q60 48 68 42" stroke-linecap="round"/></svg>`,
  knowledgeChat: `<svg viewBox="0 0 120 70" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="16" y="16" width="40" height="24" rx="4"/><path d="M22 44 L32 36 L50 36" stroke-linecap="round"/><rect x="64" y="30" width="40" height="24" rx="4" fill="currentColor" opacity="0.14" stroke="currentColor"/><path d="M70 58 L80 50 L98 50" stroke-linecap="round"/></svg>`,
  graphTree: `<svg viewBox="0 0 120 70" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="60" cy="16" r="6" fill="currentColor"/><circle cx="28" cy="42" r="6" fill="currentColor"/><circle cx="60" cy="42" r="6" fill="currentColor"/><circle cx="92" cy="42" r="6" fill="currentColor"/><circle cx="44" cy="60" r="4" fill="currentColor" opacity="0.6"/><circle cx="76" cy="60" r="4" fill="currentColor" opacity="0.6"/><path d="M60 22 L30 38 M60 22 L60 38 M60 22 L90 38 M32 46 L42 58 M60 46 L48 58 M60 46 L74 58 M88 46 L78 58"/></svg>`,
  boxArrows: `<svg viewBox="0 0 120 70" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="14" y="14" width="92" height="42" rx="3"/><path d="M14 26 L106 26 M14 38 L106 38 M50 14 L50 56 M78 14 L78 56"/></svg>`,
  hnStory: `<svg viewBox="0 0 120 70" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="20" y="14" width="80" height="42" rx="3"/><path d="M26 26 L86 26 M26 34 L72 34 M26 42 L80 42"/><circle cx="92" cy="34" r="3" fill="currentColor"/></svg>`,
  parser: `<svg viewBox="0 0 120 70" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="18" y="14" width="30" height="42" rx="2"/><path d="M24 24 L42 24 M24 32 L42 32 M24 40 L36 40"/><path d="M52 35 L68 35 M60 29 L68 35 L60 41" stroke-linecap="round" stroke-linejoin="round"/><rect x="72" y="14" width="30" height="42" rx="2" fill="currentColor" opacity="0.14"/><path d="M78 24 L96 24 M78 32 L96 32 M78 40 L90 40 M78 48 L96 48" stroke-width="1.2"/></svg>`,
  taxonomy: `<svg viewBox="0 0 120 70" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="48" y="10" width="24" height="14" rx="2"/><rect x="20" y="34" width="24" height="14" rx="2"/><rect x="48" y="34" width="24" height="14" rx="2"/><rect x="76" y="34" width="24" height="14" rx="2"/><path d="M60 24 L32 34 M60 24 L60 34 M60 24 L88 34"/><rect x="20" y="54" width="16" height="10" rx="1.5" opacity="0.6"/><rect x="52" y="54" width="16" height="10" rx="1.5" opacity="0.6"/></svg>`,
} as const;

// The full catalog. Order determines display sequence within each category.
export const examples: ExampleCard[] = [
  // ── Vector Indexes ──
  {
    slug: 'simple_vector_index',
    title: 'Simple Vector *Index*',
    index: '01 / 20',
    category: 'search',
    thumbLabel: 'Text · Postgres',
    motif: MOTIFS.dots,
    description: 'The cleanest "hello world" for CocoIndex + embeddings — index markdown, query it with natural language.',
    tags: [
      { kind: 'src', label: 'Local FS' },
      { kind: 'tgt', label: 'Postgres' },
      { kind: 'lvl', label: 'Starter' },
    ],
    footMeta: '~6 min · starter',
    sourceSlug: 'text_embedding',
  },
  {
    slug: 'code_index',
    title: 'Codebase *Indexing*',
    index: '02 / 20',
    category: 'search',
    thumbLabel: 'Code · Tree-sitter',
    motif: MOTIFS.nodeGraph,
    description: 'Walk a repo, split by syntax, embed, and query your codebase in English. Real-time RAG for code.',
    tags: [
      { kind: 'src', label: 'Local FS' },
      { kind: 'ops', label: 'Tree-sitter' },
      { kind: 'tgt', label: 'Postgres' },
    ],
    footMeta: '~10 min',
    sourceSlug: 'code_embedding',
  },
  {
    slug: 'academic_papers_index',
    title: 'Academic *Papers*',
    index: '03 / 20',
    category: 'search',
    thumbLabel: 'PDFs · metadata',
    motif: MOTIFS.pdf,
    description: 'Extract metadata, chunk and embed abstracts, enable semantic + author-based search over academic PDFs.',
    tags: [
      { kind: 'src', label: 'PDF' },
      { kind: 'llm', label: 'Any LLM' },
      { kind: 'tgt', label: 'Postgres' },
    ],
    footMeta: '~20 min',
    sourceSlug: 'paper_metadata',
  },

  // ── Custom Building Blocks ──
  {
    slug: 'postgres_source',
    title: 'Postgres as a *Source*',
    index: '04 / 20',
    category: 'ingest',
    thumbLabel: 'Postgres · CDC',
    motif: MOTIFS.postgres,
    description: 'Use an existing Postgres table as a CocoIndex source. AI transforms + data mappings flow into pgvector.',
    tags: [
      { kind: 'src', label: 'Postgres' },
      { kind: 'tgt', label: 'pgvector' },
      { kind: 'ops', label: 'Data mapping' },
    ],
    footMeta: '~12 min',
  },
  {
    slug: 'custom_source_hackernews',
    title: 'Custom Source *HN*',
    index: '05 / 20',
    category: 'ingest',
    thumbLabel: 'HN · Algolia API',
    motif: MOTIFS.hnStory,
    description: 'Treat any API as a first-class incremental source. A custom HN connector that stays in sync with Postgres.',
    tags: [
      { kind: 'src', label: 'HN API' },
      { kind: 'ops', label: 'Custom source' },
      { kind: 'tgt', label: 'Postgres' },
    ],
    footMeta: '~30 min',
    sourceSlug: 'custom_source_hn',
  },
  {
    slug: 'custom_targets',
    title: 'Custom *Targets*',
    index: '06 / 20',
    category: 'ingest',
    thumbLabel: 'Markdown → HTML',
    motif: MOTIFS.filesTransform,
    description: 'Export markdown files to local HTML using a custom target. The simplest file-to-file pipeline shape.',
    tags: [
      { kind: 'src', label: 'Local FS' },
      { kind: 'tgt', label: 'Local FS' },
      { kind: 'lvl', label: 'Starter' },
    ],
    footMeta: '~8 min',
    sourceSlug: 'custom_output_files',
  },

  // ── Structured Extraction (HN is featured) ──
  {
    slug: 'hackernews-trending-topics',
    title: 'HN Trending *Topics*',
    index: '07 / 20',
    category: 'llm',
    thumbLabel: '★ HackerNews',
    motif: MOTIFS.hnArrow,
    description: 'Custom source + LLM extraction + live SQL. The showcase multi-stage example with 92% fewer API calls after the first sync.',
    tags: [
      { kind: 'src', label: 'HN API' },
      { kind: 'llm', label: 'Gemini 2.5' },
      { kind: 'tgt', label: 'Postgres' },
      { kind: 'lvl', label: 'Advanced' },
    ],
    footMeta: '~45 min · featured',
    sourceSlug: 'hn_trending_topics',
    featured: true,
  },
  {
    slug: 'manual_extraction',
    title: 'Python Manual *Extraction*',
    index: '08 / 20',
    category: 'llm',
    thumbLabel: 'Ollama · local',
    motif: MOTIFS.forms,
    description: 'Extract structured data from the Python manual markdowns with a local Ollama model.',
    tags: [
      { kind: 'src', label: 'Markdown' },
      { kind: 'llm', label: 'Ollama' },
      { kind: 'ops', label: 'Structured' },
    ],
    footMeta: '~18 min',
  },
  {
    slug: 'patient_form_extraction',
    title: 'Patient *Form* Extraction',
    index: '09 / 20',
    category: 'llm',
    thumbLabel: 'Nested · typed',
    motif: MOTIFS.forms,
    description: 'Extract nested structured data from patient intake forms with field-level transformation and data mapping.',
    tags: [
      { kind: 'src', label: 'PDF' },
      { kind: 'llm', label: 'Any LLM' },
      { kind: 'ops', label: 'Data mapping' },
    ],
    footMeta: '~22 min',
  },
  {
    slug: 'patient_form_extraction_baml',
    title: 'Patient Intake *(BAML)*',
    index: '10 / 20',
    category: 'llm',
    thumbLabel: 'BAML · typed',
    motif: MOTIFS.cross,
    description: 'BAML as the typed contract between LLM and code. Same intake problem, stronger guarantees.',
    tags: [
      { kind: 'src', label: 'PDF' },
      { kind: 'llm', label: 'BAML' },
      { kind: 'ops', label: 'Structured' },
    ],
    footMeta: '~25 min',
  },
  {
    slug: 'patient_form_extraction_dspy',
    title: 'Patient Intake *(DSPy)*',
    index: '11 / 20',
    category: 'llm',
    thumbLabel: 'DSPy · vision',
    motif: MOTIFS.dspy,
    description: 'DSPy-style prompt programming on vision models. Compare the ergonomics to the BAML variant side by side.',
    tags: [
      { kind: 'src', label: 'PDF' },
      { kind: 'llm', label: 'DSPy' },
      { kind: 'ops', label: 'Vision models' },
    ],
    footMeta: '~28 min',
  },
  {
    slug: 'document_ai',
    title: 'Document AI *Parser*',
    index: '12 / 20',
    category: 'llm',
    thumbLabel: 'Google · parse',
    motif: MOTIFS.parser,
    description: 'Bring your own parser. Google Document AI extracts, CocoIndex embeds and stores for semantic search.',
    tags: [
      { kind: 'src', label: 'PDF' },
      { kind: 'ops', label: 'Custom parser' },
      { kind: 'tgt', label: 'Postgres' },
    ],
    footMeta: '~20 min',
  },

  // ── Knowledge Graphs ──
  {
    slug: 'knowledge-graph-for-docs',
    title: 'Knowledge Graph for *Docs*',
    index: '13 / 20',
    category: 'agents',
    thumbLabel: 'Docs · Neo4j',
    motif: MOTIFS.graphTree,
    description: 'Build live knowledge for agents from documentation — incremental triple extraction with LLMs.',
    tags: [
      { kind: 'src', label: 'Markdown' },
      { kind: 'llm', label: 'Any LLM' },
      { kind: 'tgt', label: 'Neo4j' },
    ],
    footMeta: '~30 min',
    sourceSlug: 'docs_to_knowledge_graph',
  },
  {
    slug: 'meeting_notes_graph',
    title: 'Meeting Notes *Graph*',
    index: '14 / 20',
    category: 'agents',
    thumbLabel: 'Drive · Neo4j',
    motif: MOTIFS.knowledgeChat,
    description: 'Turn Google Drive meeting notes into an automatically updating Neo4j knowledge graph.',
    tags: [
      { kind: 'src', label: 'Drive' },
      { kind: 'llm', label: 'Any LLM' },
      { kind: 'tgt', label: 'Neo4j' },
    ],
    footMeta: '~32 min',
  },
  {
    slug: 'product_recommendation',
    title: 'Product *Recommendation*',
    index: '15 / 20',
    category: 'agents',
    thumbLabel: 'Taxonomy · graph',
    motif: MOTIFS.taxonomy,
    description: 'Real-time recommendation engine — product taxonomy understanding via LLM, stored in a graph database.',
    tags: [
      { kind: 'src', label: 'Catalog' },
      { kind: 'llm', label: 'Any LLM' },
      { kind: 'tgt', label: 'Graph DB' },
    ],
    footMeta: '~35 min',
  },

  // ── Multimodal ──
  {
    slug: 'image_search',
    title: 'Image Search *(ColPali)*',
    index: '16 / 20',
    category: 'image',
    thumbLabel: 'ColPali · FastAPI',
    motif: MOTIFS.image,
    description: 'ColPali embeddings served behind a FastAPI endpoint. Page-level multi-vector image search.',
    tags: [
      { kind: 'src', label: 'Images' },
      { kind: 'llm', label: 'ColPali' },
      { kind: 'tgt', label: 'Postgres' },
    ],
    footMeta: '~22 min',
  },
  {
    slug: 'image_search_clip',
    title: 'Image Search *(CLIP)*',
    index: '17 / 20',
    category: 'image',
    thumbLabel: 'CLIP · query',
    motif: MOTIFS.image,
    description: 'CLIP embeddings over a folder of images. Query by text or reference image.',
    tags: [
      { kind: 'src', label: 'Images' },
      { kind: 'llm', label: 'CLIP' },
      { kind: 'tgt', label: 'Postgres' },
    ],
    footMeta: '~15 min',
    sourceSlug: 'image_search',
  },
  {
    slug: 'multi_format_index',
    title: 'Multi-format *Index*',
    index: '18 / 20',
    category: 'image',
    thumbLabel: 'PDF · slides · images',
    motif: MOTIFS.slides,
    description: 'ColPali over PDFs, images, academic papers, and slides — mixed together in the same vector space, no OCR.',
    tags: [
      { kind: 'src', label: 'Mixed' },
      { kind: 'llm', label: 'ColPali' },
      { kind: 'ops', label: 'No OCR' },
    ],
    footMeta: '~25 min',
    sourceSlug: 'multi_format_indexing',
  },
  {
    slug: 'pdf_elements',
    title: 'PDF *Elements*',
    index: '19 / 20',
    category: 'image',
    thumbLabel: 'PDF · unified',
    motif: MOTIFS.pdf,
    description: 'Extract, embed, and index both text and images from PDFs — SentenceTransformers + CLIP in one vector space.',
    tags: [
      { kind: 'src', label: 'PDF' },
      { kind: 'llm', label: 'CLIP + ST' },
      { kind: 'ops', label: 'Unified' },
    ],
    footMeta: '~20 min',
    sourceSlug: 'pdf_elements_embedding',
  },
  {
    slug: 'photo_search',
    title: 'Photo Search *(Faces)*',
    index: '20 / 20',
    category: 'image',
    thumbLabel: 'Faces · similarity',
    motif: MOTIFS.face,
    description: 'Detect, extract, and embed faces from photos. Export to a vector DB for face similarity queries.',
    tags: [
      { kind: 'src', label: 'Images' },
      { kind: 'ops', label: 'Face detect' },
      { kind: 'tgt', label: 'Postgres' },
    ],
    footMeta: '~18 min',
    sourceSlug: 'face_recognition',
  },
];

// Featured example (rendered as the big hero card on the listing).
export const featuredSlug = 'hackernews-trending-topics';

// Helper lookups
export const byCategory = (cat: Category): ExampleCard[] =>
  examples.filter((e) => e.category === cat && !e.featured);

export const findExample = (slug: string): ExampleCard | undefined =>
  examples.find((e) => e.slug === slug);

// Sidebar groupings on the listing page — derived from the catalog.
export const SIDEBAR_TARGETS = ['Postgres', 'Neo4j', 'LanceDB', 'Graph DB', 'Local FS'];
export const SIDEBAR_SOURCES = ['Local FS', 'PDF', 'Drive', 'Postgres', 'HN API', 'Images'];
export const SIDEBAR_LLMS    = ['OpenAI', 'Gemini', 'Anthropic', 'Ollama', 'BAML', 'DSPy', 'CLIP', 'ColPali'];
export const POPULAR: Array<{ slug: string; label: string; count: string }> = [
  { slug: 'hackernews-trending-topics', label: 'HN Trending',        count: '★'  },
  { slug: 'code_index',                 label: 'Codebase Indexing',  count: '★'  },
  { slug: 'simple_vector_index',        label: 'Simple Vector Index', count: '★' },
  { slug: 'image_search_clip',          label: 'Image Search (CLIP)', count: '★' },
];

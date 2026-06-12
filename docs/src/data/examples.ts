// Metadata for the /docs/examples listing and per-example pages.
//
// Card metadata for the /docs/examples listing. Markdown walkthrough bodies live
// in src/content/example-posts/<slug>.md and are rendered by
// src/pages/examples/[slug].astro beneath the shared hero. Titles may use
// *asterisks* to mark the italic-coral accent — see consts.titleMarkup.
//
// This file grows as new walkthroughs land in src/content/example-posts.

export type Category = 'search' | 'ingest' | 'llm' | 'agents' | 'image';

export const CATEGORY_META: Record<Category, { label: string; em?: string; lead: string; thumbClass: string }> = {
  search: { label: 'Vector ', em: 'Indexes', lead: 'Embed your documents, store vectors, answer by meaning.', thumbClass: 'search' },
  ingest: { label: 'Custom ', em: 'Building Blocks', lead: 'Bring your own source, target, or parser. Same declarative flow.', thumbClass: 'ingest' },
  llm: { label: 'Structured ', em: 'Extraction', lead: 'LLM-extract typed, structured data from code and documents — with instructor, BAML, or DSPy.', thumbClass: 'llm' },
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
  textVec: `<svg viewBox="0 0 120 70" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="12" y="16" width="26" height="38" rx="2"/><path d="M17 25 L33 25 M17 32 L29 32 M17 39 L33 39 M17 46 L25 46" stroke-width="1.2"/><path d="M44 35 L56 35 M50 30 L56 35 L50 40" stroke-linecap="round" stroke-linejoin="round"/><circle cx="66" cy="24" r="2.4" fill="currentColor"/><circle cx="78" cy="24" r="2.4" fill="currentColor"/><circle cx="66" cy="35" r="2.4" fill="currentColor"/><circle cx="78" cy="35" r="2.4" fill="currentColor"/><circle cx="66" cy="46" r="2.4" fill="currentColor"/><circle cx="78" cy="46" r="2.4" fill="currentColor"/><path d="M86 35 L98 35 M92 30 L98 35 L92 40" stroke-linecap="round" stroke-linejoin="round"/><circle cx="106" cy="31" r="7"/><path d="M111 36 L116 41" stroke-linecap="round"/></svg>`,
  graph: `<svg viewBox="0 0 120 70" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M34 20 L58 36 M86 19 L64 36 M34 50 L56 41 M86 51 L66 40 M36 19 L84 18"/><circle cx="30" cy="18" r="6"/><circle cx="90" cy="17" r="6"/><circle cx="28" cy="52" r="6"/><circle cx="92" cy="52" r="6"/><circle cx="61" cy="38" r="8" fill="currentColor" opacity="0.14"/><circle cx="61" cy="38" r="8"/></svg>`,
} as const;

export const examples: ExampleCard[] = [
  {
    slug: 'text-embedding',
    title: 'Semantic Search *101*',
    index: '01 / 06',
    category: 'search',
    thumbLabel: 'Markdown · embeddings',
    motif: MOTIFS.textVec,
    description: 'Chunk Markdown files, embed each chunk, store the vectors in Postgres, and search them in plain English. The simplest end-to-end vector index — the best place to start.',
    tags: [
      { kind: 'src', label: 'Local FS' },
      { kind: 'tgt', label: 'Postgres' },
      { kind: 'ops', label: 'Embeddings' },
      { kind: 'lvl', label: 'Starter' },
    ],
    footMeta: '~6 min · starter',
    sourceSlug: 'text_embedding',
  },
  {
    slug: 'index-codebase',
    title: 'Index Your *Codebase*',
    index: '02 / 06',
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
    index: '03 / 06',
    category: 'llm',
    thumbLabel: 'Code · structured output',
    motif: MOTIFS.repos,
    description: 'Walk many Python repos, LLM-extract a typed summary per file — classes, functions, Mermaid call graphs — and aggregate into an always-fresh wiki page per project. The flagship v1 walkthrough.',
    tags: [
      { kind: 'src', label: 'Multi-repo' },
      { kind: 'tgt', label: 'Local FS' },
      { kind: 'llm', label: 'Any LLM' },
      { kind: 'ops', label: 'Structured output' },
      { kind: 'lvl', label: 'Advanced' },
    ],
    footMeta: '~35 min · featured',
    sourceSlug: 'multi_codebase_summarization',
    featured: true,
  },
  {
    slug: 'pdf-to-markdown',
    title: 'PDF → *Markdown*',
    index: '04 / 06',
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
    index: '05 / 06',
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
  {
    slug: 'docs-to-knowledge-graph',
    title: 'Docs → *Knowledge Graph*',
    index: '06 / 06',
    category: 'agents',
    thumbLabel: 'Markdown · LLM + Neo4j',
    motif: MOTIFS.graph,
    description: 'Turn a folder of Markdown docs into a Neo4j concept graph — LLM-extracted (subject, predicate, object) triples that stay in sync as the docs change.',
    tags: [
      { kind: 'src', label: 'Local FS' },
      { kind: 'llm', label: 'Any LLM' },
      { kind: 'tgt', label: 'Neo4j' },
      { kind: 'lvl', label: 'Intermediate' },
    ],
    footMeta: '~20 min',
    sourceSlug: 'docs_to_knowledge_graph',
  },
];

export const featuredSlug = 'multi-codebase-summarization';

// Featured examples still show up in their home category grid — when the
// catalog is small, hiding them leaves categories visibly empty.
export const byCategory = (cat: Category): ExampleCard[] =>
  examples.filter((e) => e.category === cat);

export const findExample = (slug: string): ExampleCard | undefined =>
  examples.find((e) => e.slug === slug);

// Sidebar facets shown on the listing — every entry is demonstrated by a
// runnable example in EXAMPLE_CATALOG below (keep the two in sync).
export const SIDEBAR_TARGETS = ['Postgres', 'Qdrant', 'LanceDB', 'Turbopuffer', 'Neo4j', 'FalkorDB', 'SurrealDB', 'Kafka', 'Local FS'];
export const SIDEBAR_SOURCES = ['Local FS', 'Amazon S3', 'Google Drive', 'OCI Storage', 'Postgres', 'Kafka', 'YouTube', 'HackerNews'];
export const SIDEBAR_LLMS = ['OpenAI', 'Gemini', 'Any via LiteLLM'];
export const POPULAR: Array<{ slug: string; label: string; count: string }> = [
  { slug: 'text-embedding', label: 'Semantic Search 101', count: '★' },
  { slug: 'index-codebase', label: 'Index Your Codebase', count: '★' },
  { slug: 'multi-codebase-summarization', label: 'Multi-codebase Summarization', count: '★' },
  { slug: 'pdf-to-markdown', label: 'PDF → Markdown', count: '★' },
  { slug: 'podcast-to-knowledge-graph', label: 'Podcasts → Knowledge Graph', count: '★' },
  { slug: 'docs-to-knowledge-graph', label: 'Docs → Knowledge Graph', count: '★' },
];

// Full catalog of runnable examples in the cocoindex monorepo, used to build the
// machine-readable index at /docs/llms.txt so agents can see the whole set in one
// fetch (the on-page listing curates only a few). `dir` is the folder under
// examples/ in the main repo; entries with `docs` also have a step-by-step
// walkthrough on the site, the rest link to source on GitHub. The groups render
// as subsections in llms.txt so agents can navigate by use case. Add an entry to
// the right group when a new example lands under examples/.
export type ExampleCatalogEntry = {
  dir: string;       // examples/<dir> in github.com/cocoindex-io/cocoindex
  title: string;
  description: string;
  docs?: string;     // docs slug when a full walkthrough exists (→ /docs/examples/<docs>)
  run?: string;      // shortest useful command for agents after install/env setup
};

export type ExampleCatalogGroup = {
  title: string;     // subsection header in llms.txt
  blurb: string;     // one-liner under the header — helps agents pick a group
  entries: ExampleCatalogEntry[];
};

const RUN_MAIN = 'cocoindex update main';
const RUN_MAIN_PY = 'cocoindex update main.py';

export const EXAMPLE_CATALOG_GROUPS: ExampleCatalogGroup[] = [
  {
    title: 'Documented walkthroughs',
    blurb: 'Step-by-step guides on the docs site — the best entry points.',
    entries: [
      { dir: 'text_embedding', docs: 'text-embedding', title: 'Semantic Search 101', description: 'Chunk local Markdown, embed each chunk, store the vectors in Postgres (pgvector), and search in plain English. The simplest end-to-end vector index.', run: RUN_MAIN },
      { dir: 'code_embedding', docs: 'index-codebase', title: 'Index Your Codebase', description: 'Walk a repo, split by syntax with Tree-sitter, embed, and query your codebase in English — a live pgvector index for AI coding agents.', run: RUN_MAIN },
      { dir: 'multi_codebase_summarization', docs: 'multi-codebase-summarization', title: 'Multi-codebase Summarization', description: 'Walk many Python repos, LLM-extract typed per-file info (classes, functions, Mermaid call graphs), and aggregate into an always-fresh Markdown wiki page per project.', run: RUN_MAIN_PY },
      { dir: 'pdf_to_markdown', docs: 'pdf-to-markdown', title: 'PDF → Markdown', description: 'Incrementally convert a folder of local PDFs to Markdown with docling.', run: RUN_MAIN },
      { dir: 'conversation_to_knowledge', docs: 'podcast-to-knowledge-graph', title: 'Podcasts → Knowledge Graph', description: 'Turn YouTube podcasts into a SurrealDB knowledge graph: diarized transcription, two-step LLM extraction, and embedding-based entity resolution.', run: 'cocoindex update conv_knowledge.app' },
      { dir: 'docs_to_knowledge_graph', docs: 'docs-to-knowledge-graph', title: 'Docs → Knowledge Graph', description: 'Turn a folder of Markdown docs into a Neo4j concept graph: LLM-extracted (subject, predicate, object) triples, declared as nodes and edges that stay in sync.', run: RUN_MAIN },
    ],
  },
  {
    title: 'Vector search',
    blurb: 'Embed and semantically search text from more sources, into more vector stores.',
    entries: [
      { dir: 'text_embedding_qdrant', title: 'Text Embedding · Qdrant', description: 'Embed local Markdown files and store the chunks + vectors in Qdrant; semantic-search demo.' },
      { dir: 'text_embedding_lancedb', title: 'Text Embedding · LanceDB', description: 'Embed local Markdown files and store the chunks + vectors in LanceDB; semantic-search demo.' },
      { dir: 'text_embedding_turbopuffer', title: 'Text Embedding · Turbopuffer', description: 'Embed local Markdown files into a Turbopuffer namespace; semantic-search demo.' },
      { dir: 'code_embedding_lancedb', title: 'Code Embedding · LanceDB', description: 'Extract code chunks from Python/Rust/TOML/Markdown and store code + vectors in LanceDB; semantic code search.' },
      { dir: 'pdf_embedding', title: 'PDF Embedding', description: 'Convert local PDFs to Markdown, chunk, embed, and store in Postgres (pgvector); query demo.' },
      { dir: 'entire_session_search', title: 'Entire Session Search', description: 'Semantic search over AI coding sessions captured by Entire — transcripts, prompts, and context summaries into Postgres (pgvector).' },
      { dir: 'amazon_s3_embedding', title: 'Amazon S3 Embedding', description: 'Embed Markdown files from an S3 bucket into Postgres (pgvector); semantic-search demo.' },
      { dir: 'gdrive_text_embedding', title: 'Google Drive Text Embedding', description: 'Embed text files from Google Drive into Postgres (pgvector); query demo.' },
      { dir: 'oci_object_storage_embedding', title: 'OCI Object Storage Embedding', description: 'Embed Markdown files from Oracle Cloud (OCI) Object Storage into Postgres (pgvector); query demo.' },
      { dir: 'postgres_source', title: 'Postgres as a Source', description: 'Use an existing PostgreSQL table as a CocoIndex source: derive fields, embed, and store the results back.' },
    ],
  },
  {
    title: 'Multimodal',
    blurb: 'Images and audio — same declarative flow, different encoder.',
    entries: [
      { dir: 'image_search', title: 'Image Search (CLIP)', description: 'Build an image-search index with CLIP embeddings and Qdrant; query in natural language via FastAPI + React.', run: 'python -m uvicorn api:app --reload --host 0.0.0.0 --port 8000' },
      { dir: 'image_search_colpali', title: 'Image Search (ColPali)', description: 'Image search using the ColPali multi-vector model with Qdrant MaxSim; natural-language queries via FastAPI.', run: 'python -m uvicorn api:app --reload --host 0.0.0.0 --port 8000' },
      { dir: 'audio_to_text', title: 'Audio → Text', description: 'Transcribe local audio files with LiteLLM and store one row per file in Postgres, keyed by filename.', run: RUN_MAIN_PY },
    ],
  },
  {
    title: 'Structured extraction',
    blurb: 'LLM-extract typed, structured data — with instructor, BAML, or DSPy.',
    entries: [
      { dir: 'hn_trending_topics', title: 'HackerNews Trending Topics', description: 'Scrape recent HackerNews threads and comments via the Algolia HN API, LLM-extract topics, and store in Postgres.' },
      { dir: 'paper_metadata', title: 'Paper Metadata', description: 'LLM-extract title, authors, and abstract from PDF papers into Postgres, with embeddings for semantic search.' },
      { dir: 'patient_intake_extraction_baml', title: 'Patient Intake Extraction · BAML', description: 'Extract structured data from patient intake forms with BAML.', run: RUN_MAIN_PY },
      { dir: 'patient_intake_extraction_dspy', title: 'Patient Intake Extraction · DSPy', description: 'Extract structured data from patient intake forms with DSPy.', run: RUN_MAIN_PY },
    ],
  },
  {
    title: 'Knowledge graphs',
    blurb: 'Extract entities and relationships into graph databases that stay in sync.',
    entries: [
      { dir: 'meeting_notes_graph_neo4j', title: 'Meeting Notes → Knowledge Graph · Neo4j', description: 'Extract structured info from Google Drive meeting notes into a Neo4j knowledge graph.' },
      { dir: 'meeting_notes_graph_falkordb', title: 'Meeting Notes → Knowledge Graph · FalkorDB', description: 'Extract structured info from Google Drive meeting notes into a FalkorDB knowledge graph.' },
    ],
  },
  {
    title: 'Custom building blocks & streaming',
    blurb: 'Bring your own transform or wire pipelines to streaming systems like Kafka.',
    entries: [
      { dir: 'files_transform', title: 'Files Transform', description: 'Watch a directory of Markdown files and convert each to HTML with markdown-it-py, writing .html outputs incrementally.' },
      { dir: 'csv_to_kafka', title: 'CSV → Kafka', description: 'Watch local CSV files, convert each row to JSON, and publish to a Kafka topic — only changed rows on edit.', run: 'cocoindex update -L main.py' },
      { dir: 'kafka_to_lancedb', title: 'Kafka → LanceDB', description: 'Consume JSON messages from a Kafka topic and dispatch them to two LanceDB tables by message shape.', run: 'cocoindex update -L main.py' },
    ],
  },
  {
    title: 'Rust',
    blurb: 'The same declarative flows using the CocoIndex Rust API.',
    entries: [
      { dir: 'rust', title: 'Rust Examples', description: 'Rust ports of many of the examples above — the same declarative flows using the CocoIndex Rust API.', run: 'cd rust/<example> && follow its README; common index command: cargo run -- index' },
    ],
  },
];

export const EXAMPLE_CATALOG: ExampleCatalogEntry[] = EXAMPLE_CATALOG_GROUPS.flatMap((g) => g.entries);

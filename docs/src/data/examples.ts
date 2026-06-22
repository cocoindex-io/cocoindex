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
    index: '01 / 26',
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
    index: '02 / 26',
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
    index: '03 / 26',
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
    index: '04 / 26',
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
    index: '05 / 26',
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
    index: '06 / 26',
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
  {
    slug: 'meeting-notes-to-knowledge-graph',
    title: 'Meeting Notes → *Knowledge Graph*',
    index: '07 / 26',
    category: 'agents',
    thumbLabel: 'Google Drive · LLM + Neo4j',
    motif: MOTIFS.graph,
    description: 'Turn Google Drive meeting notes into a Neo4j knowledge graph — LLM extraction of organizers, attendees, and tasks, plus embedding-based person entity resolution, kept in sync as notes change.',
    tags: [
      { kind: 'src', label: 'Google Drive' },
      { kind: 'llm', label: 'Any LLM' },
      { kind: 'ops', label: 'Entity resolution' },
      { kind: 'tgt', label: 'Neo4j' },
      { kind: 'lvl', label: 'Intermediate' },
    ],
    footMeta: '~25 min',
    sourceSlug: 'meeting_notes_graph_neo4j',
  },
  {
    slug: 'csv-to-kafka',
    title: 'CSV → *Kafka*',
    index: '08 / 26',
    category: 'ingest',
    thumbLabel: 'CSV · live Kafka target',
    motif: MOTIFS.pdfToMd,
    description: 'Watch a folder of CSV files and publish each row as a JSON message to a Kafka topic — declarative target states, only-changed-rows produces, and live mode in ~60 lines.',
    tags: [
      { kind: 'src', label: 'Local FS' },
      { kind: 'tgt', label: 'Kafka' },
      { kind: 'ops', label: 'Live mode' },
      { kind: 'lvl', label: 'Starter' },
    ],
    footMeta: '~12 min',
    sourceSlug: 'csv_to_kafka',
  },
  {
    slug: 'pdf-embedding',
    title: 'Semantic Search over *PDFs*',
    index: '09 / 26',
    category: 'search',
    thumbLabel: 'PDF · docling + embeddings',
    motif: MOTIFS.textVec,
    description: 'Convert local PDFs to Markdown with docling on a GPU runner, chunk, embed, and store the vectors in Postgres — then query in natural language. A vector index over your documents.',
    tags: [
      { kind: 'src', label: 'PDF' },
      { kind: 'tgt', label: 'Postgres' },
      { kind: 'ops', label: 'docling + embeddings' },
      { kind: 'lvl', label: 'Starter' },
    ],
    footMeta: '~12 min',
    sourceSlug: 'pdf_embedding',
  },
  {
    slug: 'image-search',
    title: 'Search Images by *Text*',
    index: '10 / 26',
    category: 'image',
    thumbLabel: 'Images · CLIP + Qdrant',
    motif: MOTIFS.textVec,
    description: 'Embed images with CLIP, store the vectors in Qdrant, and search your photos in natural language through a FastAPI + React app — live updates, no tags or captions.',
    tags: [
      { kind: 'src', label: 'Local FS' },
      { kind: 'ops', label: 'CLIP' },
      { kind: 'tgt', label: 'Qdrant' },
      { kind: 'lvl', label: 'Intermediate' },
    ],
    footMeta: '~15 min',
    sourceSlug: 'image_search',
  },
  {
    slug: 'audio-to-text',
    title: "Audio to *Text*",
    index: '11 / 26',
    category: 'image',
    thumbLabel: "Audio \u00b7 LiteLLM",
    motif: MOTIFS.textVec,
    description: "Transcribe local audio files with a LiteLLM speech-to-text model and store one transcript row per file in Postgres, keyed by filename.",
    tags: [
      { kind: 'src', label: "Audio files" },
      { kind: 'tgt', label: "Postgres" },
      { kind: 'llm', label: "LiteLLM" },
      { kind: 'ops', label: "Transcription" },
      { kind: 'lvl', label: "Beginner" },
    ],
    footMeta: "~12 min",
    sourceSlug: 'audio_to_text',
  },
  {
    slug: 'hackernews-trending-topics',
    title: "Trending Topics from *HackerNews*",
    index: '12 / 26',
    category: 'llm',
    thumbLabel: "HN API \u00b7 LLM topics",
    motif: MOTIFS.textVec,
    description: "Scrape recent HackerNews threads and comments via the Algolia HN API, extract topics with an LLM, and rank what is trending in Postgres.",
    tags: [
      { kind: 'src', label: "HackerNews API" },
      { kind: 'tgt', label: "Postgres" },
      { kind: 'llm', label: "LiteLLM" },
      { kind: 'ops', label: "Custom source" },
      { kind: 'lvl', label: "Intermediate" },
    ],
    footMeta: "~10 min",
    sourceSlug: 'hn_trending_topics',
  },
  {
    slug: 'paper-metadata',
    title: "Index *Academic Papers*",
    index: '13 / 26',
    category: 'llm',
    thumbLabel: "PDF \u00b7 LLM extract",
    motif: MOTIFS.pdfToMd,
    description: "LLM-extract title, authors, and abstract from academic PDFs into typed rows, embed them, and store it all in Postgres with pgvector.",
    tags: [
      { kind: 'src', label: "PDF" },
      { kind: 'llm', label: "gpt-4o" },
      { kind: 'tgt', label: "Postgres" },
      { kind: 'ops', label: "pgvector" },
      { kind: 'lvl', label: "Intermediate" },
    ],
    footMeta: "~12 min",
    sourceSlug: 'paper_metadata',
  },
  {
    slug: 'patient-intake-baml',
    title: "Patient Intake Forms to Typed JSON with *BAML*",
    index: '14 / 26',
    category: 'llm',
    thumbLabel: "PDF \u00b7 BAML",
    motif: MOTIFS.pdfToMd,
    description: "Extract schema-validated patient records from intake-form PDFs with one type-safe BAML Gemini-vision call per file, writing a JSON file per form.",
    tags: [
      { kind: 'src', label: "PDF" },
      { kind: 'tgt', label: "JSON files" },
      { kind: 'llm', label: "Gemini" },
      { kind: 'ops', label: "BAML" },
      { kind: 'lvl', label: "Intermediate" },
    ],
    footMeta: "~10 min",
    sourceSlug: 'patient_intake_extraction_baml',
  },
  {
    slug: 'patient-intake-dspy',
    title: "Patient Intake Forms to Typed JSON with *DSPy*",
    index: '15 / 26',
    category: 'llm',
    thumbLabel: "PDF \u00b7 DSPy",
    motif: MOTIFS.pdfToMd,
    description: "Render patient intake PDFs to images and extract a typed Patient with a DSPy ChainOfThought vision module on Gemini, writing one validated JSON file per form.",
    tags: [
      { kind: 'src', label: "PDF" },
      { kind: 'tgt', label: "JSON" },
      { kind: 'llm', label: "DSPy" },
      { kind: 'llm', label: "Gemini" },
      { kind: 'ops', label: "structured extraction" },
      { kind: 'lvl', label: "intermediate" },
    ],
    footMeta: "~10 min",
    sourceSlug: 'patient_intake_extraction_dspy',
  },
  {
    slug: 'postgres-source',
    title: "Postgres as a *Source*",
    index: '16 / 26',
    category: 'ingest',
    thumbLabel: "Postgres \u00b7 pgvector",
    motif: MOTIFS.textVec,
    description: "Read rows from an existing Postgres table, derive fields, embed each row, and write the vectors back to Postgres with pgvector.",
    tags: [
      { kind: 'src', label: "Postgres" },
      { kind: 'tgt', label: "pgvector" },
      { kind: 'llm', label: "SentenceTransformers" },
      { kind: 'ops', label: "Embeddings" },
      { kind: 'lvl', label: "Beginner" },
    ],
    footMeta: "~8 min",
    sourceSlug: 'postgres_source',
  },
  {
    slug: 'files-transform',
    title: "Transform a *Folder of Files*",
    index: '17 / 26',
    category: 'ingest',
    thumbLabel: "Markdown \u00b7 markdown-it",
    motif: MOTIFS.pdfToMd,
    description: "The smallest end-to-end CocoIndex pipeline \u2014 watch a folder of Markdown, render each file to HTML, and write the outputs to a local folder incrementally.",
    tags: [
      { kind: 'src', label: "Local files" },
      { kind: 'tgt', label: "Local files" },
      { kind: 'ops', label: "markdown-it-py" },
      { kind: 'ops', label: "Custom transform" },
      { kind: 'lvl', label: "Beginner" },
    ],
    footMeta: "~6 min",
    sourceSlug: 'files_transform',
  },
  {
    slug: 'kafka-to-lancedb',
    title: "Consume Kafka into *LanceDB*",
    index: '18 / 26',
    category: 'ingest',
    thumbLabel: "Kafka \u00b7 dispatch",
    motif: MOTIFS.textVec,
    description: "Consume JSON messages off a Kafka topic and dispatch each one \u2014 by its shape \u2014 into the matching LanceDB table.",
    tags: [
      { kind: 'src', label: "Kafka" },
      { kind: 'tgt', label: "LanceDB" },
      { kind: 'ops', label: "live mode" },
      { kind: 'lvl', label: "intermediate" },
    ],
    footMeta: "~10 min",
    sourceSlug: 'kafka_to_lancedb',
  },
  {
    slug: 'entire-session-search',
    title: "Search Your *AI Coding Sessions*",
    index: '19 / 26',
    category: 'search',
    thumbLabel: "Entire \u00b7 Embeddings",
    motif: MOTIFS.textVec,
    description: "Index AI coding sessions captured by Entire \u2014 transcripts, prompts, and context summaries \u2014 into Postgres for natural-language semantic search.",
    tags: [
      { kind: 'src', label: "Entire sessions" },
      { kind: 'tgt', label: "Postgres + pgvector" },
      { kind: 'ops', label: "sentence-transformers" },
      { kind: 'ops', label: "incremental" },
      { kind: 'lvl', label: "Beginner" },
    ],
    footMeta: "~10 min",
    sourceSlug: 'entire_session_search',
  },
  {
    slug: 'image-search-colpali',
    title: "Image Search with *ColPali*",
    index: '20 / 26',
    category: 'image',
    thumbLabel: "Images \u00b7 ColPali",
    motif: MOTIFS.textVec,
    description: "Embed images and queries into multi-vector ColPali bags of patch vectors, store them in Qdrant, and rank with late-interaction MaxSim through a FastAPI app.",
    tags: [
      { kind: 'src', label: "Images" },
      { kind: 'llm', label: "ColPali" },
      { kind: 'tgt', label: "Qdrant MaxSim" },
      { kind: 'ops', label: "FastAPI" },
      { kind: 'lvl', label: "Intermediate" },
    ],
    footMeta: "~12 min",
    sourceSlug: 'image_search_colpali',
  },
  {
    slug: 'text-embedding-qdrant',
    title: "Semantic Search with *Qdrant*",
    index: '21 / 26',
    category: 'search',
    thumbLabel: "Markdown \u00b7 Qdrant",
    motif: MOTIFS.textVec,
    description: "The Semantic Search 101 pipeline pointed at Qdrant \u2014 chunk Markdown, embed locally, and upsert the vectors into a managed Qdrant collection.",
    tags: [
      { kind: 'src', label: "Markdown" },
      { kind: 'tgt', label: "Qdrant" },
      { kind: 'ops', label: "Embedding" },
      { kind: 'lvl', label: "Beginner" },
    ],
    footMeta: "~5 min",
    sourceSlug: 'text_embedding_qdrant',
  },
  {
    slug: 'text-embedding-lancedb',
    title: "Semantic Search with *LanceDB*",
    index: '22 / 26',
    category: 'search',
    thumbLabel: "Markdown \u00b7 LanceDB",
    motif: MOTIFS.textVec,
    description: "The Semantic Search 101 pipeline with LanceDB as the target \u2014 chunk Markdown, embed each chunk, and store the vectors in an embedded, file-based store with no server to run.",
    tags: [
      { kind: 'src', label: "Markdown" },
      { kind: 'tgt', label: "LanceDB" },
      { kind: 'llm', label: "sentence-transformers" },
      { kind: 'ops', label: "vector index" },
      { kind: 'lvl', label: "beginner" },
    ],
    footMeta: "~5 min",
    sourceSlug: 'text_embedding_lancedb',
  },
  {
    slug: 'text-embedding-turbopuffer',
    title: "Semantic Search with *Turbopuffer*",
    index: '23 / 26',
    category: 'search',
    thumbLabel: "Markdown \u00b7 Turbopuffer",
    motif: MOTIFS.textVec,
    description: "Chunk Markdown, embed each chunk, and upsert the vectors into a managed Turbopuffer namespace \u2014 the Semantic Search 101 pipeline pointed at a serverless vector store.",
    tags: [
      { kind: 'src', label: "Markdown" },
      { kind: 'tgt', label: "Turbopuffer" },
      { kind: 'ops', label: "sentence-transformers" },
      { kind: 'lvl', label: "Beginner" },
    ],
    footMeta: "~7 min",
    sourceSlug: 'text_embedding_turbopuffer',
  },
  {
    slug: 'amazon-s3-embedding',
    title: "Embed Markdown from *Amazon S3*",
    index: '24 / 26',
    category: 'search',
    thumbLabel: "S3 \u00b7 pgvector",
    motif: MOTIFS.textVec,
    description: "The Semantic Search 101 pipeline with an Amazon S3 bucket as the source instead of a local folder.",
    tags: [
      { kind: 'src', label: "Amazon S3" },
      { kind: 'tgt', label: "Postgres + pgvector" },
      { kind: 'llm', label: "sentence-transformers" },
      { kind: 'ops', label: "chunk + embed" },
      { kind: 'lvl', label: "Beginner" },
    ],
    footMeta: "~6 min",
    sourceSlug: 'amazon_s3_embedding',
  },
  {
    slug: 'google-drive-embedding',
    title: "Semantic Search over *Google Drive*",
    index: '25 / 26',
    category: 'search',
    thumbLabel: "Google Drive \u00b7 Embed",
    motif: MOTIFS.textVec,
    description: "The Semantic Search 101 pipeline with Google Drive as the source \u2014 chunk and embed every document and store the vectors in Postgres with pgvector.",
    tags: [
      { kind: 'src', label: "Google Drive" },
      { kind: 'tgt', label: "Postgres / pgvector" },
      { kind: 'ops', label: "Embeddings" },
      { kind: 'lvl', label: "Beginner" },
    ],
    footMeta: "~6 min",
    sourceSlug: 'gdrive_text_embedding',
  },
  {
    slug: 'oci-object-storage-embedding',
    title: "Embed *OCI Object Storage*",
    index: '26 / 26',
    category: 'search',
    thumbLabel: "OCI \u00b7 pgvector",
    motif: MOTIFS.textVec,
    description: "Chunk and embed Markdown objects from an Oracle Cloud (OCI) Object Storage bucket into Postgres/pgvector, with optional live updates via OCI Streaming.",
    tags: [
      { kind: 'src', label: "OCI Object Storage" },
      { kind: 'tgt', label: "Postgres / pgvector" },
      { kind: 'llm', label: "sentence-transformers" },
      { kind: 'ops', label: "OCI Streaming (live)" },
      { kind: 'lvl', label: "Beginner" },
    ],
    footMeta: "~7 min",
    sourceSlug: 'oci_object_storage_embedding',
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
  { slug: 'meeting-notes-to-knowledge-graph', label: 'Meeting Notes → Knowledge Graph', count: '★' },
  { slug: 'csv-to-kafka', label: 'CSV → Kafka', count: '★' },
  { slug: 'pdf-embedding', label: 'Semantic Search over PDFs', count: '★' },
  { slug: 'image-search', label: 'Search Images by Text', count: '★' },
  { slug: 'audio-to-text', label: "Audio to Text", count: '★' },
  { slug: 'hackernews-trending-topics', label: "Trending Topics from HackerNews", count: '★' },
  { slug: 'paper-metadata', label: "Index Academic Papers", count: '★' },
  { slug: 'patient-intake-baml', label: "Patient Intake Forms to Typed JSON with BAML", count: '★' },
  { slug: 'patient-intake-dspy', label: "Patient Intake Forms to Typed JSON with DSPy", count: '★' },
  { slug: 'postgres-source', label: "Postgres as a Source", count: '★' },
  { slug: 'files-transform', label: "Transform a Folder of Files", count: '★' },
  { slug: 'kafka-to-lancedb', label: "Consume Kafka into LanceDB", count: '★' },
  { slug: 'entire-session-search', label: "Search Your AI Coding Sessions", count: '★' },
  { slug: 'image-search-colpali', label: "Image Search with ColPali", count: '★' },
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
      { dir: 'meeting_notes_graph_neo4j', docs: 'meeting-notes-to-knowledge-graph', title: 'Meeting Notes → Knowledge Graph', description: 'Turn Google Drive meeting notes into a Neo4j knowledge graph: LLM extraction of organizers, attendees, and tasks, plus embedding-based person entity resolution.', run: RUN_MAIN },
      { dir: 'csv_to_kafka', docs: 'csv-to-kafka', title: 'CSV → Kafka', description: 'Watch a folder of CSV files and publish each row as a JSON message to a Kafka topic: declarative target states, only-changed-rows produces, and live mode.', run: 'cocoindex update -L main.py' },
      { dir: 'pdf_embedding', docs: 'pdf-embedding', title: 'Semantic Search over PDFs', description: 'Convert local PDFs to Markdown with docling (on a GPU runner), chunk, embed, and store the vectors in Postgres (pgvector); natural-language query demo.', run: RUN_MAIN },
      { dir: 'image_search', docs: 'image-search', title: 'Search Images by Text', description: 'Embed images with CLIP, store the vectors in Qdrant, and search your photos in natural language through a FastAPI + React app; live updates.', run: 'python -m uvicorn api:app --reload --host 0.0.0.0 --port 8000' },
    ],
  },
  {
    title: 'Vector search',
    blurb: 'Embed and semantically search text from more sources, into more vector stores.',
    entries: [
      { dir: 'text_embedding_qdrant', docs: 'text-embedding-qdrant', title: 'Text Embedding · Qdrant', description: 'Embed local Markdown files and store the chunks + vectors in Qdrant; semantic-search demo.' },
      { dir: 'text_embedding_lancedb', docs: 'text-embedding-lancedb', title: 'Text Embedding · LanceDB', description: 'Embed local Markdown files and store the chunks + vectors in LanceDB; semantic-search demo.' },
      { dir: 'text_embedding_turbopuffer', docs: 'text-embedding-turbopuffer', title: 'Text Embedding · Turbopuffer', description: 'Embed local Markdown files into a Turbopuffer namespace; semantic-search demo.' },
      { dir: 'code_embedding_lancedb', title: 'Code Embedding · LanceDB', description: 'Extract code chunks from Python/Rust/TOML/Markdown and store code + vectors in LanceDB; semantic code search.' },
      { dir: 'entire_session_search', docs: 'entire-session-search', title: 'Entire Session Search', description: 'Semantic search over AI coding sessions captured by Entire — transcripts, prompts, and context summaries into Postgres (pgvector).' },
      { dir: 'amazon_s3_embedding', docs: 'amazon-s3-embedding', title: 'Amazon S3 Embedding', description: 'Embed Markdown files from an S3 bucket into Postgres (pgvector); semantic-search demo.' },
      { dir: 'gdrive_text_embedding', docs: 'google-drive-embedding', title: 'Google Drive Text Embedding', description: 'Embed text files from Google Drive into Postgres (pgvector); query demo.' },
      { dir: 'oci_object_storage_embedding', docs: 'oci-object-storage-embedding', title: 'OCI Object Storage Embedding', description: 'Embed Markdown files from Oracle Cloud (OCI) Object Storage into Postgres (pgvector); query demo.' },
      { dir: 'postgres_source', docs: 'postgres-source', title: 'Postgres as a Source', description: 'Use an existing PostgreSQL table as a CocoIndex source: derive fields, embed, and store the results back.' },
    ],
  },
  {
    title: 'Multimodal',
    blurb: 'Images and audio — same declarative flow, different encoder.',
    entries: [
      { dir: 'image_search_colpali', docs: 'image-search-colpali', title: 'Image Search (ColPali)', description: 'Image search using the ColPali multi-vector model with Qdrant MaxSim; natural-language queries via FastAPI.', run: 'python -m uvicorn api:app --reload --host 0.0.0.0 --port 8000' },
      { dir: 'audio_to_text', docs: 'audio-to-text', title: 'Audio → Text', description: 'Transcribe local audio files with LiteLLM and store one row per file in Postgres, keyed by filename.', run: RUN_MAIN_PY },
    ],
  },
  {
    title: 'Structured extraction',
    blurb: 'LLM-extract typed, structured data — with instructor, BAML, or DSPy.',
    entries: [
      { dir: 'hn_trending_topics', docs: 'hackernews-trending-topics', title: 'HackerNews Trending Topics', description: 'Scrape recent HackerNews threads and comments via the Algolia HN API, LLM-extract topics, and store in Postgres.' },
      { dir: 'paper_metadata', docs: 'paper-metadata', title: 'Paper Metadata', description: 'LLM-extract title, authors, and abstract from PDF papers into Postgres, with embeddings for semantic search.' },
      { dir: 'patient_intake_extraction_baml', docs: 'patient-intake-baml', title: 'Patient Intake Extraction · BAML', description: 'Extract structured data from patient intake forms with BAML.', run: RUN_MAIN_PY },
      { dir: 'patient_intake_extraction_dspy', docs: 'patient-intake-dspy', title: 'Patient Intake Extraction · DSPy', description: 'Extract structured data from patient intake forms with DSPy.', run: RUN_MAIN_PY },
    ],
  },
  {
    title: 'Knowledge graphs',
    blurb: 'Extract entities and relationships into graph databases that stay in sync.',
    entries: [
      { dir: 'meeting_notes_graph_falkordb', title: 'Meeting Notes → Knowledge Graph · FalkorDB', description: 'Extract structured info from Google Drive meeting notes into a FalkorDB knowledge graph.' },
    ],
  },
  {
    title: 'Custom building blocks & streaming',
    blurb: 'Bring your own transform or wire pipelines to streaming systems like Kafka.',
    entries: [
      { dir: 'files_transform', docs: 'files-transform', title: 'Files Transform', description: 'Watch a directory of Markdown files and convert each to HTML with markdown-it-py, writing .html outputs incrementally.' },
      { dir: 'kafka_to_lancedb', docs: 'kafka-to-lancedb', title: 'Kafka → LanceDB', description: 'Consume JSON messages from a Kafka topic and dispatch them to two LanceDB tables by message shape.', run: 'cocoindex update -L main.py' },
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

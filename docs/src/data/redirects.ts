// Legacy v0 (Docusaurus) → v1 (Astro) URL map, consumed by astro.config.mjs.
// v1 reorganized the namespace (sources/ + targets/ → connectors/, core/ →
// programming_guide/, custom_ops/ → advanced_topics/, ai/ → ops/, tutorials/
// dropped, examples renamed), so every old inbound link / bookmark / search
// result below would 404 without a redirect.
//
// Mechanics (verified against astro 6.1.8): Astro prepends `base` (`/docs`)
// to the SOURCE keys only; DESTINATIONS are emitted verbatim, so they must be
// absolute `/docs/...` paths with the trailing slash (trailingSlash:
// 'always'). Pages with no v1 equivalent point at the nearest section
// overview rather than 404.
export const redirects: Record<string, string> = {
  // sources/* → connectors/*
  '/sources': '/docs/connectors/',
  '/sources/localfile': '/docs/connectors/localfs/',
  '/sources/amazons3': '/docs/connectors/amazon_s3/',
  '/sources/googledrive': '/docs/connectors/google_drive/',
  '/sources/postgres': '/docs/connectors/postgres/',
  '/sources/azureblob': '/docs/connectors/', // no Azure Blob connector in v1

  // targets/* → connectors/*
  '/targets': '/docs/connectors/',
  '/targets/postgres': '/docs/connectors/postgres/',
  '/targets/qdrant': '/docs/connectors/qdrant/',
  '/targets/lancedb': '/docs/connectors/lancedb/',
  '/targets/neo4j': '/docs/connectors/neo4j/',
  '/targets/doris': '/docs/connectors/doris/',
  '/targets/chromadb': '/docs/connectors/', // dropped in v1
  '/targets/pinecone': '/docs/connectors/', // dropped in v1
  '/targets/kuzu': '/docs/connectors/',     // dropped in v1
  '/targets/ladybug': '/docs/connectors/',  // dropped in v1

  // core/* → programming_guide / common_resources / cli
  '/core/basics': '/docs/programming_guide/core_concepts/',
  '/core/flow_def': '/docs/programming_guide/core_concepts/',
  '/core/flow_methods': '/docs/programming_guide/app/',
  '/core/settings': '/docs/advanced_topics/multiple_environments/',
  '/core/data_types': '/docs/common_resources/data_types/',
  '/core/cli': '/docs/cli/',

  // custom_ops/* → programming_guide / advanced_topics
  '/custom_ops/custom_functions': '/docs/programming_guide/function/',
  '/custom_ops/custom_targets': '/docs/advanced_topics/custom_target_connector/',
  '/custom_ops/custom_sources': '/docs/advanced_topics/live_component/', // closest: custom live component

  // ai/* → ops/*
  '/ai/llm': '/docs/ops/litellm/',

  // ops/functions (one monolithic page) → ops overview (now split per op)
  '/ops/functions': '/docs/ops/',

  // contributing
  '/contributing/new_built_in_target': '/docs/advanced_topics/custom_target_connector/',

  // tutorials/* (section dropped) → nearest concept page
  '/tutorials/live_updates': '/docs/programming_guide/live_mode/',
  '/tutorials/docker_pgvector_setup': '/docs/connectors/postgres/',
  '/tutorials/control_flow': '/docs/programming_guide/',
  '/tutorials/manage_flow_dynamically': '/docs/programming_guide/app/',

  // no v1 equivalent → land on the closest section / docs entry
  '/query': '/docs/getting_started/overview/',
  '/cocoinsight_access': '/docs/getting_started/overview/',

  // examples/* — v1 renamed every walkthrough slug (only
  // hackernews-trending-topics survived). Direct successor where one exists,
  // the examples listing otherwise.
  '/examples/academic_papers_index': '/docs/examples/paper-metadata/',
  '/examples/code_index': '/docs/examples/index-codebase/',
  '/examples/custom_source_hackernews': '/docs/examples/hackernews-trending-topics/',
  '/examples/custom_targets': '/docs/advanced_topics/custom_target_connector/',
  '/examples/document_ai': '/docs/examples/',
  '/examples/image_search': '/docs/examples/image-search/',
  '/examples/image_search_clip': '/docs/examples/image-search-colpali/',
  '/examples/knowledge-graph-for-docs': '/docs/examples/docs-to-knowledge-graph/',
  '/examples/manual_extraction': '/docs/examples/manuals-llm-extraction/',
  '/examples/meeting_notes_graph': '/docs/examples/meeting-notes-to-knowledge-graph/',
  '/examples/multi_format_index': '/docs/examples/multi-format-indexing/',
  '/examples/patient_form_extraction': '/docs/examples/patient-intake-baml/',
  '/examples/patient_form_extraction_baml': '/docs/examples/patient-intake-baml/',
  '/examples/patient_form_extraction_dspy': '/docs/examples/patient-intake-dspy/',
  '/examples/pdf_elements': '/docs/examples/pdf-to-markdown/',
  '/examples/photo_search': '/docs/examples/image-search/',
  '/examples/postgres_source': '/docs/examples/postgres-source/',
  '/examples/product_recommendation': '/docs/examples/product-recommendation/',
  '/examples/simple_vector_index': '/docs/examples/',
};

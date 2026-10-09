-- Component catalog for the multi_source_knowledge_graph example.
-- Usage: psql "$POSTGRES_URL" -f ./prepare_source_data.sql
--
-- One row per component of the CocoIndex project. `aliases` lists the other
-- ways a component is written in docs and issues (the key and name always
-- count); `depends_on` references other keys and becomes DEPENDS_ON edges.
DROP TABLE IF EXISTS catalog_components;
CREATE TABLE catalog_components (
    key text PRIMARY KEY,
    name text NOT NULL,
    kind text NOT NULL,
    area text NOT NULL,
    aliases text[] NOT NULL DEFAULT '{}',
    depends_on text[] NOT NULL DEFAULT '{}'
);
INSERT INTO catalog_components (key, name, kind, area, aliases, depends_on) VALUES
    -- Source / target connectors
    ('localfs',             'Local filesystem',       'connector', 'connectors/storage', ARRAY['local files', 'local filesystem', 'walk_dir'], ARRAY[]::text[]),
    ('amazon_s3',           'Amazon S3',              'connector', 'connectors/storage', ARRAY['s3', 'aws s3'], ARRAY[]::text[]),
    ('azure_blob',          'Azure Blob Storage',     'connector', 'connectors/storage', ARRAY['azure blob'], ARRAY[]::text[]),
    ('google_drive',        'Google Drive',           'connector', 'connectors/storage', ARRAY['gdrive', 'googledrive'], ARRAY[]::text[]),
    ('oci_object_storage',  'OCI Object Storage',     'connector', 'connectors/storage', ARRAY['oci', 'oracle cloud object storage'], ARRAY[]::text[]),
    ('kafka',               'Apache Kafka',           'connector', 'connectors/stream',  ARRAY[]::text[], ARRAY[]::text[]),
    ('iggy',                'Apache Iggy',            'connector', 'connectors/stream',  ARRAY[]::text[], ARRAY[]::text[]),
    ('postgres',            'PostgreSQL',             'connector', 'connectors/sql',     ARRAY['postgresql', 'pgvector', 'asyncpg'], ARRAY[]::text[]),
    ('sqlite',              'SQLite',                 'connector', 'connectors/sql',     ARRAY['sqlite3'], ARRAY[]::text[]),
    ('bigquery',            'Google BigQuery',        'connector', 'connectors/sql',     ARRAY['big query'], ARRAY[]::text[]),
    ('doris',               'Apache Doris',           'connector', 'connectors/sql',     ARRAY[]::text[], ARRAY[]::text[]),
    ('snowflake',           'Snowflake',              'connector', 'connectors/sql',     ARRAY[]::text[], ARRAY[]::text[]),
    ('lancedb',             'LanceDB',                'connector', 'connectors/vector',  ARRAY['lance'], ARRAY[]::text[]),
    ('qdrant',              'Qdrant',                 'connector', 'connectors/vector',  ARRAY[]::text[], ARRAY[]::text[]),
    ('turbopuffer',         'Turbopuffer',            'connector', 'connectors/vector',  ARRAY[]::text[], ARRAY[]::text[]),
    ('valkey',              'Valkey',                 'connector', 'connectors/vector',  ARRAY['valkey-search', 'valkey-glide'], ARRAY[]::text[]),
    ('zvec',                'Zvec',                   'connector', 'connectors/vector',  ARRAY[]::text[], ARRAY[]::text[]),
    ('neo4j',               'Neo4j',                  'connector', 'connectors/graph',   ARRAY[]::text[], ARRAY[]::text[]),
    ('falkordb',            'FalkorDB',               'connector', 'connectors/graph',   ARRAY['falkor'], ARRAY[]::text[]),
    ('surrealdb',           'SurrealDB',              'connector', 'connectors/graph',   ARRAY['surreal', 'surrealql'], ARRAY[]::text[]),
    -- Built-in operations
    ('text_splitter',       'Recursive text splitter', 'op',       'ops',                ARRAY['recursivesplitter', 'recursive_splitter', 'splitter', 'chunker', 'chunking'], ARRAY['ops_text']),
    ('sentence_transformers', 'SentenceTransformer embedder', 'op', 'ops',               ARRAY['sentence-transformers', 'sentencetransformer', 'sentence transformer', 'sentencetransformerembedder'], ARRAY[]::text[]),
    ('litellm_embedding',   'LiteLLM embedder',       'op',        'ops',                ARRAY['litellm'], ARRAY[]::text[]),
    ('entity_resolution',   'Entity resolution',      'op',        'ops',                ARRAY['resolve_entities', 'entity-resolution'], ARRAY[]::text[]),
    ('code_ops',            'Code chunking and matching ops', 'op', 'ops',               ARRAY['cocoindex.ops.code', 'codeast'], ARRAY['code_ast', 'code_match']),
    -- Rust crates
    ('core',                'Core engine (rust/core)', 'crate',    'engine',             ARRAY['cocoindex_core', 'lmdb', 'state store'], ARRAY['utils']),
    ('utils',               'Shared utilities (rust/utils)', 'crate', 'engine',          ARRAY['cocoindex_utils'], ARRAY[]::text[]),
    ('py_bindings',         'Python bindings (rust/py)', 'crate',  'sdk',                ARRAY['pyo3', 'cocoindex_py', 'rust/py'], ARRAY['core']),
    ('code_ast',            'Tree-sitter foundation (rust/code_ast)', 'crate', 'code',   ARRAY['tree-sitter', 'tree_sitter', 'treesitter'], ARRAY[]::text[]),
    ('ops_text',            'Text ops (rust/ops_text)', 'crate',   'code',               ARRAY[]::text[], ARRAY['code_ast']),
    ('code_match',          'Structural code matching (rust/code_match)', 'crate', 'code', ARRAY['code match'], ARRAY['code_ast']),
    -- SDKs and tooling
    ('python_sdk',          'Python SDK',             'sdk',       'sdk',                ARRAY['python api', 'python package', 'pip install cocoindex'], ARRAY['py_bindings']),
    ('rust_sdk',            'Rust SDK',               'sdk',       'sdk',                ARRAY['rust api', 'cocoindex crate', 'cargo add cocoindex'], ARRAY['core']),
    ('cli',                 'cocoindex CLI',          'tool',      'tooling',            ARRAY['cocoindex update', 'cocoindex server', 'cocoindex drop', 'command line'], ARRAY['python_sdk']);

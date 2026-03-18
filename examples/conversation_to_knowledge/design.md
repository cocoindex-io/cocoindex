# Conversation to Knowledge — Design

## Overview

Convert podcast sessions (from YouTube) into a structured knowledge graph stored in SurrealDB, using CocoIndex for declarative pipeline orchestration with incremental updates.

## Technology Choices

| Concern | Choice | Rationale |
|---------|--------|-----------|
| Audio download | `yt-dlp` | Standard, reliable YouTube downloader |
| Transcription + diarization | OpenAI `gpt-4o-transcribe-diarize` | Single API call gives speaker-labeled transcript. $0.006/min. No GPU, no extra deps beyond `openai` |
| LLM (extraction + resolution) | `instructor` + `litellm` → **`gpt-5.4-mini`** (configurable via `LLM_MODEL` in `.env`) | Good balance of cost ($0.75/$4.50 per 1M tokens) and capability for structured extraction |
| Embedding (entity resolution) | `sentence-transformers/all-MiniLM-L6-v2` | Fast, good for short entity name similarity |
| In-memory vector search | `faiss-cpu` (`IndexFlatIP`) | SIMD-optimized exact search with incremental `add()`; no GPU needed |
| Target database | SurrealDB | Graph DB with relations, per spec |
| Data models | Pydantic | Per spec |
| Pipeline | CocoIndex | Per spec |

## Input Format

A directory of plain text files, each containing YouTube URLs (one per line):

```
input/
├── ai_podcasts.txt
├── tech_interviews.txt
└── ...
```

Each file:
```
https://www.youtube.com/watch?v=dQw4w9WgXcQ
https://youtu.be/abc123
https://www.youtube.com/watch?v=xyz789
```

Empty lines and lines starting with `#` are ignored. Video IDs are extracted from URLs via regex:

```python
_YOUTUBE_URL_RE = re.compile(
    r"(?:youtube\.com/watch\?v=|youtu\.be/|youtube\.com/embed/)([a-zA-Z0-9_-]{11})"
)

def extract_video_id(url: str) -> str:
    m = _YOUTUBE_URL_RE.search(url)
    if m is None:
        raise ValueError(f"Cannot extract YouTube video ID from: {url}")
    return m.group(1)
```

We read these with `localfs.walk_dir()` matching `**/*.txt`.

## Data Models (Pydantic)

### Entity Models (for SurrealDB)

```python
@dataclass
class Session:
    id: str           # YouTube video ID
    name: str         # Extracted by LLM from transcript context
    description: str | None  # Extracted by LLM
    transcript: str   # Full transcript with speaker labels
    date: str | None  # Extracted by LLM if mentioned, else from yt-dlp metadata

@dataclass
class Person:
    id: str           # Auto-generated (hash of canonical name)
    name: str         # Canonical, Wikipedia-style name

@dataclass
class Tech:
    id: str
    name: str

@dataclass
class Org:
    id: str
    name: str

@dataclass
class Statement:
    id: str           # Auto-generated (hash of session_id + statement text)
    statement: str    # The statement text (one thematic claim)
```

### Relationship Models (SurrealDB Relations)

Relations have no additional edge fields — just `id` (auto-derived from `from_id + to_id`).

### LLM Extraction Model (Pydantic, for instructor)

We give the LLM the full session info (transcript + yt-dlp metadata) and let it extract everything in one call:

```python
class RawStatement(pydantic.BaseModel):
    """A thematic claim or statement made during the session."""
    statement: str
    speakers: list[str]           # Names of persons who made the statement
    involved_persons: list[str]   # Person names involved
    involved_techs: list[str]     # Tech names involved
    involved_orgs: list[str]      # Org names involved

class SessionExtraction(pydantic.BaseModel):
    """LLM output: metadata + entities extracted from a session."""
    name: str                           # Session/episode name
    description: str | None             # Brief description
    date: str | None                    # Date if mentioned (ISO format)
    persons_attending: list[str]        # Names of speakers/attendees
    statements: list[RawStatement]      # Thematic claims with attributions
```

## SurrealDB Schema

### Node Tables

| Table | Fields | Notes |
|-------|--------|-------|
| `session` | `id`, `name`, `description?`, `transcript`, `date?` | SCHEMAFULL |
| `person` | `id`, `name` | SCHEMAFULL |
| `tech` | `id`, `name` | SCHEMAFULL |
| `org` | `id`, `name` | SCHEMAFULL |
| `statement` | `id`, `statement` | SCHEMAFULL |

### Relation Tables (Edges)

| Relation | FROM → TO | Edge Fields |
|----------|-----------|-------------|
| `person_session` | `person` → `session` | (none) |
| `session_statement` | `session` → `statement` | (none) |
| `person_statement` | `person` → `statement` | (none) |
| `statement_involves` | `statement` → `person` / `tech` / `org` | (none, polymorphic TO) |

`statement_involves` is polymorphic — the TO side can be `person`, `tech`, or `org`. The SurrealDB connector supports this via listing multiple targets.

## Processing Pipeline

### Phase 1: Per-Session Processing (mounted as components, memoized)

```
input .txt files
  └─ parse URLs → list of video IDs
       └─ for each session (use_mount per video ID):
            ├─ [1] Fetch transcript: yt-dlp audio + gpt-4o-transcribe-diarize  (memo=True)
            │       → speaker-labeled transcript + yt-dlp metadata
            ├─ [2] LLM extraction: extract metadata, persons, statements  (memo=True)
            │       → SessionExtraction (name, description, date, persons, statements)
            ├─ [3] Declare Session node → SurrealDB
            ├─ [4] Declare Statement nodes + session_statement edges → SurrealDB
            └─ [5] Return raw entities (persons, techs, orgs) + statement linkages
                   for entity resolution in Phase 2
```

#### Step 1: Fetch Transcript

```python
@coco.fn(memo=True)
async def fetch_transcript(youtube_id: str) -> SessionTranscript:
    """Download audio via yt-dlp, transcribe with speaker diarization via OpenAI."""
    # 1. yt-dlp: download audio to temp file + fetch metadata (title, upload_date)
    # 2. OpenAI gpt-4o-transcribe-diarize: transcribe with speaker labels
    #    - Use chunking_strategy="auto" for audio > 30s
    #    - Response format: "diarized_json" → segments with speaker labels
    # 3. Format transcript as "Speaker 0: ...\nSpeaker 1: ..." text
    # 4. Return SessionTranscript(transcript=..., yt_metadata=...)
    ...
```

`SessionTranscript` carries the raw transcript text and yt-dlp metadata (title, upload_date) as fallback for LLM extraction.

#### Step 2: LLM Extraction (single call per session)

```python
@coco.fn(memo=True)
async def extract_session(transcript: SessionTranscript) -> SessionExtraction:
    """Give LLM the full transcript + metadata, extract everything at once."""
    client = instructor.from_litellm(litellm.acompletion)
    return await client.chat.completions.create(
        model=os.environ.get("LLM_MODEL", "gpt-5.4-mini"),
        response_model=SessionExtraction,
        messages=[
            {"role": "system", "content": EXTRACTION_PROMPT},
            {"role": "user", "content": f"Video title: {transcript.yt_title}\n"
                                        f"Upload date: {transcript.yt_upload_date}\n\n"
                                        f"Transcript:\n{transcript.transcript}"},
        ],
    )
```

The prompt instructs the LLM to:
- Name the session clearly (use video title as hint)
- Write a brief description
- Identify the date if mentioned in conversation
- List all persons who attended/spoke
- Extract thematic statements, attributing each to its speaker(s) and tagging involved persons, techs, and orgs
- Use Wikipedia-style canonical names (e.g. "Franklin D. Roosevelt", "Python (programming language)")

#### Steps 3-5: Declare + Return

```python
@coco.fn
async def process_session(
    youtube_id: str,
    session_table: surrealdb.TableTarget,
    statement_table: surrealdb.TableTarget,
    session_statement_rel: surrealdb.RelationTarget,
) -> SessionRawEntities:
    transcript = await coco.use_mount(
        coco.component_subpath("fetch"),
        fetch_transcript, youtube_id,
    )
    extraction = await coco.use_mount(
        coco.component_subpath("extract"),
        extract_session, transcript,
    )

    # Declare session node (use LLM-extracted metadata, fallback to yt-dlp)
    session = Session(
        id=youtube_id,
        name=extraction.name or transcript.yt_title,
        description=extraction.description,
        transcript=transcript.transcript,
        date=extraction.date or transcript.yt_upload_date,
    )
    session_table.declare_record(row=session)

    # Declare statements + session_statement edges
    for stmt in extraction.statements:
        stmt_id = make_id(youtube_id, stmt.statement)
        statement_table.declare_record(row=Statement(id=stmt_id, statement=stmt.statement))
        session_statement_rel.declare_relation(from_id=youtube_id, to_id=stmt_id)

    # Collect all raw entity names for Phase 2
    return SessionRawEntities(
        session_id=youtube_id,
        persons=extraction.persons_attending,
        statements=extraction.statements,
    )
```

### Phase 2: Entity Resolution

Uses **faiss** (`faiss-cpu`) for in-memory vector search. As entities are processed one by one ("bubble sort"), each embedding is added to a `faiss.IndexFlatIP` index, so nearest-neighbor queries only search among already-processed entities. `IndexFlatIP` operates on L2-normalized vectors, making inner product equivalent to cosine similarity.

For each entity type (Person, Tech, Org) independently:

```
1. Collect all raw entities from all sessions → all_raw_entities: set[str]

2. Initialize faiss index:
   index = faiss.IndexFlatIP(embedding_dim)
   index_names: list[str] = []   # maps faiss row index → entity name

3. Build deduplication_dict via "bubble sort" approach:
   For each entity in all_raw_entities:
     a. Compute embedding (memoized) via SentenceTransformerEmbedder
     b. L2-normalize the embedding
     c. Query index for top N nearest neighbors with similarity > (1 - MAX_DISTANCE)
        (if neighbor is a dup in deduplication_dict, collect its canonical instead)
     d. If candidates exist (excluding self):
        - LLM call (memoized): "Are any of these the same entity as '{entity}'?
          Pick by number, or 'none'."
        - LLM picks canonical name (or declares new canonical)
     e. Update deduplication_dict:
        - entity → None (canonical) or entity → canonical_name
        - If another existing canonical is identified as dup of current,
          update that entry too
     f. Add embedding to faiss index, append name to index_names

4. Output: deduplication_dict: dict[str, str | None]
   e.g. {"Apple Inc.": None, "Apple": "Apple Inc.", "AAPL": "Apple Inc."}
```

**Threshold defaults:**
- `MAX_DISTANCE_FOR_RESOLUTION`: 0.3 (cosine distance, i.e. similarity > 0.7)
- `N` (top candidates): 5

```python
import faiss

@coco.fn(memo=True)
async def compute_entity_embedding(name: str) -> NDArray:
    embedder = coco.use_context(EMBEDDER)
    return await embedder.embed(name)

async def resolve_entities(all_raw_entities: set[str]) -> dict[str, str | None]:
    dim = 384  # all-MiniLM-L6-v2 dimension
    index = faiss.IndexFlatIP(dim)
    index_names: list[str] = []
    dedup: dict[str, str | None] = {}

    for entity in all_raw_entities:
        embedding = await compute_entity_embedding(entity)
        faiss.normalize_L2(embedding.reshape(1, -1))

        candidates = []
        if index.ntotal > 0:
            sims, idxs = index.search(embedding.reshape(1, -1), k=min(N, index.ntotal))
            for sim, idx in zip(sims[0], idxs[0]):
                if sim >= 1.0 - MAX_DISTANCE and idx >= 0:
                    cand = index_names[idx]
                    canonical = resolve_canonical(cand, dedup)
                    if canonical != entity:
                        candidates.append(canonical)

        if candidates:
            match = await resolve_entity_pair(entity, list(set(candidates)))
            dedup[entity] = match  # None if entity is canonical, else canonical name
        else:
            dedup[entity] = None  # new canonical

        index.add(embedding.reshape(1, -1))
        index_names.append(entity)

    return dedup

@coco.fn(memo=True)
async def resolve_entity_pair(entity: str, candidates: list[str]) -> str | None:
    """LLM decides if entity matches any candidate; returns canonical or None."""
    client = instructor.from_litellm(litellm.acompletion)
    ...
```

### Phase 3: Knowledge Base Creation

With the deduplication dicts resolved, declare all remaining nodes and edges:

```python
@coco.fn
async def create_knowledge_base(
    all_session_raw: list[SessionRawEntities],
    person_dedup: dict[str, str | None],
    tech_dedup: dict[str, str | None],
    org_dedup: dict[str, str | None],
    person_table: surrealdb.TableTarget,
    tech_table: surrealdb.TableTarget,
    org_table: surrealdb.TableTarget,
    person_session_rel: surrealdb.RelationTarget,
    person_statement_rel: surrealdb.RelationTarget,
    statement_involves_rel: surrealdb.RelationTarget,
):
    # Declare canonical person/tech/org nodes
    for name, upstream in person_dedup.items():
        if upstream is None:  # This is a canonical entity
            person_table.declare_record(row=Person(id=make_id(name), name=name))
    # ... same for tech, org

    # Declare relationships using canonical names
    for session_raw in all_session_raw:
        for person_name in session_raw.persons:
            canonical = resolve_canonical(person_name, person_dedup)
            person_session_rel.declare_relation(
                from_id=make_id(canonical), to_id=session_raw.session_id)

        for stmt in session_raw.statements:
            stmt_id = make_id(session_raw.session_id, stmt.statement)
            for speaker in stmt.speakers:
                canonical = resolve_canonical(speaker, person_dedup)
                person_statement_rel.declare_relation(
                    from_id=make_id(canonical), to_id=stmt_id)
            for p in stmt.involved_persons:
                canonical = resolve_canonical(p, person_dedup)
                statement_involves_rel.declare_relation(
                    from_id=stmt_id, to_id=make_id(canonical), to_table=person_table)
            for t in stmt.involved_techs:
                canonical = resolve_canonical(t, tech_dedup)
                statement_involves_rel.declare_relation(
                    from_id=stmt_id, to_id=make_id(canonical), to_table=tech_table)
            for o in stmt.involved_orgs:
                canonical = resolve_canonical(o, org_dedup)
                statement_involves_rel.declare_relation(
                    from_id=stmt_id, to_id=make_id(canonical), to_table=org_table)
```

Helper to chase dedup chains:
```python
def resolve_canonical(name: str, dedup: dict[str, str | None]) -> str:
    while dedup.get(name) is not None:
        name = dedup[name]
    return name
```

## App Structure

```python
@coco.fn
async def app_main(input_dir: pathlib.Path) -> None:
    # --- Setup targets ---
    session_table = await surrealdb.mount_table_target(DB, "session", session_schema)
    statement_table = await surrealdb.mount_table_target(DB, "statement", statement_schema)
    person_table = await surrealdb.mount_table_target(DB, "person", person_schema)
    tech_table = await surrealdb.mount_table_target(DB, "tech", tech_schema)
    org_table = await surrealdb.mount_table_target(DB, "org", org_schema)

    session_statement_rel = await surrealdb.mount_relation_target(
        DB, "session_statement", session_table, statement_table, None)
    person_session_rel = await surrealdb.mount_relation_target(
        DB, "person_session", person_table, session_table, None)
    person_statement_rel = await surrealdb.mount_relation_target(
        DB, "person_statement", person_table, statement_table, None)
    statement_involves_rel = await surrealdb.mount_relation_target(
        DB, "statement_involves", statement_table,
        [person_table, tech_table, org_table], None)  # polymorphic TO

    # --- Phase 1: Per-session processing ---
    files = localfs.walk_dir(input_dir, path_matcher=PatternFilePathMatcher(
        included_patterns=["**/*.txt"]))

    all_session_raw: list[SessionRawEntities] = []
    for key, file in files.items():
        text = await file.read_text()
        for line in text.strip().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            youtube_id = extract_video_id(line)
            raw = await coco.use_mount(
                coco.component_subpath("session", youtube_id),
                process_session, youtube_id,
                session_table, statement_table, session_statement_rel,
            )
            all_session_raw.append(raw)

    # --- Phase 2: Entity resolution ---
    all_raw_persons = collect_all_raw(all_session_raw, "persons")
    all_raw_techs = collect_all_raw(all_session_raw, "techs")
    all_raw_orgs = collect_all_raw(all_session_raw, "orgs")

    person_dedup = await coco.use_mount(
        coco.component_subpath("resolve", "person"),
        resolve_entities, all_raw_persons)
    tech_dedup = await coco.use_mount(
        coco.component_subpath("resolve", "tech"),
        resolve_entities, all_raw_techs)
    org_dedup = await coco.use_mount(
        coco.component_subpath("resolve", "org"),
        resolve_entities, all_raw_orgs)

    # --- Phase 3: Declare knowledge base ---
    await coco.mount(
        coco.component_subpath("knowledge_base"),
        create_knowledge_base,
        all_session_raw, person_dedup, tech_dedup, org_dedup,
        person_table, tech_table, org_table,
        person_session_rel, person_statement_rel, statement_involves_rel,
    )

app = coco.App(
    coco.AppConfig(name="ConversationToKnowledge"),
    app_main,
    input_dir=pathlib.Path("./input"),
)
```

## Lifespan & Context

```python
SURREAL_DB = coco.ContextKey[surrealdb.ConnParams]("surreal_db", tracked=False)
EMBEDDER = coco.ContextKey[SentenceTransformerEmbedder]("embedder")

@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    builder.provide(SURREAL_DB, surrealdb.make_conn_params(
        url=os.environ["SURREALDB_URL"],
        namespace="cocoindex",
        database="knowledge",
        credentials=surrealdb.Credentials(
            username=os.environ.get("SURREALDB_USER", "root"),
            password=os.environ.get("SURREALDB_PASS", "root"),
        ),
    ))
    builder.provide(EMBEDDER, SentenceTransformerEmbedder(
        "sentence-transformers/all-MiniLM-L6-v2"))
    yield
```

## Environment Variables (`.env`)

```env
# Required
OPENAI_API_KEY=sk-...
SURREALDB_URL=ws://localhost:8000/rpc

# Optional (with defaults)
SURREALDB_USER=root
SURREALDB_PASS=root
LLM_MODEL=gpt-5.4-mini
```

## ID Generation

All entity IDs are deterministic hashes of their canonical name, enabling stable references:

```python
def make_id(*parts: str) -> str:
    """Deterministic ID from name parts."""
    key = "|".join(parts)
    return hashlib.sha256(key.encode()).hexdigest()[:16]
```

Relation IDs are auto-derived from `from_id + to_id` by the SurrealDB connector.

## File Structure

```
examples/conversation_to_knowledge/
├── spec.md              # Requirements (existing)
├── design.md            # This file
├── main.py              # App entry point, lifespan, app_main
├── models.py            # Pydantic + dataclass models
├── fetch.py             # YouTube download + OpenAI diarized transcription
├── extract.py           # LLM entity/metadata extraction (instructor + litellm)
├── resolve.py           # Entity resolution (embedding + LLM)
├── pyproject.toml       # Dependencies
└── input/               # Sample input files
    └── sample.txt
```

## Dependencies

```toml
[project]
dependencies = [
    "cocoindex>=1.0.0a1",
    "yt-dlp",
    "openai",
    "instructor",
    "litellm",
    "sentence-transformers",
    "faiss-cpu",
    "numpy",
    "pydantic",
    "surrealdb",
]
```

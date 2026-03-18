We want to convert a bunch of podcast sessions to knowledge base.

## Data Model

### Entities

- *Session*: a session of podcast
  - property: name, description (optional), transcript labeled with speaker, date (optional)
- *Person*: can be speakers in podcasts, or people mentioned by them
  - property: name
- *Tech*: can be a product (e.g. ChatGPT), a technique (e.g. LLM), an idea (e.g. no code)
  - property: name
- *Org*: can be a company, an organization (e.g. W3C), a divison of the government (e.g. US congress, US department of education)
  - property: name
- *Statement*: a statement about persons, techs and orgs
  - property: id, statement

Note: Properties listed above are what matters for our business logic. An auto-generated key field can be added for those entities without an simple key field, which makes it's easier to identify these entities for most database. (e.g. `id` for SurrealDB)

All names should be clear enough without ambiguity for common audience. In general, they're good Wikiepdia entry names. Examples:

- Franklin D. Roosevelt
- Large language model
- Apple Inc.
- Python (programming language)

### Relationships

- Person -*person_session*-> Session: the person attended the session
- Session -*session_statement*-> Statement: the satement was made in the session.
- Person -*person_statement*-> Statement: the statement was made by the person.
- Statement -*statement_involves*-> Person / Tech / Org: the statement involves these person / tech / orgs.

## Supported podcast sources

At the current version, we only support YouTube

## Processing Flow

### Per-session processing

Users provide a folder, with a list of files, each with a list of source video locations (e.g. YouTube Video ID).

Processing for each session should be mounted as a component, and memoized.
The component does processing for individual session, and declare target states that don't need cross-session entity resolutions.
For things that need cross-session entity resolutions, it returns them (i.e. we should use `use_mount()`), for later stages to consume.

#### Get Session

For each video, we fetch and convert it to a Session entity.

#### Extract raw Person, raw Tech, raw Org, Statements

We extract Persons attended in the Session.

We extract Statements from the Session. A statement contains the following entities:
  - The Persons who made the statement
  - The Persons, Techs and Orgs that the statement involves

#### Declare target states for Sessions and Statements

The Sessions and Statements are final, so we can directly declare them as nodes together with the *session_statement* relationship in the target database here, so we won't need to carry these entities (especially Session with large text blobs) for later processing.

Entities involved in statements above are *raw* entities, as they need to be resolved later.

### Entity Resolution for Person, Tech and Org

We do entity resolution for each entity type separately. For each one, we leverage in-memory embedding match. Here's our approach for each type of entity:

The output we want is a *deduplication dict* with type `dict[str, str | Literal['True']]`, i.e. `name -> canonical_name | None` where `None` means the `name` itself is canonical. e.g. `{'A': None, 'B': 'A'}` means `A` is identified as a canonical upstream of `B`.
Note that ther can be multiple hops in the chain, e.g. `{'A': None, 'B': 'A', 'C': 'B'}`. And to find the canoinical of a given name, we need to iterate until reaching the one with value `None`.

To construct the deduplication dict, we need to:

1. Get the set of all raw entities (`all_raw_entities`).

2. For each item in `all_raw_entities`, we compute (memoized!) its embedding. Now we have a `entity_embedding_map`.

3. Then do a process similar to "bubble sort", i.e. for each `entity` in `all_raw_entities`, we
   - Find *candidates of duplicated entities* by finding the nearest entities in `entity_embedding_map` such that
     - The entity is already processed (i.e. exists in `duplication_dict`). If it's already a duplication of others, collect the canonical instead.
     - The distance is under a certain threshold `MAX_DISTANCE_FOR_RESOLUTION`
     - We only pick the top `N` entities with least distance to the current entity in  `entity_embedding_map`
   - If there're more than 0 (excluding the current entity), we need to invoke LLM to do a resolution: let LLM to answer if any of them mean the same thing as the current one, and let LLM to pick which one to be used as canonical. Let the LLM to pick by numbers. (memoized!)
   - With this, we can update `duplication_dict`:
     - Put the current `entity` into `duplication_dict`: `None` (if canonical) or the canonical one
     - If another one is dup of the current `entity`, update the dict entry for the other instead, to mark it as a dup of the current.


### Knowedge Base Creation

Now, with the deduplication dict, we have our canonical entities and our relationships pointing to our canoical entities. We can declare the entire knowledge base.


## Technology

Use CocoIndex for processing.

Use SurrealDB for target knowledge database. CocoIndex has a target connector for it.

Use Pydantic for various models. Use instructor + LiteLLM to talk with LLM and get structured output from it.

Use SentenceTransformerEmbedder for embedding.

For YouTube audio fetching and conversion, `yt-dlp` + `pyannote` is one option I've heard. I'm open to other online options especially if it can offer higher conversion speed and easy to setup (e.g. we already have OpenAI API key, so any OpenAI service will be very easy for us to use).

For others, please make your own judgement.

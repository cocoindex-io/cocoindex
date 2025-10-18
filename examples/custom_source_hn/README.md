# HackerNews Custom Source Example

This example demonstrates how to create and use a custom source connector with CocoIndex to index data from the HackerNews API. Everything is contained in a single `main.py` file for simplicity. It showcases the complete workflow of:

1. **Custom Source Implementation**: Creating a source connector that fetches data from external APIs
2. **Data Processing**: Handling complex nested data (threads with flattened comments)
3. **Real-time Indexing**: Building searchable indexes that stay up-to-date with external data
4. **Search Interface**: Querying the indexed data with full-text search capabilities

## Features

- **HackerNews API Integration**: Fetches threads and comments using the official HackerNews search and items APIs
- **Streaming Data Access**: Implements efficient streaming to handle large datasets
- **Comment Flattening**: Recursively flattens nested comment threads into searchable records
- **Ordinal Support**: Uses `updated_at` timestamps to track data freshness
- **Full-text Search**: Enables powerful text search across both threads and comments
- **Auto-refresh**: Periodically syncs with HackerNews to get the latest content

## Setup

1. **Install Dependencies**:
   ```bash
   pip install -e .
   ```

2. **Set up Database**:
   Create a `.env` file with your PostgreSQL connection:
   ```
   COCOINDEX_DATABASE_URL=postgresql://username:password@localhost:5432/database
   ```

3. **Run the Example**:
   ```bash
   python main.py
   ```

## How It Works

### Custom Source Implementation

The `HackerNewsConnector` class is decorated with `@source_connector` and implements the required interface:

```python
@source_connector(spec_cls=HackerNewsSourceSpec, key_type=str, value_type=HackerNewsThread)
class HackerNewsConnector:
    async def list_async(self, options: SourceReadOptions):
        # Fetch threads using HN search API
        # Returns: AsyncIterator[tuple[str, dict]]

    async def get_value_async(self, key: str, options: SourceReadOptions):
        # Fetch specific thread using HN items API
        # Returns: PartialSourceRow[str, HackerNewsThread]

    def provides_ordinal(self) -> bool:
        # Indicates this source provides timestamp ordering
        return True
```

### Data Schema

Each HackerNews thread is structured as:

```python
@dataclasses.dataclass
class HackerNewsComment:
    id: str                           # Comment ID
    author: str                       # Comment author
    text: str                         # Comment text
    created_at: int                   # Creation timestamp

@dataclasses.dataclass
class HackerNewsThread:
    id: str                           # Thread ID
    title: str                        # Thread title
    author: str                       # Thread author
    url: str = ""                     # External URL (if any)
    type: str = ""                    # Thread type (story, ask, etc.)
    text: str = ""                    # Thread content
    created_at: int = 0               # Creation timestamp
    comments: list[HackerNewsComment] = None  # Flattened comments as dataclass objects
```

### Flow Definition

The flow creates two searchable indexes:

1. **Thread Index**: Searchable thread titles and content
2. **Comment Index**: Searchable comments with thread context

```python
@cocoindex.flow_def(name="HackerNewsIndex")
def hackernews_flow(flow_builder, data_scope):
    # Add source with refresh interval
    data_scope["threads"] = flow_builder.add_source(
        HackerNewsSourceSpec(
            query="python AI machine learning",
            max_results=50,
        ),
        refresh_interval=timedelta(minutes=30),
    )
```

## API Endpoints Used

- **Search API**: `https://hn.algolia.com/api/v1/search`
  - Used by `list_async()` to discover threads
  - Supports query parameters for filtering

- **Items API**: `https://hn.algolia.com/api/v1/items/{id}`
  - Used by `get_value_async()` to fetch full thread details
  - Includes complete comment trees

## Search Capabilities

The example provides two search handlers:

1. **Thread Search**: Search thread titles and content
2. **Comment Search**: Search comment text with thread context

Both use PostgreSQL's full-text search capabilities for efficient querying.

## Customization

You can customize the source by modifying:

- **Search Query**: Change the HackerNews search terms
- **Max Results**: Adjust the number of threads to index
- **Refresh Interval**: Change how often data is synced
- **Schema**: Add or modify fields in the thread structure
- **Search Logic**: Enhance the search algorithms

## Learning Points

This example demonstrates several important CocoIndex concepts:

1. **Custom Sources**: How to integrate any external API as a data source
2. **Streaming**: Efficient handling of large datasets with async iterators
3. **Complex Schemas**: Working with nested data structures
4. **Ordinal Tracking**: Using timestamps for data freshness
5. **Search Integration**: Building searchable indexes from structured data

The pattern shown here can be adapted for any external API or data source, making it a powerful template for custom integrations.

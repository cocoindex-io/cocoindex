# CocoIndex Extras

This package provides extra utilities for common data processing tasks in CocoIndex.

## Modules

### `sentence_transformers`

Provides integration with the [sentence-transformers](https://www.sbert.net/) library for text embeddings.

**Key class: `SentenceTransformerEmbedder`**

A wrapper around SentenceTransformer models that:

- Implements `VectorSchemaProvider` for seamless integration with CocoIndex connectors
- Handles model caching and thread-safe GPU access automatically
- Provides a simple `encode(text: str)` method that returns a 1D embedding vector
- Returns properly typed numpy arrays

**Basic usage:**

```python
from cocoindex.extras.sentence_transformers import SentenceTransformerEmbedder

# Initialize embedder
embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")

# Encode text
embedding = embedder.encode("Hello, world!")  # Shape: (384,)

# Get vector schema for database integration
schema = embedder.__coco_vector_schema__()
print(f"Dimension: {schema.size}, dtype: {schema.dtype}")
```

**Using with CocoIndex connectors:**

```python
from dataclasses import dataclass
from typing import Annotated
from numpy.typing import NDArray

@dataclass
class DocEmbedding:
    text: str
    # Use embedder as a VectorSchemaProvider in type annotations
    embedding: Annotated[NDArray, embedder]
```

### `text`

Provides text processing utilities including:
- `detect_code_language()`: Detect programming language from filename
- `SeparatorSplitter`: Split text by regex separators
- `RecursiveSplitter`: Advanced text chunking with language awareness
- `CustomLanguageConfig`: Define custom language splitting rules

See the module docstrings for detailed usage examples.

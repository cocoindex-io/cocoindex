# Text Processing

The `cocoindex.ops.text` module provides operations for text processing, including:

- Code language detection
- Text chunking and splitting
- Syntax-aware code splitting

## Available functions and classes

### `detect_code_language()`

Detect the programming language from a filename.

**Usage:**

```python
from cocoindex.ops.text import detect_code_language

language = detect_code_language(filename="main.py")
print(language)  # "python"

language = detect_code_language(filename="app.rs")
print(language)  # "rust"

language = detect_code_language(filename="unknown.xyz")
print(language)  # None
```

### `SeparatorSplitter`

Split text by regex separators.

**Usage:**

```python
from cocoindex.ops.text import SeparatorSplitter

splitter = SeparatorSplitter()

text = "First sentence. Second sentence. Third sentence."
chunks = splitter.split(
    text,
    chunk_size=100,
    chunk_overlap=20,
    separators=[r"\.\s+"]  # Split on periods followed by whitespace
)

for chunk in chunks:
    print(chunk.text)
```

### `RecursiveSplitter`

Advanced text chunking with language awareness and syntax-aware splitting for code.

**Features:**
- Supports many programming languages
- Preserves code structure
- Customizable chunk sizes and overlap
- Returns chunks with position information

**Usage:**

```python
from cocoindex.ops.text import RecursiveSplitter

splitter = RecursiveSplitter()

# Split markdown text
text = "# Title\n\nParagraph 1.\n\nParagraph 2."
chunks = splitter.split(
    text,
    chunk_size=2000,
    chunk_overlap=500,
    language="markdown"
)

for chunk in chunks:
    print(f"Chunk: {chunk.text}")
    print(f"Start: line {chunk.start.line}, char {chunk.start.char_offset}")
    print(f"End: line {chunk.end.line}, char {chunk.end.char_offset}")
```

**Language-aware code splitting:**

```python
# Split Python code
python_code = '''
def hello():
    print("Hello, world!")

def goodbye():
    print("Goodbye!")
'''

chunks = splitter.split(
    python_code,
    chunk_size=1000,
    min_chunk_size=300,
    chunk_overlap=300,
    language="python"
)
```

**Supported languages:**
- Python
- Rust
- JavaScript/TypeScript
- Markdown
- And many more...

### `CustomLanguageConfig`

Define custom language splitting rules.

**Usage:**

```python
from cocoindex.ops.text import CustomLanguageConfig, RecursiveSplitter

# Create custom language config for abstracts
abstract_config = CustomLanguageConfig(
    language_name="abstract",
    separators_regex=[
        r"[.?!]+\s+",  # Sentence boundaries
        r"[:;]\s+",     # Clause boundaries
        r",\s+",        # Comma boundaries
        r"\s+",         # Whitespace
    ]
)

splitter = RecursiveSplitter(custom_languages=[abstract_config])

chunks = splitter.split(
    "This is a sample abstract. It has multiple sentences...",
    chunk_size=500,
    chunk_overlap=150,
    language="abstract"
)
```

## API reference

For detailed API documentation, refer to the module docstrings:

```python
from cocoindex.ops import text

help(text.RecursiveSplitter)
help(text.SeparatorSplitter)
help(text.detect_code_language)
help(text.CustomLanguageConfig)
```

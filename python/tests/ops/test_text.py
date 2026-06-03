"""Tests for cocoindex.ops.text module."""

from cocoindex.ops.text import (
    detect_code_language,
    SeparatorSplitter,
    CustomLanguageConfig,
    RecursiveSplitter,
)
from cocoindex.resources.chunk import Chunk, TextPosition


def test_detect_code_language_known_extensions() -> None:
    """Test detect_code_language with known file extensions."""
    assert detect_code_language(filename="main.py") == "python"
    assert detect_code_language(filename="app.rs") == "rust"
    assert detect_code_language(filename="index.js") == "javascript"
    assert detect_code_language(filename="style.css") == "css"
    assert detect_code_language(filename="App.svelte") == "svelte"
    assert detect_code_language(filename="App.vue") == "vue"
    assert detect_code_language(filename="script.jl") == "julia"
    assert detect_code_language(filename="Main.elm") == "elm"
    assert detect_code_language(filename="index.astro") == "astro"
    assert detect_code_language(filename="deploy.sh") == "bash"
    assert detect_code_language(filename="CMakeLists.cmake") == "cmake"
    assert detect_code_language(filename="main.tf") == "hcl"


def test_detect_code_language_unknown_extension() -> None:
    """Test detect_code_language with unknown file extension."""
    assert detect_code_language(filename="file.xyz") is None
    assert detect_code_language(filename="noextension") is None


def test_separator_splitter_basic() -> None:
    """Test SeparatorSplitter with basic paragraph splitting."""
    splitter = SeparatorSplitter([r"\n\n+"])
    chunks = splitter.split("Para1\n\nPara2\n\nPara3")

    assert len(chunks) == 3
    assert chunks[0].text == "Para1"
    assert chunks[1].text == "Para2"
    assert chunks[2].text == "Para3"


def test_separator_splitter_returns_chunk_type() -> None:
    """Test that SeparatorSplitter returns proper Chunk objects."""
    splitter = SeparatorSplitter([r"\n"])
    chunks = splitter.split("Line1\nLine2")

    assert len(chunks) == 2
    assert isinstance(chunks[0], Chunk)
    assert isinstance(chunks[0].start, TextPosition)
    assert isinstance(chunks[0].end, TextPosition)


def test_separator_splitter_position_info() -> None:
    """Test that SeparatorSplitter returns correct position information."""
    splitter = SeparatorSplitter([r"\n"])
    chunks = splitter.split("Line1\nLine2")

    # First chunk
    assert chunks[0].text == "Line1"
    assert chunks[0].start.byte_offset == 0
    assert chunks[0].start.line == 1
    assert chunks[0].start.column == 1
    assert chunks[0].end.byte_offset == 5

    # Second chunk
    assert chunks[1].text == "Line2"
    assert chunks[1].start.line == 2
    assert chunks[1].start.column == 1


def test_separator_splitter_keep_separator_left() -> None:
    """Test SeparatorSplitter with keep_separator='left'."""
    splitter = SeparatorSplitter([r"\."], keep_separator="left")
    chunks = splitter.split("A. B. C")

    assert len(chunks) == 3
    assert chunks[0].text == "A."
    assert chunks[1].text == "B."
    assert chunks[2].text == "C"


def test_separator_splitter_keep_separator_right() -> None:
    """Test SeparatorSplitter with keep_separator='right'."""
    splitter = SeparatorSplitter([r"\."], keep_separator="right")
    chunks = splitter.split("A. B. C")

    assert len(chunks) == 3
    assert chunks[0].text == "A"
    assert chunks[1].text == ". B"
    assert chunks[2].text == ". C"


def test_separator_splitter_trim() -> None:
    """Test SeparatorSplitter with trim option."""
    splitter = SeparatorSplitter([r"\|"], trim=True)
    chunks = splitter.split("  A  |  B  ")

    assert chunks[0].text == "A"
    assert chunks[1].text == "B"


def test_separator_splitter_no_trim() -> None:
    """Test SeparatorSplitter with trim=False."""
    splitter = SeparatorSplitter([r"\|"], trim=False)
    chunks = splitter.split("  A  |  B  ")

    assert chunks[0].text == "  A  "
    assert chunks[1].text == "  B  "


def test_separator_splitter_reuse() -> None:
    """Test that SeparatorSplitter can be reused for multiple texts."""
    splitter = SeparatorSplitter([r"\n\n+"])

    chunks1 = splitter.split("A\n\nB")
    chunks2 = splitter.split("X\n\nY\n\nZ")

    assert len(chunks1) == 2
    assert len(chunks2) == 3


def test_recursive_splitter_basic() -> None:
    """Test RecursiveSplitter with basic text."""
    splitter = RecursiveSplitter()
    chunks = splitter.split("Short text.", chunk_size=100)

    assert len(chunks) >= 1
    assert all(isinstance(c, Chunk) for c in chunks)


def test_recursive_splitter_returns_chunk_type() -> None:
    """Test that RecursiveSplitter returns proper Chunk objects."""
    splitter = RecursiveSplitter()
    chunks = splitter.split("Some text here.", chunk_size=100)

    assert len(chunks) >= 1
    assert isinstance(chunks[0], Chunk)
    assert isinstance(chunks[0].start, TextPosition)
    assert isinstance(chunks[0].end, TextPosition)


def test_recursive_splitter_with_language() -> None:
    """Test RecursiveSplitter with language parameter."""
    splitter = RecursiveSplitter()
    code = "def foo():\n    pass\n\ndef bar():\n    pass"
    chunks = splitter.split(code, chunk_size=30, language="python")

    assert len(chunks) >= 1
    assert all(isinstance(c, Chunk) for c in chunks)


def test_recursive_splitter_with_svelte() -> None:
    """Test RecursiveSplitter with Svelte syntax-aware splitting."""
    splitter = RecursiveSplitter()
    code = (
        '<script lang="ts">\n'
        "  let count = 0;\n"
        "  function increment() { count += 1; }\n"
        "</script>\n\n"
        "<button on:click={increment}>\n"
        "  Clicked {count} times\n"
        "</button>\n\n"
        "<style>\n  button { color: red; }\n</style>\n"
    )
    chunks = splitter.split(code, chunk_size=80, min_chunk_size=20, language="svelte")

    assert len(chunks) >= 1
    assert all(isinstance(c, Chunk) for c in chunks)


def test_recursive_splitter_with_julia() -> None:
    """Test RecursiveSplitter with Julia syntax-aware splitting."""
    splitter = RecursiveSplitter()
    code = (
        "function foo(x)\n    return x + 1\nend\n\n"
        "struct Point\n    x::Int\n    y::Int\nend\n\n"
        'module MyModule\n    export hello\n    hello() = println("hi")\nend\n'
    )
    chunks = splitter.split(code, chunk_size=60, min_chunk_size=20, language="julia")

    assert len(chunks) >= 1
    assert all(isinstance(c, Chunk) for c in chunks)


def test_recursive_splitter_with_vue() -> None:
    """Test RecursiveSplitter with Vue syntax-aware splitting."""
    splitter = RecursiveSplitter()
    code = (
        "<template>\n"
        '  <div class="hello">\n'
        "    <h1>{{ msg }}</h1>\n"
        '    <button @click="increment">Count is: {{ count }}</button>\n'
        "  </div>\n"
        "</template>\n\n"
        "<script>\n"
        "export default {\n"
        "  data() { return { msg: 'Hello', count: 0 } },\n"
        "  methods: { increment() { this.count += 1 } },\n"
        "}\n"
        "</script>\n\n"
        "<style scoped>\n.hello { color: blue; }\n</style>\n"
    )
    chunks = splitter.split(code, chunk_size=80, min_chunk_size=20, language="vue")

    assert len(chunks) >= 1
    assert all(isinstance(c, Chunk) for c in chunks)


def test_recursive_splitter_with_elm() -> None:
    """Test RecursiveSplitter with Elm syntax-aware splitting."""
    splitter = RecursiveSplitter()
    code = (
        "module Main exposing (main)\n\n"
        "import Html exposing (text)\n\n"
        "greet : String -> String\n"
        'greet name =\n    "Hello, " ++ name ++ "!"\n\n'
        "main =\n"
        '    text (greet "World")\n'
    )
    chunks = splitter.split(code, chunk_size=60, min_chunk_size=20, language="elm")

    assert len(chunks) >= 1
    assert all(isinstance(c, Chunk) for c in chunks)


def test_recursive_splitter_with_astro() -> None:
    """Test RecursiveSplitter with Astro syntax-aware splitting."""
    splitter = RecursiveSplitter()
    code = (
        "---\n"
        'const title = "Hello";\n'
        "---\n\n"
        "<html>\n"
        "  <head><title>{title}</title></head>\n"
        "  <body>\n"
        "    <h1>{title}</h1>\n"
        "  </body>\n"
        "</html>\n"
    )
    chunks = splitter.split(code, chunk_size=60, min_chunk_size=20, language="astro")

    assert len(chunks) >= 1
    assert all(isinstance(c, Chunk) for c in chunks)


def test_recursive_splitter_reuse() -> None:
    """Test that RecursiveSplitter can be reused for multiple texts."""
    splitter = RecursiveSplitter()

    chunks1 = splitter.split("Text one.", chunk_size=100)
    chunks2 = splitter.split("Text two is longer.", chunk_size=100)

    assert len(chunks1) >= 1
    assert len(chunks2) >= 1


def test_custom_language_config() -> None:
    """Test RecursiveSplitter with custom language configuration."""
    config = CustomLanguageConfig(
        language_name="myformat",
        separators_regex=[r"---"],
        aliases=["mf"],
    )
    splitter = RecursiveSplitter(custom_languages=[config])

    chunks = splitter.split(
        "Part1---Part2---Part3",
        chunk_size=10,
        min_chunk_size=3,
        language="myformat",
    )

    assert len(chunks) == 3
    assert chunks[0].text == "Part1"
    assert chunks[1].text == "Part2"
    assert chunks[2].text == "Part3"


def test_custom_language_config_alias() -> None:
    """Test that custom language aliases work."""
    config = CustomLanguageConfig(
        language_name="myformat",
        separators_regex=[r"---"],
        aliases=["mf"],
    )
    splitter = RecursiveSplitter(custom_languages=[config])

    # Use alias instead of full name
    chunks = splitter.split(
        "PartA---PartB",
        chunk_size=10,
        min_chunk_size=3,
        language="mf",
    )

    assert len(chunks) == 2
    assert chunks[0].text == "PartA"
    assert chunks[1].text == "PartB"


def test_recursive_splitter_with_bash() -> None:
    """Test RecursiveSplitter with Bash syntax-aware splitting."""
    splitter = RecursiveSplitter()
    code = (
        "#!/usr/bin/env bash\n\n"
        "greet() {\n"
        '    echo "Hello, $1!"\n'
        "}\n\n"
        "for name in Alice Bob; do\n"
        '    greet "$name"\n'
        "done\n"
    )
    chunks = splitter.split(code, chunk_size=60, min_chunk_size=20, language="bash")

    assert len(chunks) >= 1
    assert all(isinstance(c, Chunk) for c in chunks)


def test_recursive_splitter_with_cmake() -> None:
    """Test RecursiveSplitter with CMake syntax-aware splitting."""
    splitter = RecursiveSplitter()
    code = (
        "cmake_minimum_required(VERSION 3.20)\n"
        "project(MyProject)\n\n"
        "function(add_my_target name)\n"
        "    add_executable(${name} main.cpp)\n"
        "    target_compile_features(${name} PRIVATE cxx_std_17)\n"
        "endfunction()\n\n"
        "add_my_target(app)\n"
    )
    chunks = splitter.split(code, chunk_size=80, min_chunk_size=20, language="cmake")

    assert len(chunks) >= 1
    assert all(isinstance(c, Chunk) for c in chunks)


def test_recursive_splitter_with_hcl() -> None:
    """Test RecursiveSplitter with HCL/Terraform syntax-aware splitting."""
    splitter = RecursiveSplitter()
    code = (
        'terraform {\n  required_version = ">= 1.0"\n}\n\n'
        'resource "aws_s3_bucket" "example" {\n'
        '  bucket = "my-bucket"\n\n'
        "  tags = {\n"
        '    Name = "example"\n'
        "  }\n"
        "}\n\n"
        'output "bucket_name" {\n'
        "  value = aws_s3_bucket.example.bucket\n"
        "}\n"
    )
    chunks = splitter.split(code, chunk_size=80, min_chunk_size=20, language="hcl")

    assert len(chunks) >= 1
    assert all(isinstance(c, Chunk) for c in chunks)


def test_recursive_splitter_with_python() -> None:
    """Test RecursiveSplitter with Python syntax-aware splitting."""
    splitter = RecursiveSplitter()
    code = (
        "import os\n\n\n"
        "def read_file(path: str) -> str:\n"
        "    with open(path) as f:\n"
        "        return f.read()\n\n\n"
        "class FileProcessor:\n"
        "    def __init__(self, root: str) -> None:\n"
        "        self.root = root\n\n"
        "    def process(self) -> list[str]:\n"
        "        return [read_file(p) for p in os.listdir(self.root)]\n"
    )
    chunks = splitter.split(code, chunk_size=80, min_chunk_size=20, language="python")

    assert len(chunks) >= 1
    assert all(isinstance(c, Chunk) for c in chunks)


def test_recursive_splitter_with_rust() -> None:
    """Test RecursiveSplitter with Rust syntax-aware splitting."""
    splitter = RecursiveSplitter()
    code = (
        "use std::collections::HashMap;\n\n"
        "pub struct Counter {\n"
        "    counts: HashMap<String, usize>,\n"
        "}\n\n"
        "impl Counter {\n"
        "    pub fn new() -> Self {\n"
        "        Self { counts: HashMap::new() }\n"
        "    }\n\n"
        "    pub fn add(&mut self, key: &str) {\n"
        "        *self.counts.entry(key.to_string()).or_insert(0) += 1;\n"
        "    }\n"
        "}\n\n"
        "fn main() {\n"
        "    let mut c = Counter::new();\n"
        '    c.add("hello");\n'
        "}\n"
    )
    chunks = splitter.split(code, chunk_size=80, min_chunk_size=20, language="rust")

    assert len(chunks) >= 1
    assert all(isinstance(c, Chunk) for c in chunks)


def test_recursive_splitter_with_go() -> None:
    """Test RecursiveSplitter with Go syntax-aware splitting."""
    splitter = RecursiveSplitter()
    code = (
        "package main\n\n"
        'import "fmt"\n\n'
        "type Greeter struct {\n"
        "    Name string\n"
        "}\n\n"
        "func (g Greeter) Greet() string {\n"
        '    return fmt.Sprintf("Hello, %s!", g.Name)\n'
        "}\n\n"
        "func main() {\n"
        '    g := Greeter{Name: "World"}\n'
        "    fmt.Println(g.Greet())\n"
        "}\n"
    )
    chunks = splitter.split(code, chunk_size=80, min_chunk_size=20, language="go")

    assert len(chunks) >= 1
    assert all(isinstance(c, Chunk) for c in chunks)


def test_recursive_splitter_with_java() -> None:
    """Test RecursiveSplitter with Java syntax-aware splitting."""
    splitter = RecursiveSplitter()
    code = (
        "import java.util.List;\n\n"
        "public class DataProcessor {\n"
        "    private final List<String> items;\n\n"
        "    public DataProcessor(List<String> items) {\n"
        "        this.items = items;\n"
        "    }\n\n"
        "    public int count() {\n"
        "        return items.size();\n"
        "    }\n\n"
        "    public static void main(String[] args) {\n"
        '        DataProcessor p = new DataProcessor(List.of("a", "b"));\n'
        "        System.out.println(p.count());\n"
        "    }\n"
        "}\n"
    )
    chunks = splitter.split(code, chunk_size=80, min_chunk_size=20, language="java")

    assert len(chunks) >= 1
    assert all(isinstance(c, Chunk) for c in chunks)


def test_recursive_splitter_with_typescript() -> None:
    """Test RecursiveSplitter with TypeScript syntax-aware splitting."""
    splitter = RecursiveSplitter()
    code = (
        "interface User {\n"
        "  id: number;\n"
        "  name: string;\n"
        "}\n\n"
        "function greet(user: User): string {\n"
        "  return `Hello, ${user.name}!`;\n"
        "}\n\n"
        "class UserService {\n"
        "  private users: User[] = [];\n\n"
        "  add(user: User): void {\n"
        "    this.users.push(user);\n"
        "  }\n\n"
        "  getAll(): User[] {\n"
        "    return this.users;\n"
        "  }\n"
        "}\n"
    )
    chunks = splitter.split(
        code, chunk_size=80, min_chunk_size=20, language="typescript"
    )

    assert len(chunks) >= 1
    assert all(isinstance(c, Chunk) for c in chunks)


def test_recursive_splitter_with_json() -> None:
    """Test RecursiveSplitter with JSON syntax-aware splitting."""
    splitter = RecursiveSplitter()
    code = (
        "{\n"
        '  "name": "cocoindex",\n'
        '  "version": "1.0.0",\n'
        '  "dependencies": {\n'
        '    "numpy": ">=1.24",\n'
        '    "pydantic": ">=2.0"\n'
        "  },\n"
        '  "scripts": {\n'
        '    "test": "pytest",\n'
        '    "build": "maturin develop"\n'
        "  }\n"
        "}\n"
    )
    chunks = splitter.split(code, chunk_size=80, min_chunk_size=20, language="json")

    assert len(chunks) >= 1
    assert all(isinstance(c, Chunk) for c in chunks)

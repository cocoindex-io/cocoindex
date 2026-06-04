//! Port of `python/tests/ops/test_text.py`.
//!
//! Run with: `cargo test -p cocoindex --features text --test ops_text`.
#![cfg(feature = "text")]

use cocoindex::ops::text::{
    CustomLanguageConfig, KeepSeparator, RecursiveSplitter, SeparatorSplitConfig,
    SeparatorSplitter, detect_code_language,
};

#[test]
fn detect_code_language_known_extensions() {
    assert_eq!(detect_code_language("main.py").as_deref(), Some("python"));
    assert_eq!(detect_code_language("app.rs").as_deref(), Some("rust"));
    assert_eq!(
        detect_code_language("index.js").as_deref(),
        Some("javascript")
    );
    assert_eq!(detect_code_language("style.css").as_deref(), Some("css"));
    assert_eq!(
        detect_code_language("App.svelte").as_deref(),
        Some("svelte")
    );
    assert_eq!(detect_code_language("App.vue").as_deref(), Some("vue"));
    assert_eq!(detect_code_language("script.jl").as_deref(), Some("julia"));
    assert_eq!(detect_code_language("Main.elm").as_deref(), Some("elm"));
    assert_eq!(
        detect_code_language("index.astro").as_deref(),
        Some("astro")
    );
    assert_eq!(detect_code_language("deploy.sh").as_deref(), Some("bash"));
    assert_eq!(
        detect_code_language("CMakeLists.cmake").as_deref(),
        Some("cmake")
    );
    assert_eq!(detect_code_language("main.tf").as_deref(), Some("hcl"));
}

#[test]
fn detect_code_language_unknown_extension() {
    assert_eq!(detect_code_language("file.xyz"), None);
    assert_eq!(detect_code_language("noextension"), None);
}

#[test]
fn separator_splitter_basic() {
    let splitter = SeparatorSplitter::new([r"\n\n+"]).unwrap();
    let text = "Para1\n\nPara2\n\nPara3";
    let chunks = splitter.split(text);

    assert_eq!(chunks.len(), 3);
    assert_eq!(chunks[0].text(text), "Para1");
    assert_eq!(chunks[1].text(text), "Para2");
    assert_eq!(chunks[2].text(text), "Para3");
}

#[test]
fn separator_splitter_position_info() {
    let splitter = SeparatorSplitter::new([r"\n"]).unwrap();
    let text = "Line1\nLine2";
    let chunks = splitter.split(text);

    assert_eq!(chunks.len(), 2);
    // First chunk
    assert_eq!(chunks[0].text(text), "Line1");
    assert_eq!(chunks[0].start.byte_offset, 0);
    assert_eq!(chunks[0].start.line, 1);
    assert_eq!(chunks[0].start.column, 1);
    assert_eq!(chunks[0].end.byte_offset, 5);
    // Second chunk
    assert_eq!(chunks[1].text(text), "Line2");
    assert_eq!(chunks[1].start.line, 2);
    assert_eq!(chunks[1].start.column, 1);
}

#[test]
fn separator_splitter_keep_separator_left() {
    let splitter = SeparatorSplitter::with_config(SeparatorSplitConfig {
        separators_regex: vec![r"\.".to_string()],
        keep_separator: Some(KeepSeparator::Left),
        ..Default::default()
    })
    .unwrap();
    let text = "A. B. C";
    let chunks = splitter.split(text);

    assert_eq!(chunks.len(), 3);
    assert_eq!(chunks[0].text(text), "A.");
    assert_eq!(chunks[1].text(text), "B.");
    assert_eq!(chunks[2].text(text), "C");
}

#[test]
fn separator_splitter_keep_separator_right() {
    let splitter = SeparatorSplitter::with_config(SeparatorSplitConfig {
        separators_regex: vec![r"\.".to_string()],
        keep_separator: Some(KeepSeparator::Right),
        ..Default::default()
    })
    .unwrap();
    let text = "A. B. C";
    let chunks = splitter.split(text);

    assert_eq!(chunks.len(), 3);
    assert_eq!(chunks[0].text(text), "A");
    assert_eq!(chunks[1].text(text), ". B");
    assert_eq!(chunks[2].text(text), ". C");
}

#[test]
fn separator_splitter_trim_and_no_trim() {
    let text = "  A  |  B  ";

    let trimmed = SeparatorSplitter::new([r"\|"]).unwrap().split(text);
    assert_eq!(trimmed[0].text(text), "A");
    assert_eq!(trimmed[1].text(text), "B");

    let untrimmed = SeparatorSplitter::with_config(SeparatorSplitConfig {
        separators_regex: vec![r"\|".to_string()],
        trim: false,
        ..Default::default()
    })
    .unwrap()
    .split(text);
    assert_eq!(untrimmed[0].text(text), "  A  ");
    assert_eq!(untrimmed[1].text(text), "  B  ");
}

#[test]
fn separator_splitter_reuse() {
    let splitter = SeparatorSplitter::new([r"\n\n+"]).unwrap();
    assert_eq!(splitter.split("A\n\nB").len(), 2);
    assert_eq!(splitter.split("X\n\nY\n\nZ").len(), 3);
}

#[test]
fn recursive_splitter_basic() {
    let splitter = RecursiveSplitter::new().unwrap();
    let chunks = splitter.split("Short text.", 100);
    assert!(!chunks.is_empty());
}

/// Mirror the per-language smoke tests: each language should produce at least
/// one chunk whose `text` round-trips against the source.
#[test]
fn recursive_splitter_with_languages() {
    let splitter = RecursiveSplitter::new().unwrap();
    let cases: &[(&str, &str)] = &[
        ("python", "def foo():\n    pass\n\ndef bar():\n    pass"),
        (
            "svelte",
            "<script lang=\"ts\">\n  let count = 0;\n</script>\n\n<button>{count}</button>\n\n<style>\n  button { color: red; }\n</style>\n",
        ),
        (
            "julia",
            "function foo(x)\n    return x + 1\nend\n\nstruct Point\n    x::Int\nend\n",
        ),
        (
            "vue",
            "<template>\n  <h1>{{ msg }}</h1>\n</template>\n\n<script>\nexport default { data() { return { msg: 'Hi' } } }\n</script>\n",
        ),
        (
            "elm",
            "module Main exposing (main)\n\nimport Html exposing (text)\n\nmain =\n    text \"World\"\n",
        ),
        (
            "astro",
            "---\nconst title = \"Hello\";\n---\n\n<html>\n  <h1>{title}</h1>\n</html>\n",
        ),
        (
            "bash",
            "#!/usr/bin/env bash\n\ngreet() {\n    echo \"Hello, $1!\"\n}\n\ngreet World\n",
        ),
        (
            "cmake",
            "cmake_minimum_required(VERSION 3.20)\nproject(MyProject)\n\nfunction(add_my_target name)\n    add_executable(${name} main.cpp)\nendfunction()\n",
        ),
        (
            "hcl",
            "terraform {\n  required_version = \">= 1.0\"\n}\n\nresource \"aws_s3_bucket\" \"example\" {\n  bucket = \"my-bucket\"\n}\n",
        ),
    ];
    for (language, code) in cases {
        let chunks = splitter.split_with(
            code,
            cocoindex::ops::text::RecursiveChunkConfig {
                chunk_size: 80,
                min_chunk_size: Some(20),
                chunk_overlap: None,
                language: Some(language.to_string()),
            },
        );
        assert!(
            !chunks.is_empty(),
            "language `{language}` produced no chunks"
        );
        for chunk in &chunks {
            // text(source) must be a valid slice of the original source.
            assert_eq!(chunk.text(code), &code[chunk.range()]);
        }
    }
}

#[test]
fn custom_language_config_and_alias() {
    let config = CustomLanguageConfig {
        language_name: "myformat".to_string(),
        aliases: vec!["mf".to_string()],
        separators_regex: vec![r"---".to_string()],
    };
    let splitter = RecursiveSplitter::with_custom_languages(vec![config]).unwrap();

    let text = "Part1---Part2---Part3";
    let chunks = splitter.split_with(
        text,
        cocoindex::ops::text::RecursiveChunkConfig {
            chunk_size: 10,
            min_chunk_size: Some(3),
            chunk_overlap: None,
            language: Some("myformat".to_string()),
        },
    );
    assert_eq!(chunks.len(), 3);
    assert_eq!(chunks[0].text(text), "Part1");
    assert_eq!(chunks[1].text(text), "Part2");
    assert_eq!(chunks[2].text(text), "Part3");

    // Alias resolves to the same language.
    let alias_text = "PartA---PartB";
    let chunks = splitter.split_with(
        alias_text,
        cocoindex::ops::text::RecursiveChunkConfig {
            chunk_size: 10,
            min_chunk_size: Some(3),
            chunk_overlap: None,
            language: Some("mf".to_string()),
        },
    );
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].text(alias_text), "PartA");
    assert_eq!(chunks[1].text(alias_text), "PartB");
}

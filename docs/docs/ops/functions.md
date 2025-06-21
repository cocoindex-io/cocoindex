---
title: Functions
description: CocoIndex Built-in Functions
---

# CocoIndex Built-in Functions

## ParseJson

`ParseJson` parses a given text to JSON.

The spec takes the following fields:

*   `text` (type: `str`, required): The source text to parse.
*   `language` (type: `str`, optional): The language of the source text.  Only `json` is supported now.  Default to `json`.

Return type: `Json`

## SplitRecursively

`SplitRecursively` splits a document into chunks of a given size.
It tries to split at higher-level boundaries. If each chunk is still too large, it tries at the next level of boundaries.
For example, for a Markdown file, it identifies boundaries in this order: level-1 sections, level-2 sections, level-3 sections, paragraphs, sentences, etc.

Input data:

*   `text` (type: `str`, required): The text to split.
*   `chunk_size` (type: `int`, required): The maximum size of each chunk, in bytes.
*   `min_chunk_size` (type: `int`, optional): The minimum size of each chunk, in bytes. If not provided, default to `chunk_size / 2`.

    :::note

    `SplitRecursively` will do its best to make the output chunks sized between `min_chunk_size` and `chunk_size`.
    However, it's possible that some chunks are smaller than `min_chunk_size` or larger than `chunk_size` in rare cases, e.g. too short input text, or non-splittable large text.

    Please avoid setting `min_chunk_size` to a value too close to `chunk_size`, to leave more rooms for the function to plan the optimal chunking.

    :::

*   `chunk_overlap` (type: `int`, optional): The maximum overlap size between adjacent chunks, in bytes.
*   `language` (type: `str`, optional): The language of the document.
    Can be a language name (e.g. `Python`, `Javascript`, `Markdown`) or a file extension (e.g. `.py`, `.js`, `.md`).

*   `custom_languages` (type: `list[CustomLanguageSpec]`, optional): This allows you to customize the way to chunking specific languages using regular expressions. Each `CustomLanguageSpec` is a dict with the following fields:
    *   `language_name` (type: `str`, required): Name of the language.
    *   `aliases` (type: `list[str]`, optional): A list of aliases for the language.
        It's an error if any language name or alias is duplicated.

    *   `separators_regex` (type: `list[str]`, required): A list of regex patterns to split the text.
        Higher-level boundaries should come first, and lower-level should be listed later. e.g. `[r"\n# ", r"\n## ", r"\n\n", r"\. "]`.
        See [regex Syntax](https://docs.rs/regex/latest/regex/#syntax) for supported regular expression syntax.

    :::note

    We use the `language` field to determine how to split the input text, following these rules:

    *   We'll match the input `language` field against the `language_name` or `aliases` of each custom language specification, and use the matched one. If value of `language` is null, it'll be treated as empty string when matching `language_name` or `aliases`.
    *   If no match is found, we'll match the `language` field against the builtin language configurations.
        For all supported builtin language names and aliases (extensions), see [the code](https://github.com/search?q=org%3Acocoindex-io+lang%3Arust++%22static+TREE_SITTER_LANGUAGE_BY_LANG%22&type=code).
    *   If no match is found, the input will be treated as plain text.

    :::

Return type: [KTable](/docs/core/data_types#ktable), each row represents a chunk, with the following sub fields:

*   `location` (type: `range`): The location of the chunk.
*   `text` (type: `str`): The text of the chunk.

## SentenceTransformerEmbed

`SentenceTransformerEmbed` embeds a text into a vector space using the [SentenceTransformer](https://huggingface.co/sentence-transformers) library.

The spec takes the following fields:

*   `model` (type: `str`, required): The name of the SentenceTransformer model to use.
*   `args` (type: `dict[str, Any]`, optional): Additional arguments to pass to the SentenceTransformer constructor. e.g. `{"trust_remote_code": True}`

Input data:

*   `text` (type: `str`, required): The text to embed.

Return type: `vector[float32; N]`, where `N` is determined by the model

## ExtractByLlm

`ExtractByLlm` extracts structured information from a text using specified LLM. The spec takes the following fields:

*   `llm_spec` (type: `cocoindex.LlmSpec`, required): The specification of the LLM to use. See [LLM Spec](/docs/ai/llm#llm-spec) for more details.
*   `output_type` (type: `type`, required): The type of the output. e.g. a dataclass type name. See [Data Types](/docs/core/data_types) for all supported data types. The LLM will output values that match the schema of the type.
*   `instruction` (type: `str`, optional): Additional instruction for the LLM.

:::tip Clear type definitions

Definitions of the `output_type` is fed into LLM as guidance to generate the output.
To improve the quality of the extracted information, giving clear definitions for your dataclasses is especially important, e.g.

*   Provide readable field names for your dataclasses.
*   Provide reasonable docstrings for your dataclasses.
*   For any optional fields, clearly annotate that they are optional, by `SomeType | None` or `typing.Optional[SomeType]`.

:::

Input data:

*   `text` (type: `str`, required): The text to extract information from.

Return type: As specified by the `output_type` field in the spec. The extracted information from the input text.

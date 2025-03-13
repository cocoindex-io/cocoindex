---
title: Functions
description: CocoIndex Built-in Functions
---

# CocoIndex Built-in Functions

## SplitRecursively

`SplitRecursively` splits a document into chunks of a given size.
It tries to split at higher-level boundaries. If each chunk is still too large, it tries at the next level of boundaries.
For example, for a Markdown file, it identifies boundaries in this order: level-1 sections, level-2 sections, level-3 sections, paragraphs, sentences, etc.

Input data:

*   `text` (type: `str`, required): The text to split.
*   `chunk_size` (type: `int`, required): The maximum size of each chunk, in bytes.
*   `chunk_overlap` (type: `int`, optional): The maximum overlap size between adjacent chunks, in bytes.
*   `language` (type: `str`, optional): The language of the document. Currently it supports `markdown`, `python` and  `javascript`. If unspecified, will treat it as plain text.

Return type: `Table`, each row represents a chunk, with the following sub fields:

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

Input data:

*   `text` (type: `str`, required): The text to extract information from.

Return type: As specified by the `output_type` field in the spec. The extracted information from the input text.

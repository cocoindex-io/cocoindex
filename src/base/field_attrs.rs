use const_format::concatcp;

pub static COCOINDEX_PREFIX: &str = "cocoindex.io/";

/// Present for bytes and str. It points to fields that represents the original file name for the data.
/// Type: AnalyzedValueMapping
pub static CONTENT_FILENAME: &str = concatcp!(COCOINDEX_PREFIX, "content_filename");

/// Present for bytes and str. It points to fields that represents mime types for the data.
/// Type: AnalyzedValueMapping
pub static CONTENT_MIME_TYPE: &str = concatcp!(COCOINDEX_PREFIX, "content_mime_type");

/// Present for chunks. It points to fields that the chunks are for.
/// Type: AnalyzedValueMapping
pub static CHUNK_BASE_TEXT: &str = concatcp!(COCOINDEX_PREFIX, "chunk_base_text");

/// Base text for an embedding vector.
pub static _VECTOR_ORIGIN_TEXT: &str = concatcp!(COCOINDEX_PREFIX, "vector_origin_text");

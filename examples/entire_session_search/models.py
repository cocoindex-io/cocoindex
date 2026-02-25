"""Data models for Entire session search."""

from dataclasses import dataclass


@dataclass
class TranscriptChunk:
    """A single turn from a conversation transcript."""

    role: str
    text: str


@dataclass
class SessionInfo:
    """Checkpoint and session identifiers extracted from a file path."""

    checkpoint_id: str
    session_index: str


@dataclass
class ChunkInput:
    """A text chunk ready for embedding, with its metadata."""

    text: str
    content_type: str  # "transcript", "prompt", or "context"
    role: str  # "user", "assistant", or "" for non-transcript

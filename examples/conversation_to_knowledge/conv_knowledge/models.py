"""Data models for Conversation-to-Knowledge pipeline."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

import cocoindex as coco
import pydantic

# ---------------------------------------------------------------------------
# Context keys (shared across modules)
# ---------------------------------------------------------------------------

LLM_MODEL = coco.ContextKey[str]("llm_model")


# ---------------------------------------------------------------------------
# SurrealDB entity models (dataclasses for declare_record)
# ---------------------------------------------------------------------------


@dataclass
class Session:
    id: str  # YouTube video ID
    name: str
    transcript: str
    description: str | None = None
    date: str | None = None


@dataclass
class Person:
    id: str  # hash of canonical name
    name: str


@dataclass
class Tech:
    id: str
    name: str


@dataclass
class Org:
    id: str
    name: str


@dataclass
class Statement:
    id: str  # hash of session_id + statement text
    statement: str


# ---------------------------------------------------------------------------
# LLM extraction models (Pydantic, for instructor)
# ---------------------------------------------------------------------------


@coco.unpickle_safe
class RawStatement(pydantic.BaseModel):
    """A thematic claim or statement made during the session."""

    statement: str
    speakers: list[str]  # Names of persons who made the statement
    involved_persons: list[str] = []
    involved_techs: list[str] = []
    involved_orgs: list[str] = []


@coco.unpickle_safe
class SessionExtraction(pydantic.BaseModel):
    """LLM output: metadata + entities extracted from a session."""

    name: str
    description: str | None = None
    date: str | None = None
    persons_attending: list[str]
    statements: list[RawStatement]


# ---------------------------------------------------------------------------
# Internal transfer types
# ---------------------------------------------------------------------------


@dataclass
class SessionTranscript:
    """Carries raw transcript and yt-dlp metadata."""

    transcript: str
    yt_title: str
    yt_upload_date: str | None


@coco.unpickle_safe
@dataclass
class SessionRawEntities:
    """Raw entities from a single session, for entity resolution."""

    session_id: str
    persons: list[str]
    statements: list[RawStatement]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_id(*parts: str) -> str:
    """Deterministic ID from name parts."""
    key = "|".join(parts)
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def resolve_canonical(name: str, dedup: dict[str, str | None]) -> str:
    """Chase dedup chains to find the canonical name."""
    while dedup.get(name) is not None:
        name = dedup[name]  # type: ignore[assignment]
    return name

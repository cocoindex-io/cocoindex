"""Pydantic models for paper metadata extraction."""

from pydantic import BaseModel, Field


class AuthorModel(BaseModel):
    """Information about a paper author."""

    name: str
    email: str | None = None
    affiliation: str | None = None


class PaperMetadataModel(BaseModel):
    """Extracted metadata from an academic paper."""

    title: str
    authors: list[AuthorModel] = Field(default_factory=list)
    abstract: str

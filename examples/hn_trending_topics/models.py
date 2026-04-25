"""Pydantic models for HackerNews topic extraction."""

from pydantic import BaseModel, Field


class TopicsResponse(BaseModel):
    """Response containing a list of extracted topics."""

    topics: list[str] = Field(
        description="""List of extracted topics.

Each topic can be a product name, technology, model, people, company name, business domain, etc.
Capitalize for proper nouns and acronyms only.
Use the form that is clear alone.
Avoid acronyms unless very popular and unambiguous for common people even without context.

Examples:
- "Anthropic" (not "ANTHR")
- "Claude" (specific product name)
- "React" (well-known library)
- "PostgreSQL" (canonical database name)

For topics that are a phrase combining multiple things, normalize into multiple topics if needed. Examples:
- "books for autistic kids" -> "book", "autistic", "autistic kids"
- "local Large Language Model" -> "local Large Language Model", "Large Language Model"

For people, use preferred name and last name. Examples:
- "Bill Clinton" instead of "William Jefferson Clinton"

When there're multiple common ways to refer to the same thing, use multiple topics. Examples:
- "John Kennedy", "JFK"
"""
    )

"""LLM entity/metadata extraction via instructor + litellm."""

from __future__ import annotations

from collections.abc import Mapping

import cocoindex as coco
import instructor
import litellm

litellm.drop_params = True

from .models import (
    LLM_MODEL,
    SessionMetadata,
    SessionTranscript,
    StatementExtraction,
    Utterance,
    is_speaker_label,
)

# ---------------------------------------------------------------------------
# Shared transcript formatter
# ---------------------------------------------------------------------------


def format_transcript(
    utterances: list[Utterance], speaker_map: Mapping[str, str | None]
) -> str:
    """Format structured utterances into text with speaker names.

    Used by both extraction steps:
    - Step 1: pass empty dict → all speakers shown as "(Speaker A)", ...
    - Step 2: pass mapping from Step 1 → recognized speakers get real names,
              unrecognized stay as "(Speaker X)"
    """
    lines: list[str] = []
    for u in utterances:
        name = speaker_map.get(u.speaker)
        if name is not None:
            lines.append(f"{name}: {u.text}")
        else:
            lines.append(f"(Speaker {u.speaker}): {u.text}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Step 1: Extract metadata + identify speakers
# ---------------------------------------------------------------------------

METADATA_PROMPT = """\
You are an expert knowledge extractor analyzing a podcast/interview transcript.

Given the transcript (with speaker labels like "(Speaker A)") and YouTube metadata \
(channel name, video title, description, upload date), extract:

1. **name**: A clear, descriptive name for this session/episode. Use the video title \
and channel name as hints but make it informative.
2. **description**: A brief 1-2 sentence description of the session's main topics.
3. **date**: The date of the session in ISO format (YYYY-MM-DD) if mentioned in the \
conversation. Otherwise use the upload date provid)ed.
4. **speakers**: For each speaker label (A, B, ...) in the transcript, identify who \
they are. Use the channel name, video title, description, and conversation content as \
clues. Return their full, canonical, Wikipedia-style name (e.g. "Lex Fridman", \
"Sam Altman", not just "Lex" or "Sam"). Only include speakers you can confidently \
identify with their full name — omit any speaker you cannot identify. Do not guess.
"""


@coco.fn(memo=True)
async def extract_metadata(
    reformatted_transcript: str, transcript: SessionTranscript
) -> SessionMetadata:
    """Give LLM the reformatted transcript + all YouTube metadata to identify speakers."""
    client = instructor.from_litellm(litellm.acompletion, mode=instructor.Mode.JSON)
    result = await client.chat.completions.create(
        model=coco.use_context(LLM_MODEL),
        response_model=SessionMetadata,
        messages=[
            {"role": "system", "content": METADATA_PROMPT},
            {
                "role": "user",
                "content": (
                    f"YouTube channel: {transcript.yt_channel}\n"
                    f"Video title: {transcript.yt_title}\n"
                    f"Description: {transcript.yt_description or 'N/A'}\n"
                    f"Upload date: {transcript.yt_upload_date or 'unknown'}\n\n"
                    f"Transcript:\n{reformatted_transcript}"
                ),
            },
        ],
    )
    # Re-validate to restore class identity for pickling.
    metadata = SessionMetadata.model_validate(result.model_dump())
    # Post-filter: drop speakers whose name is not a plausible person name
    # (e.g. LLM might output "null", "all", "unknown", single words, etc.)
    metadata.speakers = [
        s for s in metadata.speakers if _is_plausible_person_name(s.name)
    ]
    return metadata


def _is_plausible_person_name(name: str) -> bool:
    """Check if a name looks like a real person name (at least two words)."""
    return len(name.split()) >= 2


# ---------------------------------------------------------------------------
# Step 2: Extract statements + involved entities
# ---------------------------------------------------------------------------

STATEMENTS_PROMPT = """\
You are an expert knowledge extractor. Given a podcast/interview transcript where \
speakers are identified by name, extract thematic claims and statements.

For each statement:
- Write the statement as a clear, standalone claim.
- List the speaker(s) who made it (by their full name as shown in the transcript).
- List persons, technologies, and organizations the statement is ABOUT. Do NOT include \
the speaker(s) in involved_persons unless the statement is specifically about them \
(e.g. their background, credentials, or personal experience). The speaker relationship \
is already captured separately.

IMPORTANT RULES:
- All entity names must be SELF-CONTAINED. Never use pronouns ("he", "she", "they"), \
speaker labels ("Speaker A"), or contextual references ("the host", "the guest", \
"the interviewer"). Every name must be a clear, unambiguous identifier that stands \
on its own.
- Use canonical, Wikipedia-style names for all entities:
  - People: "Franklin D. Roosevelt", "Yann LeCun". Only include people you can confidently \
    identify with their full name — omit anyone you cannot identify, or only know part of their name.
    Do not guess.
  - Tech: "Python (programming language)", "Large language model", "ChatGPT"
  - Orgs: "Apple Inc.", "OpenAI", "US Department of Education"
- Statements from unrecognized speakers (shown as "(Speaker X)") should still be \
extracted with their involved entities, but leave the speakers list empty for them.
- Be thorough but avoid trivial statements. Focus on substantive claims, opinions, \
and factual assertions.
"""


@coco.fn(memo=True)
async def extract_statements(
    reformatted_transcript: str,
) -> StatementExtraction:
    """Extract statements and involved entities from the reformatted transcript."""
    client = instructor.from_litellm(litellm.acompletion, mode=instructor.Mode.JSON)
    result = await client.chat.completions.create(
        model=coco.use_context(LLM_MODEL),
        response_model=StatementExtraction,
        messages=[
            {"role": "system", "content": STATEMENTS_PROMPT},
            {
                "role": "user",
                "content": f"Transcript:\n{reformatted_transcript}",
            },
        ],
    )
    # Re-validate to restore class identity for pickling.
    extraction = StatementExtraction.model_validate(result.model_dump())
    # Post-filter: strip speaker labels that leaked through from the LLM
    for stmt in extraction.statements:
        stmt.speakers = [s for s in stmt.speakers if not is_speaker_label(s)]
        stmt.involved_persons = [
            p for p in stmt.involved_persons if not is_speaker_label(p)
        ]
    return extraction

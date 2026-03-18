"""LLM entity/metadata extraction via instructor + litellm."""

from __future__ import annotations

import cocoindex as coco
import instructor
import litellm

litellm.drop_params = True

from .models import LLM_MODEL, SessionExtraction, SessionTranscript

EXTRACTION_PROMPT = """\
You are an expert knowledge extractor. Given a podcast/interview transcript with \
speaker labels and video metadata, extract:

1. **name**: A clear, descriptive name for this session/episode. Use the video title \
as a hint but make it informative.
2. **description**: A brief 1-2 sentence description of the session's main topics.
3. **date**: The date of the session in ISO format (YYYY-MM-DD) if mentioned in the \
conversation. Otherwise use the upload date provided.
4. **persons_attending**: Names of all speakers/attendees. Use Wikipedia-style canonical \
names (e.g. "Elon Musk", "Sam Altman").
5. **statements**: Extract thematic claims and statements made during the session. \
For each statement:
   - Write the statement as a clear, standalone claim
   - List the speaker(s) who made it
   - List persons, technologies, and organizations involved

Use canonical, Wikipedia-style names for all entities:
- People: "Franklin D. Roosevelt", "Yann LeCun"
- Tech: "Python (programming language)", "Large language model", "ChatGPT"
- Orgs: "Apple Inc.", "OpenAI", "US Department of Education"

Be thorough but avoid trivial statements. Focus on substantive claims, opinions, \
and factual assertions.
"""


@coco.fn
async def extract_session(transcript: SessionTranscript) -> SessionExtraction:
    """Give LLM the full transcript + metadata, extract everything at once."""
    client = instructor.from_litellm(litellm.acompletion, mode=instructor.Mode.JSON)
    result = await client.chat.completions.create(
        model=coco.use_context(LLM_MODEL),
        response_model=SessionExtraction,
        messages=[
            {"role": "system", "content": EXTRACTION_PROMPT},
            {
                "role": "user",
                "content": (
                    f"Video title: {transcript.yt_title}\n"
                    f"Upload date: {transcript.yt_upload_date or 'unknown'}\n\n"
                    f"Transcript:\n{transcript.transcript}"
                ),
            },
        ],
    )
    # instructor returns a modified copy of the model class that can't be pickled.
    # Re-validate through the original class to restore class identity.
    return SessionExtraction.model_validate(result.model_dump())

"""The model client boundary.

The protocol is structured (spec, tools, transcript prefix, hints) rather than
message-based, so a scripted model in tests can look at the same inputs the
real client renders into messages. Rendering to provider messages is the
client's job (see ``litellm_client.py``).

The client is provided through a context key, not passed as an argument, so it
is never part of a memo key.
"""

from __future__ import annotations

from typing import Protocol

import cocoindex as coco

from .hints import RunHints
from .spec import SessionSpec, ToolSpec
from .transcript import ModelOutput, Transcript


class ModelClient(Protocol):
    async def complete(
        self,
        spec: SessionSpec,
        tools: tuple[ToolSpec, ...],
        prefix: Transcript,
        hints: RunHints,
    ) -> ModelOutput: ...


MODEL_CLIENT: coco.ContextKey[ModelClient] = coco.ContextKey(
    "cocoindex_agents.model_client"
)

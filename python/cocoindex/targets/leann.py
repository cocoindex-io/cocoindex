"""LEANN vector index target for CocoIndex.

Provides efficient RAG capabilities with 97% storage savings.
"""
import dataclasses
import json
import logging
from pathlib import Path
from typing import Any, Optional

try:
    from leann import LeannBuilder
except ImportError:
    raise ImportError("LEANN library not installed. Install with: pip install leann")

from .. import op
from ..typing import FieldSchema
from ..index import IndexOptions

_logger = logging.getLogger(__name__)

class LEANN(op.TargetSpec):
    """LEANN vector index target specification."""
    index_path: str
    embedding_model: str = "facebook/contriever"
    embedding_mode: str = "sentence-transformers"
    backend_name: str = "hnsw"
    distance_metric: str = "cosine"
    text_field: str = ""
    metadata_fields: Optional[list[str]] = None
    enable_recomputation: bool = True

@dataclasses.dataclass
class _State:
    """Internal state for LEANN index."""
    index_path: str
    embedding_model: str
    embedding_mode: str
    backend_name: str
    distance_metric: str
    text_field: str
    metadata_fields: Optional[list[str]]
    key_field_schema: FieldSchema
    value_fields_schema: list[FieldSchema]
    enable_recomputation: bool

@dataclasses.dataclass
class _IndexKey:
    """Unique identifier for LEANN index."""
    index_path: str

@dataclasses.dataclass
class _MutateContext:
    """Context for mutations."""
    builder: LeannBuilder
    index_path: str
    text_field: str
    metadata_fields: Optional[list[str]]
    key_field_schema: FieldSchema

@op.target_connector(
    spec_cls=LEANN, persistent_key_type=_IndexKey, setup_state_cls=_State
)
class _Connector:
    """LEANN target connector for CocoIndex."""

    @staticmethod
    def get_persistent_key(spec: LEANN) -> _IndexKey:
        return _IndexKey(index_path=spec.index_path)

    @staticmethod
    def get_setup_state(
        spec: LEANN,
        key_fields_schema: list[FieldSchema],
        value_fields_schema: list[FieldSchema],
        index_options: IndexOptions,
    ) -> _State:
        if len(key_fields_schema) != 1:
            raise ValueError("LEANN requires one key field")
        
        text_field = spec.text_field
        if not text_field:
            for field in value_fields_schema:
                field_type = field.value_type.type
                if hasattr(field_type, "kind") and field_type.kind == "Str":
                    text_field = field.name
                    break
        
        if not text_field:
            raise ValueError("No text field found for embeddings")
        
        return _State(
            index_path=spec.index_path,
            embedding_model=spec.embedding_model,
            embedding_mode=spec.embedding_mode,
            backend_name=spec.backend_name,
            distance_metric=spec.distance_metric,
            text_field=text_field,
            metadata_fields=spec.metadata_fields,
            key_field_schema=key_fields_schema[0],
            value_fields_schema=value_fields_schema,
            enable_recomputation=spec.enable_recomputation,
        )

    @staticmethod
    def describe(key: _IndexKey) -> str:
        return f"LEANN index at {key.index_path}"

    @staticmethod
    def check_state_compatibility(
        previous: _State, current: _State
    ) -> op.TargetStateCompatibility:
        if (
            previous.key_field_schema != current.key_field_schema
            or previous.text_field != current.text_field
        ):
            return op.TargetStateCompatibility.NOT_COMPATIBLE
        return op.TargetStateCompatibility.COMPATIBLE

    @staticmethod
    async def apply_setup_change(
        key: _IndexKey, previous: _State | None, current: _State | None
    ) -> None:
        latest_state = current or previous
        if not latest_state:
            return
        Path(latest_state.index_path).parent.mkdir(parents=True, exist_ok=True)

    @staticmethod
    async def prepare(
        spec: LEANN,
        setup_state: _State,
    ) -> _MutateContext:
        builder = LeannBuilder(
            backend_name=setup_state.backend_name,
            embedding_model=setup_state.embedding_model,
            embedding_mode=setup_state.embedding_mode,
            distance_metric=setup_state.distance_metric,
            is_recompute=setup_state.enable_recomputation,
        )
        return _MutateContext(
            builder=builder,
            index_path=setup_state.index_path,
            text_field=setup_state.text_field,
            metadata_fields=setup_state.metadata_fields or [],
            key_field_schema=setup_state.key_field_schema,
        )

    @staticmethod
    async def mutate(
        *all_mutations: tuple[_MutateContext, dict[Any, dict[str, Any] | None]],
    ) -> None:
        for context, mutations in all_mutations:
            for key, value in mutations.items():
                if value is not None:
                    text = value.get(context.text_field, str(key))
                    metadata = {"id": str(key)}
                    if context.metadata_fields:
                        for field in context.metadata_fields:
                            if field in value:
                                metadata[field] = value[field]
                    context.builder.add_text(text, metadata=metadata)
            
            if context.builder.chunks:
                context.builder.build_index(context.index_path)

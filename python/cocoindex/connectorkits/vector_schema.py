"""Shared vector-schema resolution for target connector implementations."""

from __future__ import annotations

import collections.abc as _collections_abc
import typing as _typing

import cocoindex as _coco
from cocoindex.resources import schema as _schema

__all__ = ["VectorSchemas", "reject_sparse_vectors", "resolve_vector_schemas"]


class VectorSchemas(_typing.NamedTuple):
    """Dense and sparse vector schemas resolved for one column."""

    vector: _schema.VectorSchema | None
    sparse: _schema.SparseVectorSchema | None


async def resolve_vector_schemas(
    base_type: object,
    annotations: _typing.Iterable[object],
    *,
    reject_sparse_vectors_for: str | None = None,
) -> VectorSchemas:
    """Resolve dense and sparse metadata for a vector-aware connector.

    All direct annotations are inspected first. The first provider of each kind
    is retained, but providing both dense and sparse schemas is an error. Context
    keys are consulted in annotation order only when no direct schema exists,
    and resolution stops at the first key that yields either schema.

    Set ``reject_sparse_vectors_for`` to the connector name when dense vectors
    are supported but sparse vectors are not. Connectors that do not consume
    vector metadata should call :func:`reject_sparse_vectors` instead so they do
    not resolve unrelated context keys.
    """
    if reject_sparse_vectors_for is not None and base_type is _schema.SparseVector:
        _raise_sparse_vectors_unsupported(reject_sparse_vectors_for)

    direct_annotations: list[object] = []
    context_keys: list[_coco.ContextKey[object]] = []
    for annotation in annotations:
        if isinstance(annotation, _coco.ContextKey):
            context_keys.append(annotation)
        else:
            direct_annotations.append(annotation)

    schemas = await _resolve_providers(direct_annotations)
    if schemas.vector is None and schemas.sparse is None:
        for context_key in context_keys:
            schemas = await _resolve_providers([_coco.use_context(context_key)])
            if schemas.vector is not None or schemas.sparse is not None:
                break

    _validate_schema_base_types(base_type, schemas)
    return schemas


def reject_sparse_vectors(
    base_type: object,
    annotations: _typing.Iterable[object],
    *,
    connector_name: str,
) -> None:
    """Reject direct sparse metadata without resolving ``ContextKey`` values."""
    has_sparse_schema = any(
        isinstance(annotation, _schema.SparseVectorSchemaProvider)
        for annotation in annotations
    )
    _validate_sparse_schema_base_type(base_type, has_sparse_schema)
    if base_type is _schema.SparseVector:
        _raise_sparse_vectors_unsupported(connector_name)


def _validate_schema_base_types(base_type: object, schemas: VectorSchemas) -> None:
    _validate_sparse_schema_base_type(base_type, schemas.sparse is not None)
    if schemas.vector is not None and (
        base_type is _schema.SparseVector
        or (
            isinstance(base_type, type)
            and issubclass(base_type, _collections_abc.Mapping)
        )
    ):
        raise TypeError(
            f"VectorSchema requires a dense vector field, got {base_type!r}."
        )


def _validate_sparse_schema_base_type(
    base_type: object, has_sparse_schema: bool
) -> None:
    if has_sparse_schema and base_type is not _schema.SparseVector:
        raise TypeError(
            f"SparseVectorSchema requires a SparseVector field, got {base_type!r}."
        )


def _raise_sparse_vectors_unsupported(connector_name: str) -> _typing.NoReturn:
    raise ValueError(f"{connector_name} does not support sparse vector columns.")


async def _resolve_providers(annotations: _typing.Iterable[object]) -> VectorSchemas:
    vector_schema: _schema.VectorSchema | None = None
    sparse_vector_schema: _schema.SparseVectorSchema | None = None

    for annotation in annotations:
        if vector_schema is None and isinstance(
            annotation, _schema.VectorSchemaProvider
        ):
            vector_schema = await annotation.__coco_vector_schema__()
        if sparse_vector_schema is None and isinstance(
            annotation, _schema.SparseVectorSchemaProvider
        ):
            sparse_vector_schema = await annotation.__coco_sparse_vector_schema__()

        if vector_schema is not None and sparse_vector_schema is not None:
            raise ValueError(
                "A field cannot provide both VectorSchema and SparseVectorSchema"
            )

    return VectorSchemas(vector=vector_schema, sparse=sparse_vector_schema)

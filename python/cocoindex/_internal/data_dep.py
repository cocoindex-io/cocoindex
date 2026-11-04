"""``DataDep`` / ``use_data_dep``: a data dependency a body discovers while it
runs.

A memoized function or component is reused when its logic, its arguments and
the context values it read are unchanged. All of those are known before the
body runs. Some bodies read things decided only while they run: an agent
session opens files the model chose, a test runner touches the files the tests
import. ``use_data_dep`` records such a read as a ``(kind, key, state)`` record
in the current memo entry, and the next probe validates each record through
its kind's validator from the stored key and state alone: nothing from the
run that recorded it survives in memory.

Records stay in the entry of the function or component that recorded them.
They are never copied into ancestor components' entries; a component reports
one bit upward instead (see the engine's ``ComponentRunOutcome``), so a
memoized ancestor of a component that recorded a data dependency re-runs its
body instead of reusing its memo. Spec: cocoindex-specs ``data_deps/spec.md``.
"""

from __future__ import annotations

import asyncio
import logging
import reprlib
import warnings
import weakref
from typing import Any, Awaitable, Callable, NamedTuple, Sequence

from . import core
from .component_ctx import _context_var, get_context_from_ctx
from .serde import get_param_annotation, make_deserialize_fn, qualified_name
from .typing import MemoStateOutcome
from .context_keys import resolve_awaitables_sync

logger = logging.getLogger(__name__)

DataDepValidator = Callable[
    [Any, Any], "MemoStateOutcome | Awaitable[MemoStateOutcome]"
]

# Kinds alive in the process, by fingerprint. Weak: a kind is registered while
# the user's module keeps it alive and leaves when the last reference goes, so
# the registry never keeps a kind alive by itself.
_registry: weakref.WeakValueDictionary[core.Fingerprint, DataDep] = (
    weakref.WeakValueDictionary()
)


def _param_deserializer(
    validate: Callable[..., Any], position: int, what: str, kind_name: str
) -> Callable[[bytes | memoryview], Any]:
    try:
        ann = get_param_annotation(validate, position)
        return make_deserialize_fn(
            ann, source_label=f"{what} param of DataDep({kind_name!r}).validate"
        )
    except Exception:
        return make_deserialize_fn(Any)


class DataDep:
    """A kind of data dependency, declared once at module import.

    ``name`` is a stable string. It is hashed into the kind's fingerprint in
    its own namespace and identifies the kind in stored memo entries, so
    renaming a kind invalidates every entry that recorded it; that doubles as
    the version bump.

    ``validate(key, prev_state)`` takes a recorded key and the state stored
    with it, and returns a :class:`MemoStateOutcome` (sync, or an awaitable).
    It never receives ``NON_EXISTENCE``: a record is only ever validated
    against a state the body recorded. ``recollect_after_run`` is ignored.
    Annotate both parameters: the annotations drive typed deserialization of
    the stored key and state, as a state function's ``prev_state`` annotation
    does.

    **Construct at module import, never inside a component.** A memo entry is
    validated against the kinds registered at that moment, so a kind
    constructed inside a component registers too late for the entries that
    recorded it. Constructing one inside a component context raises.

    A second live kind with the same name replaces the registration and emits
    a ``RuntimeWarning``: the most recently constructed live kind validates
    records of that name. This keeps module reload working; two kinds that
    share a name by accident are a bug the warning points at.
    """

    __slots__ = (
        "_fp",
        "_name",
        "_validate",
        "_deserialize_key",
        "_deserialize_state",
        "__weakref__",
    )

    _fp: core.Fingerprint
    _name: str
    _validate: DataDepValidator
    _deserialize_key: Callable[[bytes | memoryview], Any]
    _deserialize_state: Callable[[bytes | memoryview], Any]

    def __init__(self, name: str, /, *, validate: DataDepValidator) -> None:
        if _context_var.get(None) is not None:
            raise RuntimeError(
                "coco.DataDep must be constructed outside any component context, "
                "before any component runs (at module import): a kind "
                "constructed inside a component registers too late for the memo "
                "entries that recorded it (see the DataDep docstring)."
            )
        if not isinstance(name, str) or not name:
            raise TypeError(
                f"coco.DataDep() takes a non-empty name string, got {name!r}"
            )
        if not callable(validate):
            raise TypeError(
                f"coco.DataDep({name!r}) takes a callable validate=, got {validate!r}"
            )
        self._name = name
        self._validate = validate
        # Its own namespace in the fingerprint space, as `<logic_dep>` is for
        # `LogicDep`: a kind must never collide with a function's logic
        # fingerprint or a `deps=` value.
        self._fp = core.fingerprint_str(f"<data_dep>{name}")
        self._deserialize_key = _param_deserializer(validate, 0, "key", name)
        self._deserialize_state = _param_deserializer(validate, 1, "prev_state", name)
        existing = _registry.get(self._fp)
        if existing is not None and existing is not self:
            warnings.warn(
                f"coco.DataDep({name!r}) replaces a live kind of the same name; "
                f"the new kind now validates records of that name "
                f"(validator {qualified_name(validate)})",
                RuntimeWarning,
                stacklevel=2,
            )
        _registry[self._fp] = self

    @property
    def name(self) -> str:
        return self._name

    # A kind is a process-level identity, as a `@coco.fn` function is: a copy
    # would share the fingerprint without being the registered object.
    def __copy__(self) -> DataDep:
        return self

    def __deepcopy__(self, memo: dict[int, Any]) -> DataDep:
        return self

    def __eq__(self, other: object) -> bool:
        return isinstance(other, DataDep) and other._fp == self._fp

    def __hash__(self) -> int:
        return hash(self._fp)

    def __repr__(self) -> str:
        return f"coco.DataDep({self._name!r})"

    def __coco_memo_key__(self) -> object:
        # As a memoized function's argument a kind keys by its fingerprint.
        return ("coco.DataDep", self._fp.as_bytes())


def use_data_dep(kind: DataDep, /, *, key: object, state: object) -> None:
    """Record one data dependency into the current call.

    ``key`` identifies what was read; ``state`` is what was observed. Both are
    serializable values and are **snapshotted now**: serialized once, with
    identity and equality taken as fingerprints of those bytes. The memo-key
    pipeline (``__coco_memo_key__``, registered key functions) is not
    consulted, so use plain values: the resolved path string, a tuple of mtime
    and digest. Mutating the objects afterwards changes nothing.

    Recording the same kind and key again with an equal state records once.
    Recording it with a different state makes the current run store no entry:
    output that used two states of one key has no single state to validate
    against, so the function or component re-runs next time. A body that
    writes what it then reads back should not record the read-back.

    The record rides up through every enclosing function frame regardless of
    ``logic_tracking``, into every enclosing memoized function's entry and the
    component's entry, where it stops.

    Raises:
        RuntimeError: outside an active component context, or when the
            current frame has already closed (a thread-pool job recording
            after the body returned). The check covers the current frame
            only: a ``@coco.fn`` reader started as a task that outlives the
            decorated function which started it still has its own open frame,
            so its records merge into an already-joined frame and are lost,
            as its logic deps are today. Await readers inside the function
            that started them.
        TypeError: when ``kind`` is not a :class:`DataDep`.
    """
    if not isinstance(kind, DataDep):
        raise TypeError(
            f"coco.use_data_dep() takes a coco.DataDep constructed at module "
            f"import, got {kind!r}"
        )
    fn_ctx = get_context_from_ctx()._core_fn_call_ctx
    if fn_ctx.closed:
        raise RuntimeError(
            f"coco.use_data_dep({kind!r}, key={reprlib.repr(key)}) called after "
            f"the function that owns the current frame returned: a task that "
            f"outlives its frame cannot record into it. Await the reader inside "
            f"the function that started it."
        )
    fn_ctx.add_data_dep(kind._fp, key, state)


# ---------------------------------------------------------------------------
# Probe side: decode stored records, then validate them.
# ---------------------------------------------------------------------------

# A stored record as the engine hands it to a probe:
# (kind_fp, key_fp, key, state), key and state as `StoredValue`s.
StoredDataDep = tuple[
    core.Fingerprint, core.Fingerprint, core.StoredValue, core.StoredValue
]
# A record handed back after validation: (kind_fp, key_fp, key, state). The
# key is the original `StoredValue`; the state is the original `StoredValue`
# when unchanged, or the validator's new state object otherwise. The engine
# keeps stored bytes and fingerprints as they are and serializes only a new
# state, so a refreshed record equals a fresh record of the same observation.
RefreshedDataDep = tuple[core.Fingerprint, core.Fingerprint, Any, Any]


class _DecodedDataDep(NamedTuple):
    kind: DataDep
    kind_fp: core.Fingerprint
    key_fp: core.Fingerprint
    key_stored: core.StoredValue
    state_stored: core.StoredValue
    key: Any
    state: Any


class DataDepValidation(NamedTuple):
    refreshed: list[RefreshedDataDep]
    can_reuse: bool
    states_changed: bool


def decode_stored_data_deps(
    stored: Sequence[StoredDataDep], *, where: str
) -> list[_DecodedDataDep] | None:
    """Decode phase: look each record's kind up and deserialize its key and
    state. Any failure (a kind no longer registered, a stored value that no
    longer matches the validator's annotations) is logged and makes the probe
    a miss: ``None`` is returned and no validator is called. The body then
    re-runs and re-records.
    """
    decoded: list[_DecodedDataDep] = []
    for kind_fp, key_fp, key_stored, state_stored in stored:
        kind = _registry.get(kind_fp)
        if kind is None:
            logger.warning(
                "%s: memo misses on a data dependency of kind %s, which is no "
                "longer registered (renamed or removed); the body re-runs",
                where,
                kind_fp,
            )
            return None
        try:
            key = key_stored.get(kind._deserialize_key)
            state = state_stored.get(kind._deserialize_state)
        except Exception:
            logger.warning(
                "%s: memo misses on a data dependency of kind %r whose stored "
                "key or state failed to deserialize; the body re-runs",
                where,
                kind,
                exc_info=True,
            )
            return None
        decoded.append(
            _DecodedDataDep(kind, kind_fp, key_fp, key_stored, state_stored, key, state)
        )
    return decoded


def _call_validators(decoded: Sequence[_DecodedDataDep]) -> list[Any]:
    return [rec.kind._validate(rec.key, rec.state) for rec in decoded]


def _fold_outcomes(
    decoded: Sequence[_DecodedDataDep], outcomes: Sequence[Any]
) -> DataDepValidation:
    refreshed: list[RefreshedDataDep] = []
    can_reuse = True
    states_changed = False
    for rec, outcome in zip(decoded, outcomes):
        if not isinstance(outcome, MemoStateOutcome):
            raise TypeError(
                f"{rec.kind!r}.validate must return coco.MemoStateOutcome, got "
                f"{outcome!r}"
            )
        if not outcome.memo_valid:
            can_reuse = False
        if outcome.state != rec.state:
            states_changed = True
            state_out: Any = outcome.state
        else:
            state_out = rec.state_stored
        refreshed.append((rec.kind_fp, rec.key_fp, rec.key_stored, state_out))
    return DataDepValidation(refreshed, can_reuse, states_changed)


def validate_data_deps_sync(
    decoded: Sequence[_DecodedDataDep], *, running_loop_error_msg: str
) -> DataDepValidation:
    """Validate phase, sync: call every validator; a validator's exception
    propagates. Awaitables are resolved as memo state functions' are."""
    outcomes = _call_validators(decoded)
    outcomes = resolve_awaitables_sync(
        outcomes, running_loop_error_msg=running_loop_error_msg
    )
    return _fold_outcomes(decoded, outcomes)


async def validate_data_deps_async(
    decoded: Sequence[_DecodedDataDep],
) -> DataDepValidation:
    """Validate phase, async: validators run concurrently; an exception
    propagates."""
    outcomes = _call_validators(decoded)
    awaitable_indices = [i for i, o in enumerate(outcomes) if isinstance(o, Awaitable)]
    if awaitable_indices:
        resolved = await asyncio.gather(*(outcomes[i] for i in awaitable_indices))
        for i, value in zip(awaitable_indices, resolved):
            outcomes[i] = value
    return _fold_outcomes(decoded, outcomes)

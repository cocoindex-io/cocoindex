"""``LogicDep`` / ``use_logic_dep``: a logic dependency declared once and
recorded per call.

``deps=`` on ``@coco.fn`` folds a value into a function's logic fingerprint, so
every call of the function depends on it. Some dependencies are decided per call: a tree walk's
``visit_file`` depends on the extractor of *that file's* language, and a change
to one extractor should re-run the files it extracted, not every file the walk
saw. ``LogicDep(value)`` declares such a value at module import and
``use_logic_dep(dep)`` records it as part of the current call's logic, so only
the memo entries that recorded it move when the value changes.

No engine change: a ``LogicDep`` is a registered logic fingerprint, like a
function's, and recording it adds it to the current frame's own logic set,
where the function's own fingerprint already sits. From there it propagates by
the existing rules (``logic_tracking``, the callback tunnel, the component
tree) and is validated at memo reuse like every other fingerprint.
"""

from __future__ import annotations

import reprlib
from typing import Any

from . import core
from .component_ctx import _context_var, get_context_from_ctx
from .memo_fingerprint import memo_fingerprint


class LogicDep:
    """A logic dependency declared once, at module import, and recorded per
    call with :func:`use_logic_dep`.

    ``value`` is canonicalized through the memoization-key pipeline, exactly as
    a ``deps=`` value is, and the resulting fingerprint is registered in the
    process's logic set for as long as this object lives. A memo entry that
    recorded it stays valid while a live ``LogicDep`` with the same value
    exists in the process; when a later release constructs the dependency with
    a different value, the entries that recorded the old one are invalidated.

    **Construct before any component runs — at module import — never per
    call or on first use.** An entry is validated against the fingerprints
    registered at that moment: a dependency that registers only when first
    needed evicts every entry recorded with it that is probed earlier in the
    process, and one released at the end of a call evicts them all, every
    pass. Neither is stale data, only memoization that silently stops
    working, which is why construction inside a component context raises.
    """

    __slots__ = ("_logic_fp", "_label")

    _logic_fp: core.Fingerprint
    _label: str

    def __init__(self, value: Any, /) -> None:
        if _context_var.get(None) is not None:
            raise RuntimeError(
                "coco.LogicDep must be constructed outside any component context, "
                "before any component runs (at module import): a dependency "
                "constructed inside a component registers too late for the memo "
                "entries that recorded it, or is released too early (see the "
                "LogicDep docstring)."
            )
        self._label = reprlib.repr(value)
        # Its own namespace in the fingerprint space: a function's logic
        # fingerprint hashes `module.qualname` + its canonical form, and a
        # `deps=` value is folded under `<deps>`; a dependency recorded per
        # call must never collide with either. Hex of the value fingerprint,
        # as `deps=` uses, so the stored form never depends on
        # `Fingerprint.__str__`.
        self._logic_fp = core.fingerprint_str(
            f"<logic_dep>{memo_fingerprint(value).as_bytes().hex()}"
        )
        core.register_logic_fingerprint(self._logic_fp)

    def __del__(self) -> None:
        # Unset only when `__init__` raised before registering.
        fp = getattr(self, "_logic_fp", None)
        if fp is not None:
            core.unregister_logic_fingerprint(fp)

    # Copying returns this same object, as a `@coco.fn` function does: a
    # distinct copy would share `_logic_fp` without having registered it, so
    # its `__del__` would release the registration this object holds.
    def __copy__(self) -> LogicDep:
        return self

    def __deepcopy__(self, memo: dict[int, Any]) -> LogicDep:
        return self

    def __eq__(self, other: object) -> bool:
        return isinstance(other, LogicDep) and other._logic_fp == self._logic_fp

    def __hash__(self) -> int:
        return hash(self._logic_fp)

    def __repr__(self) -> str:
        return f"coco.LogicDep({self._label})"

    def __coco_memo_key__(self) -> object:
        # As a memoized function's argument (a table of dependencies handed
        # down, say) a dependency keys by what it is: its fingerprint. Without
        # this the pipeline would pickle it and fail on the Fingerprint.
        return ("coco.LogicDep", self._logic_fp.as_bytes())


def use_logic_dep(dep: LogicDep, /) -> None:
    """Record ``dep`` as part of the current call's logic.

    The dependency is added to the current function frame beside the
    function's own fingerprint and travels exactly as that fingerprint does:
    into the entry of a memoized caller, onward through ``"full"`` callers,
    hidden by a ``"self"`` caller above unless the call is carried by
    :func:`logic_propagated_to_callers`, and from a component into every
    ancestor component's entry. It is recorded whatever the current function's
    own ``logic_tracking`` is: it describes the call, not the function's code.
    (When a memoized callee hits, its stored entry is re-offered to the caller
    whole and rides to the enclosing component regardless of modes, so a
    ``"self"`` caller may record more after a hit than after a miss — never
    less.)

    Call it as many times as the call has dependencies; recording the same
    dependency twice records it once. Must run inside a component context.

    Adopting ``use_logic_dep`` in a function that memoized callers already
    stored entries for does not re-store those entries (an entry records only
    what its writer offered when it was stored): ship the adoption with a
    ``version`` bump or a memoization-key change on those callers.

    Raises:
        RuntimeError: when called outside an active component context.
        TypeError: when ``dep`` is not a :class:`LogicDep`.
    """
    if not isinstance(dep, LogicDep):
        raise TypeError(
            f"coco.use_logic_dep() takes a coco.LogicDep constructed at module "
            f"import, got {dep!r}"
        )
    get_context_from_ctx()._core_fn_call_ctx.add_fn_logic_dep(dep._logic_fp)

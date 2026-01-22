"""
This module defines *type-level* (static-only) "typestate" markers and a small
typing helper for modeling pending vs resolved variants of objects.

Motivation
----------
Target state providers move through two phases:

- Pending: returned by functions during processing.
- Resolved: the engine resolve it after processing for the component done,
  i.e. during a "submit" phase.

The distinction is primarily to give static type checkers (mypy, pyright) enough
information to prevent using a "pending" value in places that require a "resolved"
value.

Core idea
---------
We represent typestate with phantom marker types:

    PendingS
    ResolvedS

and annotate containers with a typestate parameter:

    TargetStateProvider[PendingS]
    TargetStateProvider[ResolvedS]

For user-defined wrappers that merely *carry* providers, we want to avoid
boilerplate "resolve every field" methods. Instead, we provide a single
typing hook class `ResolvesTo[ResolvedT]`.

The `MaybePendingS` TypeVar defaults to `ResolvedS`, so users can omit the type
parameter in the common case:

    TargetStateProvider == TargetStateProvider[ResolvedS]

Typical usage
-------------
CocoIndex provides the following API to mount and block on the result of a component:

    def mount_run(
        processor_fn: Callable[..., ResolvesTo[T]],
        ...
    ) -> T:
        ...

Then `processor_fn` may return a pending object that implements `ResolvesTo[T]`,
and `mount_run` is typed to return the resolved variant `T`.

If you define a custom struct/class that carries pending/resolved providers
(together with other non-provider values) and they want it to be returned as a
function for users to mount as a component, you can make the struct generic over
the typestate parameter (PendingS/ResolvedS), so provider fields can be annotated
with the same state, and implement the `ResolvesTo` mixin:

    class MyClass(Generic[coco.MaybePendingS], ResolvesTo["MyClass"]):
        p1: TargetStateProvider[str, coco.MaybePendingS]
        p2: TargetStateProvider[str, coco.MaybePendingS]
        label: str  # non-provider fields are fine

        # Annotate the self type to expose methods that are only available in
        # the resolved state.
        def target_state(self: MyClass):
            ...
"""

from __future__ import annotations

from typing import Generic
from typing_extensions import TypeVar


class PendingS:
    """
    Type-level marker representing the "pending" typestate.

    This is a *phantom type* used purely for static typing (mypy/pyright).
    It is not intended to be instantiated, and it does not carry data at runtime.

    Typical usage:
        TargetStateProvider[PendingS]
        UserDefinedStruct[PendingS]
    """

    __slots__ = ()


class ResolvedS:
    """
    Type-level marker representing the "resolved" typestate.

    Like PendingS, this is a phantom type that exists solely for static typing.
    At runtime, Pending and Resolved values may have the exact same representation.

    Typical usage:
    TargetStateProvider[ResolvedS]
    UserDefinedStruct[ResolvedS]
    """

    __slots__ = ()


# The type that a pending object "becomes" after resolution.
# Covariant: if A is a subtype of B, then ResolvesTo[A] is a subtype of ResolvesTo[B].
ResolvedT = TypeVar("ResolvedT", covariant=True)

# Common typestate parameter for types that can be either pending or resolved.
#
# The `default=ResolvedS` means that if a user writes `TargetStateProvider` without
# explicitly parameterizing it, it defaults to the "resolved" state:
#
#     TargetStateProvider       == TargetStateProvider[ResolvedS]
#     UserDefinedStruct         == UserDefinedStruct[ResolvedS]
MaybePendingS = TypeVar("MaybePendingS", PendingS, ResolvedS, default=ResolvedS)


class ResolvesTo(Generic[ResolvedT]):
    """
    Mixin/protocol-like base class for "typestate resolution".

    This is a *static typing hook* that allows APIs like:

        def mount_run(fn: Callable[..., ResolvesTo[T]]) -> T: ...

    Users can return a pending variant from `fn`, and `mount_run` returns the
    resolved variant. The bridge is this method, which tells the type checker
    how to map from "pending container" to "resolved container".
    """

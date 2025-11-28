from typing import Any, Collection, NamedTuple
import pytest

import cocoindex as coco
from cocoindex.tests.environment import create_test_env

coco_env = create_test_env("effect")

# === Define a effect on a global map ===


class _MapDataWithPrev(NamedTuple):
    data: Any
    prev: Collection[Any]
    prev_may_be_missing: bool


_global_map_effect_data: dict[str, _MapDataWithPrev] = {}


def _global_map_effect_sink_fn(
    actions: Collection[tuple[str, _MapDataWithPrev | coco.NonExistenceType]],
) -> None:
    for key, value in actions:
        if coco.is_non_existence(value):
            del _global_map_effect_data[key]
        else:
            _global_map_effect_data[key] = value


_global_map_effect_sink = coco.EffectSink.from_fn(_global_map_effect_sink_fn)


def _global_map_effect_reconciler(
    key: str,
    desired_effect: Any | coco.NonExistenceType,
    prev_possible_states: Collection[Any],
    prev_may_be_missing: bool,
) -> coco.EffectReconcileOutput[
    tuple[str, _MapDataWithPrev | coco.NonExistenceType], Any
]:
    new_value = (
        coco.NON_EXISTENCE
        if coco.is_non_existence(desired_effect)
        else _MapDataWithPrev(
            data=desired_effect,
            prev=prev_possible_states,
            prev_may_be_missing=prev_may_be_missing,
        )
    )
    return coco.EffectReconcileOutput(
        action=(key, new_value),
        sink=_global_map_effect_sink,
        state=desired_effect,
    )


_global_map_effect_provider = coco.register_root_effect_provider(
    "test_effect/global_map",
    coco.EffectReconciler.from_fn(_global_map_effect_reconciler),
)


def _global_map_entry_effect(key: str, value: Any) -> coco.Effect:
    return _global_map_effect_provider.effect(key, value)


# === Tests ===

_source_data: dict[str, Any] = {}


@coco.function
def declare_global_map_entries(csp: coco.StatePath) -> None:
    for key, value in _source_data.items():
        coco.declare_effect(csp, _global_map_entry_effect(key, value))


declare_global_map_entries_app = coco.App(
    declare_global_map_entries,
    coco.AppConfig(name="declare_global_map_entries_app", environment=coco_env),
)


@pytest.mark.skip(
    reason="TODO: Enable this test after implementing effect logic on the engine side."
)
def test_global_map_effect_upserts() -> None:
    _global_map_effect_data.clear()
    _source_data.clear()

    _source_data["a"] = 1
    _source_data["b"] = 2
    declare_global_map_entries_app.update()
    assert _global_map_effect_data == {
        "a": _MapDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
        "b": _MapDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }

    _source_data["a"] = 3
    declare_global_map_entries_app.update()
    assert _global_map_effect_data == {
        "a": _MapDataWithPrev(data=3, prev=[1], prev_may_be_missing=True),
        "b": _MapDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }


@pytest.mark.skip(
    reason="TODO: Enable this test after implementing effect logic on the engine side with respect to deletion."
)
def test_global_map_effect_deletes() -> None:
    _global_map_effect_data.clear()
    _source_data.clear()

    _source_data["a"] = 1
    _source_data["b"] = 2
    declare_global_map_entries_app.update()
    assert _global_map_effect_data == {
        "a": _MapDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
        "b": _MapDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }

    del _source_data["a"]
    declare_global_map_entries_app.update()
    assert _global_map_effect_data == {
        "b": _MapDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }

from typing import Any, Collection, NamedTuple

import cocoindex as coco
from cocoindex.tests.environment import create_test_env

coco_env = create_test_env("effect")

# === Define a effect on a global map ===


class _MapDataWithPrev(NamedTuple):
    data: Any
    prev: Collection[Any]
    prev_may_be_missing: bool


class _MapEffectStore:
    data: dict[str, _MapDataWithPrev] = {}
    num_sink_called: int = 0

    def _sink(
        self,
        actions: Collection[tuple[str, _MapDataWithPrev | coco.NonExistenceType]],
    ) -> None:
        for key, value in actions:
            if coco.is_non_existence(value):
                del self.data[key]
            else:
                self.data[key] = value
        self.num_sink_called += 1

    def reconcile(
        self,
        key: str,
        desired_effect: Any | coco.NonExistenceType,
        prev_possible_states: Collection[Any],
        prev_may_be_missing: bool,
    ) -> (
        coco.EffectReconcileOutput[
            tuple[str, _MapDataWithPrev | coco.NonExistenceType], Any
        ]
        | None
    ):
        # Short-circuit no-change case
        if coco.is_non_existence(desired_effect):
            if len(prev_possible_states) == 0:
                return None
        else:
            if not prev_may_be_missing and all(
                prev == desired_effect for prev in prev_possible_states
            ):
                return None

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
            sink=coco.EffectSink.from_fn(self._sink),
            state=desired_effect,
        )

    def clear(self) -> None:
        self.data.clear()
        self.num_sink_called = 0


_global_map_effect_store = _MapEffectStore()
_global_map_effect_provider = coco.register_root_effect_provider(
    "test_effect/global_map",
    coco.EffectReconciler.from_fn(_global_map_effect_store.reconcile),
)


# === Tests ===

_source_data: dict[str, Any] = {}


@coco.function
def declare_global_map_entries(csp: coco.StatePath) -> None:
    for key, value in _source_data.items():
        coco.declare_effect(csp, _global_map_effect_provider.effect(key, value))


def test_global_map_effect_insert() -> None:
    _global_map_effect_store.clear()
    _source_data.clear()

    app = coco.App(
        declare_global_map_entries,
        coco.AppConfig(name="test_global_map_effect_insert", environment=coco_env),
    )

    _source_data["a"] = 1
    _source_data["b"] = 2
    app.update()
    assert _global_map_effect_store.data == {
        "a": _MapDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
        "b": _MapDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    assert _global_map_effect_store.num_sink_called == 1


def test_global_map_effect_upsert() -> None:
    _global_map_effect_store.clear()
    _source_data.clear()

    app = coco.App(
        declare_global_map_entries,
        coco.AppConfig(name="test_global_map_effect_upsert", environment=coco_env),
    )

    _source_data["a"] = 1
    _source_data["b"] = 2
    app.update()
    assert _global_map_effect_store.data == {
        "a": _MapDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
        "b": _MapDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    assert _global_map_effect_store.num_sink_called == 1

    _source_data["a"] = 3
    app.update()
    assert _global_map_effect_store.data == {
        "a": _MapDataWithPrev(data=3, prev=[1], prev_may_be_missing=False),
        "b": _MapDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    assert _global_map_effect_store.num_sink_called == 2


def test_global_map_effect_delete() -> None:
    _global_map_effect_store.clear()
    _source_data.clear()

    app = coco.App(
        declare_global_map_entries,
        coco.AppConfig(name="test_global_map_effect_delete", environment=coco_env),
    )

    _source_data["a"] = 1
    _source_data["b"] = 2
    app.update()
    assert _global_map_effect_store.data == {
        "a": _MapDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
        "b": _MapDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    assert _global_map_effect_store.num_sink_called == 1

    del _source_data["a"]
    app.update()
    assert _global_map_effect_store.data == {
        "b": _MapDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    assert _global_map_effect_store.num_sink_called == 2


def test_global_map_effect_no_change() -> None:
    _global_map_effect_store.clear()
    _source_data.clear()

    app = coco.App(
        declare_global_map_entries,
        coco.AppConfig(name="test_global_map_effect_no_change", environment=coco_env),
    )

    _source_data["a"] = 1
    _source_data["b"] = 2

    app.update()
    assert _global_map_effect_store.data == {
        "a": _MapDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
        "b": _MapDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    assert _global_map_effect_store.num_sink_called == 1

    app.update()
    assert _global_map_effect_store.data == {
        "a": _MapDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
        "b": _MapDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    assert _global_map_effect_store.num_sink_called == 1

    _source_data["a"] = 3

    app.update()
    assert _global_map_effect_store.data == {
        "a": _MapDataWithPrev(data=3, prev=[1], prev_may_be_missing=False),
        "b": _MapDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    assert _global_map_effect_store.num_sink_called == 2

    app.update()
    assert _global_map_effect_store.data == {
        "a": _MapDataWithPrev(data=3, prev=[1], prev_may_be_missing=False),
        "b": _MapDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    assert _global_map_effect_store.num_sink_called == 2

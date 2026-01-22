from __future__ import annotations

from typing import Any, Collection, Literal, NamedTuple
import threading
import cocoindex as coco


class DictDataWithPrev(NamedTuple):
    data: Any
    prev: Collection[Any]
    prev_may_be_missing: bool


class Metrics:
    data: dict[str, int]

    def __init__(self, data: dict[str, int] | None = None) -> None:
        self.data = data or {}

    def increment(self, metric: str) -> None:
        self.data[metric] = self.data.get(metric, 0) + 1

    def collect(self) -> dict[str, int]:
        m = self.data
        self.data = {}
        return m

    def __repr__(self) -> str:
        return f"Metrics{self.data}"

    def __add__(self, other: Metrics) -> Metrics:
        result = {**self.data}
        for k, v in other.data.items():
            result[k] = result.get(k, 0) + v
        return Metrics(result)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Metrics):
            return self.data == other.data
        elif isinstance(other, dict):
            return self.data == other
        else:
            return False

    def clear(self) -> None:
        self.data.clear()


class DictEffectStore:
    data: dict[str, DictDataWithPrev]
    metrics: Metrics
    _lock: threading.Lock
    _use_async: bool
    sink_exception: bool = False

    def __init__(self, use_async: bool = False) -> None:
        self.data = {}
        self.metrics = Metrics()
        self._lock = threading.Lock()
        self._use_async = use_async

    def _sink(
        self,
        actions: Collection[tuple[str, DictDataWithPrev | coco.NonExistenceType]],
    ) -> None:
        if self.sink_exception:
            raise ValueError("injected sink exception")
        with self._lock:
            for key, value in actions:
                if coco.is_non_existence(value):
                    del self.data[key]
                    self.metrics.increment("delete")
                else:
                    self.data[key] = value
                    self.metrics.increment("upsert")
            self.metrics.increment("sink")

    async def _async_sink(
        self,
        actions: Collection[tuple[str, DictDataWithPrev | coco.NonExistenceType]],
    ) -> None:
        self._sink(actions)

    def reconcile(
        self,
        key: str,
        desired_effect: Any | coco.NonExistenceType,
        prev_possible_states: Collection[Any],
        prev_may_be_missing: bool,
    ) -> (
        coco.TargetReconcileOutput[
            tuple[str, DictDataWithPrev | coco.NonExistenceType], Any
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
            else DictDataWithPrev(
                data=desired_effect,
                prev=prev_possible_states,
                prev_may_be_missing=prev_may_be_missing,
            )
        )
        return coco.TargetReconcileOutput(
            action=(key, new_value),
            sink=(
                coco.TargetActionSink.from_async_fn(self._async_sink)
                if self._use_async
                else coco.TargetActionSink.from_fn(self._sink)
            ),
            state=desired_effect,
        )

    def clear(self) -> None:
        self.data.clear()
        self.metrics.clear()


class GlobalDictTarget:
    store = DictEffectStore()
    _provider = coco.register_root_target_state_provider(
        "test_effect/global_dict", store
    )
    effect = _provider.effect


class AsyncGlobalDictTarget:
    store = DictEffectStore(use_async=True)
    _provider = coco.register_root_target_state_provider(
        "test_effect/global_dict_async", store
    )
    effect = _provider.effect


class _DictEffectStoreAction(NamedTuple):
    name: str
    exists: bool
    action: Literal["insert", "upsert", "delete"] | None


class DictsEffectStore:
    _stores: dict[str, DictEffectStore]
    metrics: Metrics
    _lock: threading.Lock
    _use_async: bool
    sink_exception: bool = False

    def __init__(self, use_async: bool = False) -> None:
        self._stores = {}
        self.metrics = Metrics()
        self._lock = threading.Lock()
        self._use_async = use_async

    def _sink(
        self, actions: Collection[_DictEffectStoreAction]
    ) -> list[coco.ChildTargetDef[DictEffectStore] | None]:
        child_effect_defs: list[coco.ChildTargetDef[DictEffectStore] | None] = []
        if self.sink_exception:
            raise ValueError("injected sink exception")
        with self._lock:
            for name, exists, action in actions:
                if action == "insert":
                    if name in self._stores:
                        raise ValueError(f"store {name} already exists")
                    self._stores[name] = DictEffectStore(use_async=self._use_async)
                elif action == "upsert":
                    if name not in self._stores:
                        self._stores[name] = DictEffectStore(use_async=self._use_async)
                elif action == "delete":
                    del self._stores[name]

                if action is not None:
                    self.metrics.increment(action)

                if exists:
                    child_effect_defs.append(coco.ChildTargetDef(self._stores[name]))
                else:
                    child_effect_defs.append(None)

            self.metrics.increment("sink")
        return child_effect_defs

    async def _async_sink(
        self,
        actions: Collection[_DictEffectStoreAction],
    ) -> list[coco.ChildTargetDef[DictEffectStore] | None]:
        return self._sink(actions)

    def reconcile(
        self,
        key: str,
        desired_effect: None | coco.NonExistenceType,
        prev_possible_states: Collection[None],
        prev_may_be_missing: bool,
    ) -> (
        coco.TargetReconcileOutput[_DictEffectStoreAction, None, DictEffectStore] | None
    ):
        sink: coco.TargetActionSink[_DictEffectStoreAction, DictEffectStore] = (
            coco.TargetActionSink.from_async_fn(self._async_sink)
            if self._use_async
            else coco.TargetActionSink.from_fn(self._sink)
        )
        if coco.is_non_existence(desired_effect):
            return coco.TargetReconcileOutput(
                action=_DictEffectStoreAction(name=key, exists=False, action="delete"),
                sink=sink,
                state=coco.NON_EXISTENCE,
            )
        if not prev_may_be_missing:
            assert len(prev_possible_states) > 0
            return coco.TargetReconcileOutput(
                action=_DictEffectStoreAction(name=key, exists=True, action=None),
                sink=sink,
                state=desired_effect,
            )

        return coco.TargetReconcileOutput(
            action=_DictEffectStoreAction(
                name=key,
                exists=True,
                action="insert" if len(prev_possible_states) == 0 else "upsert",
            ),
            sink=sink,
            state=desired_effect,
        )

    def clear(self) -> None:
        self._stores.clear()
        self.metrics.clear()

    def collect_child_metrics(self) -> dict[str, int]:
        return sum(
            (Metrics(store.metrics.collect()) for store in self._stores.values()),
            Metrics(),
        ).data

    @property
    def data(self) -> dict[str, dict[str, DictDataWithPrev]]:
        return {name: store.data for name, store in self._stores.items()}


class DictsTarget:
    store = DictsEffectStore()
    _provider = coco.register_root_target_state_provider("test_effect/dicts", store)

    @staticmethod
    @coco.function
    def declare_dict_target(
        scope: coco.Scope, name: str
    ) -> coco.PendingTargetStateProvider[str, None]:
        return coco.declare_target_state_with_child(
            scope, DictsTarget._provider.effect(name, None)
        )


class AsyncDictsTarget:
    store = DictsEffectStore(use_async=True)
    _provider = coco.register_root_target_state_provider(
        "test_effect/async_dicts", store
    )

    @staticmethod
    @coco.function
    def declare_dict_target(
        scope: coco.Scope, name: str
    ) -> coco.PendingTargetStateProvider[str, None]:
        return coco.declare_target_state_with_child(
            scope, AsyncDictsTarget._provider.effect(name, None)
        )

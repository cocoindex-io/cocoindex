from typing import Any, Collection, Literal, NamedTuple
import threading
import cocoindex as coco


class DictDataWithPrev(NamedTuple):
    data: Any
    prev: Collection[Any]
    prev_may_be_missing: bool


MetricsName = Literal["sink", "upsert", "delete"]


class Metrics:
    _data: dict[MetricsName, int]

    def __init__(self) -> None:
        self._data = {}

    def increment(self, metric: MetricsName) -> None:
        self._data[metric] = self._data.get(metric, 0) + 1

    def collect(self) -> dict[MetricsName, int]:
        m = self._data
        self._data = {}
        return m

    def __repr__(self) -> str:
        return f"Metrics{self._data}"


class DictEffectStore:
    data: dict[str, DictDataWithPrev]
    metrics: Metrics
    _lock: threading.Lock

    def __init__(self) -> None:
        self.data = {}
        self.metrics = Metrics()
        self._lock = threading.Lock()

    def _sink(
        self,
        actions: Collection[tuple[str, DictDataWithPrev | coco.NonExistenceType]],
    ) -> None:
        with self._lock:
            for key, value in actions:
                if coco.is_non_existence(value):
                    del self.data[key]
                    self.metrics.increment("delete")
                else:
                    self.data[key] = value
                    self.metrics.increment("upsert")
            self.metrics.increment("sink")

    def reconcile(
        self,
        key: str,
        desired_effect: Any | coco.NonExistenceType,
        prev_possible_states: Collection[Any],
        prev_may_be_missing: bool,
    ) -> (
        coco.EffectReconcileOutput[
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
        return coco.EffectReconcileOutput(
            action=(key, new_value),
            sink=coco.EffectSink.from_fn(self._sink),
            state=desired_effect,
        )

    def clear(self) -> None:
        self.data.clear()


class GlobalDictTarget:
    store = DictEffectStore()
    _provider = coco.register_root_effect_provider(
        "test_effect/global_dict",
        coco.EffectReconciler.from_fn(store.reconcile),
    )
    effect = _provider.effect


class DictsEffectStore:
    _stores: dict[str, DictEffectStore]
    metrics: Metrics
    _lock: threading.Lock

    def __init__(self) -> None:
        self._stores = {}
        self.metrics = Metrics()
        self._lock = threading.Lock()

    def _sink(
        self, actions: Collection[tuple[str, bool]]
    ) -> list[coco.EffectReconciler[str] | None]:
        child_recons: list[coco.EffectReconciler[str] | None] = []
        with self._lock:
            for name, exists in actions:
                if exists:
                    if name not in self._stores:
                        self._stores[name] = DictEffectStore()
                        self.metrics.increment("upsert")
                    child_recons.append(
                        coco.EffectReconciler.from_fn(self._stores[name].reconcile)
                    )
                else:
                    del self._stores[name]
                    self.metrics.increment("delete")
                    child_recons.append(None)
            self.metrics.increment("sink")
        return child_recons

    def reconcile(
        self,
        key: str,
        desired_effect: None | coco.NonExistenceType,
        prev_possible_states: Collection[None],
        _prev_may_be_missing: bool,
    ) -> (
        coco.EffectReconcileOutput[tuple[str, bool], None, coco.EffectReconciler[str]]
        | None
    ):
        if desired_effect is not coco.NON_EXISTENCE:
            return coco.EffectReconcileOutput(
                action=(key, True),
                sink=coco.EffectSink.from_fn(self._sink),
                state=coco.NON_EXISTENCE,
            )
        if len(prev_possible_states) == 0:
            return None
        return coco.EffectReconcileOutput(
            action=(key, False),
            sink=coco.EffectSink.from_fn(self._sink),
            state=None,
        )

    def clear(self) -> None:
        self._stores.clear()

    @property
    def data(self) -> dict[str, dict[str, DictDataWithPrev]]:
        return {name: store.data for name, store in self._stores.items()}


class DictsTarget:
    store = DictsEffectStore()
    _provider = coco.register_root_effect_provider(
        "test_effect/dicts",
        coco.EffectReconciler.from_fn(store.reconcile),
    )
    effect = _provider.effect

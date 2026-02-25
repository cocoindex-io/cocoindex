"""Tests for @coco.function decorator on class methods."""

from dataclasses import dataclass

import cocoindex as coco

from tests import common
from tests.common.target_states import (
    DictDataWithPrev,
    GlobalDictTarget,
    Metrics,
)

coco_env = common.create_test_env(__file__)

_metrics = Metrics()


@dataclass(frozen=True)
class SourceDataEntry:
    name: str
    version: int
    content: str

    def __coco_memo_key__(self) -> object:
        return (self.name, self.version)


# ============================================================================
# Test 1: Regular Instance Methods
# ============================================================================


class Processor:
    """Test class with regular instance methods."""

    def __init__(self, prefix: str):
        self.prefix = prefix

    @coco.function(memo=True)
    def transform(self, entry: SourceDataEntry) -> str:
        _metrics.increment("call.transform")
        return f"{self.prefix}: {entry.content}"

    @coco.function
    def process_entry(self, key: str, entry: SourceDataEntry) -> None:
        transformed = self.transform(entry)  # type: ignore[call-arg, arg-type]
        coco.declare_target_state(GlobalDictTarget.target_state(key, transformed))


def test_regular_method() -> None:
    """Test @coco.function on regular instance methods."""
    GlobalDictTarget.store.clear()
    _metrics.clear()

    processor = Processor("processed")
    source_data = {
        "A": SourceDataEntry(name="A", version=1, content="contentA"),
        "B": SourceDataEntry(name="B", version=1, content="contentB"),
    }

    @coco.function
    def process_all() -> None:
        for key, entry in source_data.items():
            processor.process_entry(key, entry)  # type: ignore[call-arg, arg-type]

    app = coco.App(
        coco.AppConfig(name="test_regular_method", environment=coco_env),
        process_all,
    )

    app.update_blocking()
    assert _metrics.collect() == {"call.transform": 2}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="processed: contentA", prev=[], prev_may_be_missing=True
        ),
        "B": DictDataWithPrev(
            data="processed: contentB", prev=[], prev_may_be_missing=True
        ),
    }

    app.update_blocking()
    assert _metrics.collect() == {}


# ============================================================================
# Test 2: Static Methods
# ============================================================================


class StaticProcessor:
    """Test class with static methods."""

    @staticmethod
    @coco.function(memo=True)
    def transform(entry: SourceDataEntry) -> str:
        """Static method with memoization."""
        _metrics.increment("call.static_transform")
        return f"static: {entry.content}"

    @staticmethod
    @coco.function
    def process_entry(key: str, entry: SourceDataEntry) -> None:
        """Static method that uses another memoized static method."""
        transformed = StaticProcessor.transform(entry)
        coco.declare_target_state(GlobalDictTarget.target_state(key, transformed))


def test_static_method() -> None:
    """Test @coco.function on static methods."""
    GlobalDictTarget.store.clear()
    _metrics.clear()

    source_data = {
        "A": SourceDataEntry(name="A", version=1, content="contentA"),
        "B": SourceDataEntry(name="B", version=1, content="contentB"),
    }

    @coco.function
    def process_all() -> None:
        for key, entry in source_data.items():
            StaticProcessor.process_entry(key, entry)

    app = coco.App(
        coco.AppConfig(name="test_static_method", environment=coco_env),
        process_all,
    )

    app.update_blocking()
    assert _metrics.collect() == {"call.static_transform": 2}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="static: contentA", prev=[], prev_may_be_missing=True
        ),
        "B": DictDataWithPrev(
            data="static: contentB", prev=[], prev_may_be_missing=True
        ),
    }

    app.update_blocking()
    assert _metrics.collect() == {}


# ============================================================================
# Test 3: Class Methods
# ============================================================================


class ClassProcessor:
    """Test class with class methods."""

    default_prefix = "class"

    @classmethod
    @coco.function(memo=True)
    def transform(cls, entry: SourceDataEntry) -> str:
        """Class method with memoization."""
        _metrics.increment("call.class_transform")
        return f"{cls.default_prefix}: {entry.content}"

    @classmethod
    @coco.function
    def process_entry(cls, key: str, entry: SourceDataEntry) -> None:
        """Class method that uses another memoized class method."""
        transformed = cls.transform(entry)  # type: ignore[call-arg, arg-type]
        coco.declare_target_state(GlobalDictTarget.target_state(key, transformed))


def test_class_method() -> None:
    """Test @coco.function on class methods."""
    GlobalDictTarget.store.clear()
    _metrics.clear()

    source_data = {
        "A": SourceDataEntry(name="A", version=1, content="contentA"),
        "B": SourceDataEntry(name="B", version=1, content="contentB"),
    }

    @coco.function
    def process_all() -> None:
        for key, entry in source_data.items():
            ClassProcessor.process_entry(key, entry)  # type: ignore[call-arg, arg-type]

    app = coco.App(
        coco.AppConfig(name="test_class_method", environment=coco_env),
        process_all,
    )

    app.update_blocking()
    assert _metrics.collect() == {"call.class_transform": 2}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="class: contentA", prev=[], prev_may_be_missing=True
        ),
        "B": DictDataWithPrev(
            data="class: contentB", prev=[], prev_may_be_missing=True
        ),
    }

    app.update_blocking()
    assert _metrics.collect() == {}


# ============================================================================
# Test 4: Async Instance Methods
# ============================================================================


class AsyncProcessor:
    """Test class with async instance methods."""

    def __init__(self, prefix: str):
        self.prefix = prefix

    @coco.fn.as_async(memo=True)
    async def transform(self, entry: SourceDataEntry) -> str:
        """Async instance method with memoization."""
        _metrics.increment("call.async_transform")
        return f"{self.prefix}: {entry.content}"

    @coco.function
    async def process_entry(self, key: str, entry: SourceDataEntry) -> None:
        """Async instance method that uses another memoized async method."""
        transformed = await self.transform(entry)
        coco.declare_target_state(GlobalDictTarget.target_state(key, transformed))


def test_async_method() -> None:
    """Test @coco.function on async instance methods."""
    GlobalDictTarget.store.clear()
    _metrics.clear()

    processor = AsyncProcessor("async")
    source_data = {
        "A": SourceDataEntry(name="A", version=1, content="contentA"),
        "B": SourceDataEntry(name="B", version=1, content="contentB"),
    }

    @coco.function
    async def process_all() -> None:
        for key, entry in source_data.items():
            await processor.process_entry(key, entry)

    app = coco.App(
        coco.AppConfig(name="test_async_method", environment=coco_env),
        process_all,
    )

    app.update_blocking()
    assert _metrics.collect() == {"call.async_transform": 2}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="async: contentA", prev=[], prev_may_be_missing=True
        ),
        "B": DictDataWithPrev(
            data="async: contentB", prev=[], prev_may_be_missing=True
        ),
    }

    app.update_blocking()
    assert _metrics.collect() == {}


# ============================================================================
# Test 5: Async Class Methods
# ============================================================================


class AsyncClassProcessor:
    """Test class with async class methods."""

    default_prefix = "async_class"

    @classmethod
    @coco.fn.as_async(memo=True)
    async def transform(cls, entry: SourceDataEntry) -> str:
        """Async class method with memoization."""
        _metrics.increment("call.async_class_transform")
        return f"{cls.default_prefix}: {entry.content}"

    @classmethod
    @coco.function
    async def process_entry(cls, key: str, entry: SourceDataEntry) -> None:
        """Async class method that uses another memoized async class method."""
        transformed = await cls.transform(entry)  # type: ignore[call-arg, arg-type]
        coco.declare_target_state(GlobalDictTarget.target_state(key, transformed))


def test_async_class_method() -> None:
    """Test @coco.function on async class methods."""
    GlobalDictTarget.store.clear()
    _metrics.clear()

    source_data = {
        "A": SourceDataEntry(name="A", version=1, content="contentA"),
        "B": SourceDataEntry(name="B", version=1, content="contentB"),
    }

    @coco.function
    async def process_all() -> None:
        for key, entry in source_data.items():
            await AsyncClassProcessor.process_entry(key, entry)  # type: ignore[call-arg, arg-type]

    app = coco.App(
        coco.AppConfig(name="test_async_class_method", environment=coco_env),
        process_all,
    )

    app.update_blocking()
    assert _metrics.collect() == {"call.async_class_transform": 2}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="async_class: contentA", prev=[], prev_may_be_missing=True
        ),
        "B": DictDataWithPrev(
            data="async_class: contentB", prev=[], prev_may_be_missing=True
        ),
    }

    app.update_blocking()
    assert _metrics.collect() == {}

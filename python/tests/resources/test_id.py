"""Tests for stable ID generation utilities."""

import uuid

import cocoindex as coco
from cocoindex.resources.id import (
    IdGenerator,
    UuidGenerator,
    generate_id,
    generate_uuid,
)

from tests import common

coco_env = common.create_test_env(__file__)


# Storage for test results across runs
_generate_id_results: dict[str, int] = {}
_generate_uuid_results: dict[str, uuid.UUID] = {}
_id_generator_results: dict[str, list[int]] = {}
_uuid_generator_results: dict[str, list[uuid.UUID]] = {}


@coco.function
async def _app_main_generate_id(deps: list[str]) -> None:
    """App main that generates IDs for each dependency."""
    for dep in deps:
        _generate_id_results[dep] = await generate_id(dep)


def test_generate_id_stability() -> None:
    """Test that generate_id returns stable IDs across runs for the same dependency."""
    _generate_id_results.clear()

    app = coco.App(
        coco.AppConfig(name="test_generate_id_stability", environment=coco_env),
        _app_main_generate_id,
        deps=["A", "B", "C"],
    )

    # First run
    app.update_blocking()
    first_run_results = dict(_generate_id_results)
    # IDs are sequential integers starting from 1 (0 is reserved)
    assert first_run_results == {"A": 1, "B": 2, "C": 3}

    # Second run - function re-executes since no memo=True
    # If generate_id is stable, we should get the same IDs
    _generate_id_results.clear()
    app.update_blocking()
    second_run_results = dict(_generate_id_results)

    assert first_run_results == second_run_results


def test_generate_id_different_deps() -> None:
    """Test that generate_id returns different IDs for different dependencies."""
    _generate_id_results.clear()

    app = coco.App(
        coco.AppConfig(name="test_generate_id_different_deps", environment=coco_env),
        _app_main_generate_id,
        deps=["X", "Y"],
    )

    app.update_blocking()
    # IDs are sequential integers starting from 1
    assert _generate_id_results == {"X": 1, "Y": 2}


@coco.function
def _app_main_generate_uuid(deps: list[str]) -> None:
    """App main that generates UUIDs for each dependency."""
    for dep in deps:
        _generate_uuid_results[dep] = generate_uuid(dep)


def test_generate_uuid_stability() -> None:
    """Test that generate_uuid returns stable UUIDs across runs for the same dependency."""
    _generate_uuid_results.clear()

    app = coco.App(
        coco.AppConfig(name="test_generate_uuid_stability", environment=coco_env),
        _app_main_generate_uuid,
        deps=["A", "B", "C"],
    )

    # First run
    app.update_blocking()
    first_run_results = dict(_generate_uuid_results)
    assert len(first_run_results) == 3
    # All UUIDs should be unique
    assert len(set(first_run_results.values())) == 3
    # All values should be valid UUIDs
    for v in first_run_results.values():
        assert isinstance(v, uuid.UUID)

    # Second run - function re-executes since no memo=True
    # If generate_uuid is stable, we should get the same UUIDs
    _generate_uuid_results.clear()
    app.update_blocking()
    second_run_results = dict(_generate_uuid_results)

    assert first_run_results == second_run_results


def test_generate_uuid_different_deps() -> None:
    """Test that generate_uuid returns different UUIDs for different dependencies."""
    _generate_uuid_results.clear()

    app = coco.App(
        coco.AppConfig(name="test_generate_uuid_different_deps", environment=coco_env),
        _app_main_generate_uuid,
        deps=["X", "Y"],
    )

    app.update_blocking()
    assert _generate_uuid_results["X"] != _generate_uuid_results["Y"]


@coco.function
async def _app_main_id_generator(deps: list[str], count: int) -> None:
    """App main that generates multiple IDs for each dependency."""
    for dep in deps:
        gen = IdGenerator()
        ids = []
        for _ in range(count):
            ids.append(await gen.next_id(dep))
        _id_generator_results[dep] = ids


@coco.function
async def _app_main_id_generator_with_deps(
    generator_deps: list[str], count: int
) -> None:
    """App main that generates IDs with different constructor deps."""
    for gen_dep in generator_deps:
        gen = IdGenerator(deps=gen_dep)  # Use gen_dep as constructor argument
        ids = []
        for _ in range(count):
            ids.append(await gen.next_id())
        _id_generator_results[gen_dep] = ids


def test_id_generator_multiple_ids() -> None:
    """Test that IdGenerator.next_id returns different IDs for the same dependency."""
    _id_generator_results.clear()

    app = coco.App(
        coco.AppConfig(name="test_id_generator_multiple_ids", environment=coco_env),
        _app_main_id_generator,
        deps=["A"],
        count=5,
    )

    app.update_blocking()
    # IDs are sequential integers starting from 1
    assert _id_generator_results == {"A": [1, 2, 3, 4, 5]}


def test_id_generator_stability() -> None:
    """Test that IdGenerator returns stable ID sequences across runs."""
    _id_generator_results.clear()

    app = coco.App(
        coco.AppConfig(name="test_id_generator_stability", environment=coco_env),
        _app_main_id_generator,
        deps=["A", "B"],
        count=3,
    )

    # First run
    app.update_blocking()
    first_run_results = {k: list(v) for k, v in _id_generator_results.items()}
    # IDs are sequential integers starting from 1, allocated per dep
    assert first_run_results == {"A": [1, 2, 3], "B": [4, 5, 6]}

    # Second run - function re-executes since no memo=True
    _id_generator_results.clear()
    app.update_blocking()
    second_run_results = {k: list(v) for k, v in _id_generator_results.items()}

    assert first_run_results == second_run_results


def test_id_generator_different_deps() -> None:
    """Test that IdGenerator returns different IDs for different dependencies."""
    _id_generator_results.clear()

    app = coco.App(
        coco.AppConfig(name="test_id_generator_different_deps", environment=coco_env),
        _app_main_id_generator,
        deps=["X", "Y"],
        count=2,
    )

    app.update_blocking()
    # IDs are sequential integers starting from 1
    assert _id_generator_results == {"X": [1, 2], "Y": [3, 4]}


def test_id_generator_constructor_deps() -> None:
    """Test that IdGenerator with different constructor deps produce different IDs."""
    _id_generator_results.clear()

    app = coco.App(
        coco.AppConfig(name="test_id_generator_constructor_deps", environment=coco_env),
        _app_main_id_generator_with_deps,
        generator_deps=["A", "B"],
        count=3,
    )

    # First run
    app.update_blocking()
    first_run_results = {k: list(v) for k, v in _id_generator_results.items()}
    # Both generators use the same next_id() call (no arg), but different constructor deps
    # So they should produce distinct ID sequences
    assert first_run_results == {"A": [1, 2, 3], "B": [4, 5, 6]}

    # Second run - verify stability
    _id_generator_results.clear()
    app.update_blocking()
    second_run_results = {k: list(v) for k, v in _id_generator_results.items()}
    assert first_run_results == second_run_results


@coco.function
def _app_main_uuid_generator(deps: list[str], count: int) -> None:
    """App main that generates multiple UUIDs for each dependency."""
    for dep in deps:
        gen = UuidGenerator()
        _uuid_generator_results[dep] = [gen.next_uuid(dep) for _ in range(count)]


@coco.function
def _app_main_uuid_generator_with_deps(generator_deps: list[str], count: int) -> None:
    """App main that generates UUIDs with different constructor deps."""
    for gen_dep in generator_deps:
        gen = UuidGenerator(deps=gen_dep)  # Use gen_dep as constructor argument
        _uuid_generator_results[gen_dep] = [gen.next_uuid() for _ in range(count)]


def test_uuid_generator_multiple_uuids() -> None:
    """Test that UuidGenerator.next_uuid returns different UUIDs for the same dependency."""
    _uuid_generator_results.clear()

    app = coco.App(
        coco.AppConfig(name="test_uuid_generator_multiple_uuids", environment=coco_env),
        _app_main_uuid_generator,
        deps=["A"],
        count=5,
    )

    app.update_blocking()
    uuids = _uuid_generator_results["A"]
    assert len(uuids) == 5
    # All UUIDs should be unique
    assert len(set(uuids)) == 5
    # All should be valid UUIDs
    for u in uuids:
        assert isinstance(u, uuid.UUID)


def test_uuid_generator_stability() -> None:
    """Test that UuidGenerator returns stable UUID sequences across runs."""
    _uuid_generator_results.clear()

    app = coco.App(
        coco.AppConfig(name="test_uuid_generator_stability", environment=coco_env),
        _app_main_uuid_generator,
        deps=["A", "B"],
        count=3,
    )

    # First run
    app.update_blocking()
    first_run_results = {k: list(v) for k, v in _uuid_generator_results.items()}

    # Second run - function re-executes since no memo=True
    _uuid_generator_results.clear()
    app.update_blocking()
    second_run_results = {k: list(v) for k, v in _uuid_generator_results.items()}

    assert first_run_results == second_run_results


def test_uuid_generator_different_deps() -> None:
    """Test that UuidGenerator returns different UUIDs for different dependencies."""
    _uuid_generator_results.clear()

    app = coco.App(
        coco.AppConfig(name="test_uuid_generator_different_deps", environment=coco_env),
        _app_main_uuid_generator,
        deps=["X", "Y"],
        count=2,
    )

    app.update_blocking()
    uuids_x = set(_uuid_generator_results["X"])
    uuids_y = set(_uuid_generator_results["Y"])
    # UUIDs for different deps should be different
    assert uuids_x.isdisjoint(uuids_y)


def test_uuid_generator_constructor_deps() -> None:
    """Test that UuidGenerator with different constructor deps produce different UUIDs."""
    _uuid_generator_results.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_uuid_generator_constructor_deps", environment=coco_env
        ),
        _app_main_uuid_generator_with_deps,
        generator_deps=["A", "B"],
        count=3,
    )

    # First run
    app.update_blocking()
    first_run_results = {k: list(v) for k, v in _uuid_generator_results.items()}
    # Both generators use the same next_uuid() call (no arg), but different constructor deps
    # So they should produce distinct UUID sequences
    uuids_a = set(first_run_results["A"])
    uuids_b = set(first_run_results["B"])
    assert len(uuids_a) == 3
    assert len(uuids_b) == 3
    assert uuids_a.isdisjoint(uuids_b)

    # Second run - verify stability
    _uuid_generator_results.clear()
    app.update_blocking()
    second_run_results = {k: list(v) for k, v in _uuid_generator_results.items()}
    assert first_run_results == second_run_results

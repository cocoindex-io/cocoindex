"""Tests for TypeChecker runtime type validation."""

import uuid
from typing import Any

import pytest

from cocoindex._internal.datatype import TypeChecker


# ---------------------------------------------------------------------------
# Simple types
# ---------------------------------------------------------------------------


class TestSimpleTypes:
    def test_str_accepts_str(self) -> None:
        checker = TypeChecker(str)
        assert checker.check("hello") == "hello"

    def test_str_rejects_int(self) -> None:
        checker = TypeChecker(str)
        with pytest.raises(TypeError, match="expected str"):
            checker.check(42)

    def test_int_accepts_int(self) -> None:
        checker = TypeChecker(int)
        assert checker.check(7) == 7

    def test_int_rejects_str(self) -> None:
        checker = TypeChecker(int)
        with pytest.raises(TypeError, match="expected int"):
            checker.check("hello")

    def test_uuid_accepts_uuid(self) -> None:
        checker = TypeChecker(uuid.UUID)
        val = uuid.uuid4()
        assert checker.check(val) is val

    def test_uuid_rejects_str(self) -> None:
        checker = TypeChecker(uuid.UUID)
        with pytest.raises(TypeError, match="expected UUID"):
            checker.check("not-a-uuid")

    def test_none_type(self) -> None:
        checker = TypeChecker(type(None))
        assert checker.check(None) is None
        with pytest.raises(TypeError, match="expected None"):
            checker.check(0)


# ---------------------------------------------------------------------------
# Union types
# ---------------------------------------------------------------------------


class TestUnionTypes:
    def test_str_or_int_accepts_both(self) -> None:
        checker: TypeChecker[str | int] = TypeChecker(str | int)  # type: ignore[arg-type]
        assert checker.check("hello") == "hello"
        assert checker.check(42) == 42

    def test_str_or_int_rejects_float(self) -> None:
        checker: TypeChecker[str | int] = TypeChecker(str | int)  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="got float"):
            checker.check(3.14)

    def test_optional_str(self) -> None:
        checker: TypeChecker[str | None] = TypeChecker(str | None)  # type: ignore[arg-type]
        assert checker.check("hello") == "hello"
        assert checker.check(None) is None

    def test_optional_str_rejects_int(self) -> None:
        checker: TypeChecker[str | None] = TypeChecker(str | None)  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            checker.check(42)


# ---------------------------------------------------------------------------
# Fixed-length tuple types
# ---------------------------------------------------------------------------


class TestFixedTuple:
    def test_accepts_correct_types(self) -> None:
        checker = TypeChecker(tuple[str, str])
        assert checker.check(("a", "b")) == ("a", "b")

    def test_rejects_wrong_element_type(self) -> None:
        """The motivating bug: tuple[int, int] must NOT pass a tuple[str, str] check."""
        checker = TypeChecker(tuple[str, str])
        with pytest.raises(TypeError, match=r"\[0\]"):
            checker.check((1, 2))

    def test_rejects_wrong_length(self) -> None:
        checker = TypeChecker(tuple[str, str])
        with pytest.raises(TypeError, match="length"):
            checker.check(("a", "b", "c"))

    def test_rejects_non_tuple(self) -> None:
        checker = TypeChecker(tuple[str, str])
        with pytest.raises(TypeError, match="expected tuple"):
            checker.check("not a tuple")

    def test_mixed_types(self) -> None:
        checker = TypeChecker(tuple[str | None, str])
        assert checker.check((None, "path")) == (None, "path")
        assert checker.check(("key", "path")) == ("key", "path")

    def test_mixed_types_rejects_none_in_wrong_position(self) -> None:
        checker = TypeChecker(tuple[str | None, str])
        with pytest.raises(TypeError, match=r"\[1\]"):
            checker.check(("key", None))

    def test_three_elements(self) -> None:
        checker = TypeChecker(tuple[str, str | None, str])
        assert checker.check(("db", None, "table")) == ("db", None, "table")
        assert checker.check(("db", "public", "table")) == ("db", "public", "table")

    def test_three_elements_rejects_wrong_type(self) -> None:
        checker = TypeChecker(tuple[str, str | None, str])
        with pytest.raises(TypeError, match=r"\[0\]"):
            checker.check((123, None, "table"))


# ---------------------------------------------------------------------------
# Variable-length tuple types
# ---------------------------------------------------------------------------


class TestVariadicTuple:
    def test_any_tuple_accepts_any_tuple(self) -> None:
        checker = TypeChecker(tuple[Any, ...])
        assert checker.check(()) == ()
        assert checker.check((1, "a", None)) == (1, "a", None)

    def test_any_tuple_rejects_non_tuple(self) -> None:
        checker = TypeChecker(tuple[Any, ...])
        with pytest.raises(TypeError, match="expected tuple"):
            checker.check([1, 2, 3])

    def test_typed_variadic(self) -> None:
        checker = TypeChecker(tuple[str, ...])
        assert checker.check(("a", "b", "c")) == ("a", "b", "c")
        assert checker.check(()) == ()

    def test_typed_variadic_rejects_wrong_element(self) -> None:
        checker = TypeChecker(tuple[str, ...])
        with pytest.raises(TypeError, match=r"\[1\]"):
            checker.check(("a", 42, "c"))


# ---------------------------------------------------------------------------
# Any type
# ---------------------------------------------------------------------------


class TestAnyType:
    def test_any_accepts_everything(self) -> None:
        checker: TypeChecker[Any] = TypeChecker(Any)  # type: ignore[arg-type]
        assert checker.check(42) == 42
        assert checker.check("hello") == "hello"
        assert checker.check(None) is None
        assert checker.check((1, 2)) == (1, 2)


# ---------------------------------------------------------------------------
# Return value identity
# ---------------------------------------------------------------------------


class TestReturnValue:
    def test_returns_same_object(self) -> None:
        """check() must return the exact same object, not a copy."""
        checker = TypeChecker(tuple[str, str])
        val = ("a", "b")
        assert checker.check(val) is val


# ---------------------------------------------------------------------------
# repr
# ---------------------------------------------------------------------------


class TestRepr:
    def test_repr(self) -> None:
        checker = TypeChecker(str)
        assert "TypeChecker" in repr(checker)

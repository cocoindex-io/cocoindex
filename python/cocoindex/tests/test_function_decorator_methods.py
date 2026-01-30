import pytest

import cocoindex as coco


def test_instance_method_scope_position_enforced() -> None:
    with pytest.raises(ValueError):

        class C:
            @coco.function()
            def m(self, x, scope, y):  # type: ignore[no-untyped-def]
                pass


def test_classmethod_scope_position_enforced_after_decorator() -> None:
    with pytest.raises(ValueError):

        class C:
            @classmethod
            @coco.function()
            def m(cls, x, scope, y):  # type: ignore[no-untyped-def]
                pass


def test_classmethod_scope_position_enforced_before_decorator() -> None:
    with pytest.raises(ValueError):

        class C:
            @coco.function()
            @classmethod
            def m(cls, x, scope, y):  # type: ignore[no-untyped-def]
                pass


def test_static_method_scope_position_enforced() -> None:
    with pytest.raises(ValueError):

        class C:
            @coco.function()
            @staticmethod
            def sm(x, scope, y):  # type: ignore[no-untyped-def]
                pass


def test_valid_decorations_do_not_raise_and_set_op_kind() -> None:
    class C:
        @coco.function()
        def im(self, scope=None):  # type: ignore[no-untyped-def]
            pass

        @classmethod
        @coco.function()
        def cm(cls, scope=None):  # type: ignore[no-untyped-def]
            pass

        @coco.function()
        @classmethod
        def cm2(cls, scope=None):  # type: ignore[no-untyped-def]
            pass

        @staticmethod
        @coco.function()
        def sm(scope=None):  # type: ignore[no-untyped-def]
            pass

        @coco.function()
        @staticmethod
        def sm2(scope=None):  # type: ignore[no-untyped-def]
            pass

    assert C.__dict__["im"].__cocoindex_op_kind__ == "Im"
    assert C.__dict__["cm"].__func__.__cocoindex_op_kind__ == "Cm"
    assert C.__dict__["cm2"].__func__.__cocoindex_op_kind__ == "Cm2"
    assert C.__dict__["sm"].__func__.__cocoindex_op_kind__ == "Sm"
    assert C.__dict__["sm2"].__func__.__cocoindex_op_kind__ == "Sm2"

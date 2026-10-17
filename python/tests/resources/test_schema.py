"""Tests for VectorSchema dtype normalization (cocoindex-io/cocoindex#2535)."""

import numpy as np
import pytest

from cocoindex._internal.serde import get_deserialize_fn, serialize
from cocoindex.resources.schema import VectorSchema


class TestVectorSchemaDtypeNormalization:
    def test_scalar_type_class_normalized_to_dtype_instance(self) -> None:
        """The documented `dtype=np.float32` (class) form is coerced to `np.dtype`."""
        schema = VectorSchema(dtype=np.float32, size=384)
        assert isinstance(schema.dtype, np.dtype)
        assert schema.dtype == np.dtype("float32")

    def test_dtype_instance_unchanged(self) -> None:
        schema = VectorSchema(dtype=np.dtype(np.float32), size=384)
        assert schema.dtype == np.dtype("float32")

    def test_dtype_string_normalized(self) -> None:
        schema = VectorSchema(dtype="float32", size=384)
        assert schema.dtype == np.dtype("float32")

    def test_forms_compare_equal_and_hash_equal(self) -> None:
        from_class = VectorSchema(dtype=np.float32, size=384)
        from_instance = VectorSchema(dtype=np.dtype("float32"), size=384)
        assert from_class == from_instance
        assert hash(from_class) == hash(from_instance)

    def test_invalid_dtype_rejected_at_construction(self) -> None:
        with pytest.raises(TypeError):
            VectorSchema(dtype="not-a-dtype", size=384)

    def test_class_form_survives_serde_round_trip(self) -> None:
        """Regression test for #2535: the class form used to fail deserialization
        on the second run (`UnpicklingError: Forbidden global ... numpy.float32`)."""
        schema = VectorSchema(dtype=np.float32, size=384)
        restored = get_deserialize_fn(VectorSchema)(serialize(schema))
        assert restored == schema
        assert isinstance(restored.dtype, np.dtype)
        assert restored.dtype == np.dtype("float32")

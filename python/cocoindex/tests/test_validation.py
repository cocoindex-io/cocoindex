"""Tests for naming validation functionality."""

import pytest
from cocoindex.validation import (
    validate_field_name,
    validate_flow_name,
    validate_full_flow_name,
    validate_app_namespace_name,
    validate_target_name,
    NamingError,
    validate_identifier_name,
)


class TestValidateIdentifierName:
    """Test the core validation function."""

    def test_valid_names(self) -> None:
        """Test that valid names pass validation."""
        valid_names = [
            "field1",
            "field_name",
            "_private",
            "a",
            "field123",
            "FIELD_NAME",
            "MyField",
            "field_123_test",
        ]

        for name in valid_names:
            result = validate_identifier_name(name)
            assert result is None, f"Valid name '{name}' failed validation: {result}"

    def test_valid_names_with_dots(self) -> None:
        """Test that valid names with dots pass validation when allowed."""
        valid_names = ["app.flow", "my_app.my_flow", "namespace.sub.flow", "a.b.c.d"]

        for name in valid_names:
            result = validate_identifier_name(name, allow_dots=True)
            assert result is None, (
                f"Valid dotted name '{name}' failed validation: {result}"
            )

    def test_invalid_starting_characters(self) -> None:
        """Test names with invalid starting characters."""
        invalid_names = [
            "123field",  # starts with digit
            ".field",  # starts with dot
            "-field",  # starts with dash
            " field",  # starts with space
        ]

        for name in invalid_names:
            result = validate_identifier_name(name)
            assert result is not None, (
                f"Invalid name '{name}' should have failed validation"
            )

    def test_double_underscore_restriction(self) -> None:
        """Test double underscore restriction."""
        invalid_names = ["__reserved", "__internal", "__test"]

        for name in invalid_names:
            result = validate_identifier_name(name)
            assert result is not None
            assert "double underscores" in result.lower()

    def test_length_restriction(self) -> None:
        """Test maximum length restriction."""
        long_name = "a" * 65
        result = validate_identifier_name(long_name, max_length=64)
        assert result is not None
        assert "maximum length" in result.lower()


class TestSpecificValidators:
    """Test the specific validation functions."""

    def test_valid_field_names(self) -> None:
        """Test valid field names."""
        valid_names = ["field1", "field_name", "_private", "FIELD"]
        for name in valid_names:
            validate_field_name(name)  # Should not raise

    def test_invalid_field_names(self) -> None:
        """Test invalid field names raise NamingError."""
        invalid_names = ["123field", "field-name", "__reserved", "a" * 65]

        for name in invalid_names:
            with pytest.raises(NamingError):
                validate_field_name(name)

    def test_flow_validation(self) -> None:
        """Test flow name validation."""
        # Valid flow names
        validate_flow_name("MyFlow")
        validate_flow_name("my_flow_123")

        # Invalid flow names
        with pytest.raises(NamingError):
            validate_flow_name("123flow")

        with pytest.raises(NamingError):
            validate_flow_name("__reserved_flow")

    def test_full_flow_name_allows_dots(self) -> None:
        """Test that full flow names allow dots."""
        validate_full_flow_name("app.my_flow")
        validate_full_flow_name("namespace.subnamespace.flow")

        # But still reject invalid patterns
        with pytest.raises(NamingError):
            validate_full_flow_name("123.invalid")

    def test_target_validation(self) -> None:
        """Test target name validation."""
        validate_target_name("my_target")
        validate_target_name("output_table")

        with pytest.raises(NamingError):
            validate_target_name("123target")

    def test_app_namespace_validation(self) -> None:
        """Test app namespace validation."""
        validate_app_namespace_name("myapp")
        validate_app_namespace_name("my_app_123")

        # Should not allow dots in app namespace
        with pytest.raises(NamingError):
            validate_app_namespace_name("my.app")

        with pytest.raises(NamingError):
            validate_app_namespace_name("123app")

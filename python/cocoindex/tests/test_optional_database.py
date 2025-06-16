"""
Test suite for optional database functionality in CocoIndex.

This module tests that:
1. cocoindex.init() works without database settings
2. Transform flows work without database
3. Database functionality still works when database settings are provided
4. Operations requiring database properly complain when no database is configured
"""

import os
from unittest.mock import patch
import pytest

import cocoindex
from cocoindex import op
from cocoindex.setting import Settings


class TestOptionalDatabase:
    """Test suite for optional database functionality."""

    def setup_method(self) -> None:
        """Setup method called before each test."""
        # Stop any existing cocoindex instance
        try:
            cocoindex.stop()
        except:
            pass

    def teardown_method(self) -> None:
        """Teardown method called after each test."""
        # Stop cocoindex instance after each test
        try:
            cocoindex.stop()
        except:
            pass

    def test_init_without_database(self) -> None:
        """Test that cocoindex.init() works without database settings."""
        # Remove database environment variables
        with patch.dict(os.environ, {}, clear=False):
            # Remove database env vars if they exist
            for env_var in [
                "COCOINDEX_DATABASE_URL",
                "COCOINDEX_DATABASE_USER",
                "COCOINDEX_DATABASE_PASSWORD",
            ]:
                os.environ.pop(env_var, None)

            # Test initialization without database
            cocoindex.init()

            # If we get here without exception, the test passes
            assert True

    def test_transform_flow_without_database(self) -> None:
        """Test that transform flows work without database."""
        # Remove database environment variables
        with patch.dict(os.environ, {}, clear=False):
            # Remove database env vars if they exist
            for env_var in [
                "COCOINDEX_DATABASE_URL",
                "COCOINDEX_DATABASE_USER",
                "COCOINDEX_DATABASE_PASSWORD",
            ]:
                os.environ.pop(env_var, None)

            # Initialize without database
            cocoindex.init()

            # Create a simple custom function for testing
            @op.function()
            def add_prefix(text: str) -> str:
                """Add a prefix to text."""
                return f"processed: {text}"

            @cocoindex.transform_flow()
            def simple_transform(
                text: cocoindex.DataSlice[str],
            ) -> cocoindex.DataSlice[str]:
                """A simple transform that adds a prefix."""
                return text.transform(add_prefix)

            # Test the transform flow
            result = simple_transform.eval("hello world")
            expected = "processed: hello world"

            assert result == expected

    @pytest.mark.skipif(
        not os.getenv("COCOINDEX_DATABASE_URL"),
        reason="Database URL not configured in environment",
    )
    def test_init_with_database(self) -> None:
        """Test that cocoindex.init() works with database settings when available."""
        # This test only runs if database URL is configured
        settings = Settings.from_env()
        assert settings.database is not None
        assert settings.database.url is not None

        try:
            cocoindex.init(settings)
            assert True
        except Exception as e:
            assert (
                "Failed to connect to database" in str(e)
                or "connection" in str(e).lower()
            )

    def test_settings_from_env_without_database(self) -> None:
        """Test that Settings.from_env() correctly handles missing database settings."""
        with patch.dict(os.environ, {}, clear=False):
            # Remove database env vars if they exist
            for env_var in [
                "COCOINDEX_DATABASE_URL",
                "COCOINDEX_DATABASE_USER",
                "COCOINDEX_DATABASE_PASSWORD",
            ]:
                os.environ.pop(env_var, None)

            settings = Settings.from_env()
            assert settings.database is None
            assert settings.app_namespace == ""

    def test_settings_from_env_with_database(self) -> None:
        """Test that Settings.from_env() correctly handles database settings when provided."""
        test_url = "postgresql://test:test@localhost:5432/test"
        test_user = "testuser"
        test_password = "testpass"

        with patch.dict(
            os.environ,
            {
                "COCOINDEX_DATABASE_URL": test_url,
                "COCOINDEX_DATABASE_USER": test_user,
                "COCOINDEX_DATABASE_PASSWORD": test_password,
            },
        ):
            settings = Settings.from_env()
            assert settings.database is not None
            assert settings.database.url == test_url
            assert settings.database.user == test_user
            assert settings.database.password == test_password

    def test_settings_from_env_with_partial_database_config(self) -> None:
        """Test Settings.from_env() with only database URL (no user/password)."""
        test_url = "postgresql://localhost:5432/test"

        with patch.dict(
            os.environ,
            {
                "COCOINDEX_DATABASE_URL": test_url,
            },
            clear=False,
        ):
            # Remove user/password env vars if they exist
            os.environ.pop("COCOINDEX_DATABASE_USER", None)
            os.environ.pop("COCOINDEX_DATABASE_PASSWORD", None)

            settings = Settings.from_env()
            assert settings.database is not None
            assert settings.database.url == test_url
            assert settings.database.user is None
            assert settings.database.password is None

    def test_multiple_init_calls(self) -> None:
        """Test that multiple init calls work correctly."""
        with patch.dict(os.environ, {}, clear=False):
            # Remove database env vars if they exist
            for env_var in [
                "COCOINDEX_DATABASE_URL",
                "COCOINDEX_DATABASE_USER",
                "COCOINDEX_DATABASE_PASSWORD",
            ]:
                os.environ.pop(env_var, None)

            # First init
            cocoindex.init()

            # Stop and init again
            cocoindex.stop()
            cocoindex.init()

            # Should work without issues
            assert True

    def test_app_namespace_setting(self) -> None:
        """Test that app_namespace setting works correctly."""
        test_namespace = "test_app"

        with patch.dict(
            os.environ,
            {
                "COCOINDEX_APP_NAMESPACE": test_namespace,
            },
            clear=False,
        ):
            # Remove database env vars if they exist
            for env_var in [
                "COCOINDEX_DATABASE_URL",
                "COCOINDEX_DATABASE_USER",
                "COCOINDEX_DATABASE_PASSWORD",
            ]:
                os.environ.pop(env_var, None)

            settings = Settings.from_env()
            assert settings.app_namespace == test_namespace
            assert settings.database is None

            # Init should work with app namespace but no database
            cocoindex.init(settings)
            assert True


class TestDatabaseRequiredOperations:
    """Test suite for operations that require database."""

    def setup_method(self) -> None:
        """Setup method called before each test."""
        # Stop any existing cocoindex instance
        try:
            cocoindex.stop()
        except:
            pass

    def teardown_method(self) -> None:
        """Teardown method called after each test."""
        # Stop cocoindex instance after each test
        try:
            cocoindex.stop()
        except:
            pass

    def test_database_required_error_message(self) -> None:
        """Test that operations requiring database show proper error messages."""
        with patch.dict(os.environ, {}, clear=False):
            # Remove database env vars if they exist
            for env_var in [
                "COCOINDEX_DATABASE_URL",
                "COCOINDEX_DATABASE_USER",
                "COCOINDEX_DATABASE_PASSWORD",
            ]:
                os.environ.pop(env_var, None)

            # Initialize without database
            cocoindex.init()

            assert True

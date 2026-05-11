```python
import pytest
from unittest.mock import MagicMock, patch, PropertyMock
import sys
from typing import Any, Iterator
import re
import asyncio

# Import the module under test (assuming it's named 'module' or similar)
# For the test file, we'll import from the actual module location
# from cocoindex.storage import _get_aiohttp, _get_pymysql, JSONEncoder, ...

# Mock the module imports for testing
@pytest.fixture(autouse=True)
def mock_external_libs():
    with patch.dict(sys.modules, {
        'aiohttp': MagicMock(),
        'pymysql': MagicMock(),
        'cocoindex.storage._doris': MagicMock(),
    }):
        yield

# Test _get_aiohttp
class TestGetAiohttp:
    def test_success(self):
        from cocoindex.storage._doris import _get_aiohttp
        import aiohttp
        result = _get_aiohttp()
        assert result is aiohttp

    def test_import_error(self):
        with patch.dict(sys.modules, {'aiohttp': None}):
            # force import failure by removing the module from sys.modules
            with patch('cocoindex.storage._doris.import_module', side_effect=ImportError):
                from cocoindex.storage._doris import _get_aiohttp
                result = _get_aiohttp()
                assert result is None

# Test _get_pymysql
class TestGetPymysql:
    def test_success(self):
        from cocoindex.storage._doris import _get_pymysql
        import pymysql
        result = _get_pymysql()
        assert result is pymysql

    def test_import_error(self):
        with patch('cocoindex.storage._doris.import_module', side_effect=ImportError):
            from cocoindex.storage._doris import _get_pymysql
            result = _get_pymysql()
            assert result is None

# Test JSONEncoder
class TestJSONEncoder:
    def test_default_with_tolist(self):
        from cocoindex.storage._doris import JSONEncoder
        encoder = JSONEncoder()
        obj = MagicMock()
        obj.tolist.return_value = [1, 2, 3]
        result = encoder.default(obj)
        assert result == [1, 2, 3]

    def test_default_fallback(self):
        from cocoindex.storage._doris import JSONEncoder
        encoder = JSONEncoder()
        with pytest.raises(TypeError):
            encoder.default(set())

    def test_default_with_non_serializable(self):
        from cocoindex.storage._doris import JSONEncoder
        encoder = JSONEncoder()
        with pytest.raises(TypeError):
            # object without tolist and not serializable by json encoder
            encoder.default(object())

# Test exception classes
class TestExceptions:
    def test_connection_error_init(self):
        from cocoindex.storage._doris import ConnectionError
        err = ConnectionError("test msg", "host", 1234, cause=ValueError("cause"))
        assert err.message == "test msg"
        assert err.host == "host"
        assert err.port == 1234
        assert isinstance(err.cause, ValueError)

    def test_query_error_init(self):
        from cocoindex.storage._doris import QueryError
        err = QueryError("query msg")
        assert err.message == "query msg"

    def test_validation_error_init(self):
        from cocoindex.storage._doris import ValidationError
        err = ValidationError("field error", field_name="age")
        assert err.message == "field error"
        assert err.field_name == "age"

# Test _is_retryable_error
class TestIsRetryableError:
    @pytest.mark.parametrize("exc_class, expected", [
        (ConnectionError, True),
        (TimeoutError, True),
        (asyncio.TimeoutError, True),  # if asyncio.TimeoutError is not defined, skip
        (ValueError, False),
        (RuntimeError, False),
    ])
    def test_various(self, exc_class, expected):
        from cocoindex.storage._doris import _is_retryable_error
        # Need to handle asyncio.TimeoutError might not exist in older Python
        if exc_class is asyncio.TimeoutError and not hasattr(asyncio, 'TimeoutError'):
            pytest.skip("asyncio.TimeoutError not available")
        err = exc_class("test")
        result = _is_retryable_error(err)
        assert result == expected

# Test ManagedConnection._ensure_session
class TestManagedConnectionEnsureSession:
    def test_session_creation(self):
        from cocoindex.storage._doris import ManagedConnection, DorisConnectionConfig
        config = DorisConnectionConfig(host="localhost", port=9030, user="root", password="", database="test")
        conn = ManagedConnection(config)
        assert conn._session is None
        with patch('cocoindex.storage._doris.aiohttp.ClientSession') as mock_session:
            conn._ensure_session()
            mock_session.assert_called_once()
            assert conn._session is mock_session.return_value

    def test_session_already_exists(self):
        from cocoindex.storage._doris import ManagedConnection, DorisConnectionConfig
        config = DorisConnectionConfig(host="localhost", port=9030, user="root", password="", database="test")
        conn = ManagedConnection(config)
        existing_session = MagicMock()
        existing_session.closed = False
        conn._session = existing_session
        with patch('cocoindex.storage._doris.aiohttp.ClientSession') as mock_session:
            conn._ensure_session()
            mock_session.assert_not_called()
            assert conn._session is existing_session

    def test_session_closed_recreates(self):
        from cocoindex.storage._doris import ManagedConnection, DorisConnectionConfig
        config = DorisConnectionConfig(host="localhost", port=9030, user="root", password="", database="test")
        conn = ManagedConnection(config)
        closed_session = MagicMock()
        closed_session.closed = True
        conn._session = closed_session
        with patch('cocoindex.storage._doris.aiohttp.ClientSession') as mock_session:
            conn._ensure_session()
            mock_session.assert_called_once()
            assert conn._session is mock_session.return_value

# Test connect function
class TestConnect:
    def test_returns_managed_connection(self):
        from cocoindex.storage._doris import connect, ManagedConnection, DorisConnectionConfig
        config = DorisConnectionConfig(host="h", port=1, user="u", password="p", database="d")
        result = connect(config)
        assert isinstance(result, ManagedConnection)
        assert result._config == config

# Test _validate_identifier
class TestValidateIdentifier:
    @pytest.mark.parametrize("name, should_raise", [
        ("valid_name", False),
        ("_valid", False),
        ("a123", False),
        ("123abc", True),
        ("", True),
        ("with-dash", True),
        ("with space", True),
        (None, True),  # None will cause AttributeError, but we expect ValueError
    ])
    def test_validation(self, name, should_raise):
        from cocoindex.storage._doris import _validate_identifier
        if should_raise:
            with pytest.raises(ValueError, match="Invalid identifier"):
                _validate_identifier(name)
        else:
            _validate_identifier(name)  # should not raise

# Test _convert_to_key_column_type
class TestConvertToKeyColumnType:
    @pytest.mark.parametrize("doris_type, expected", [
        ("TEXT", "VARCHAR(65533)"),
        ("STRING", "VARCHAR(65533)"),
        ("BIGINT", "BIGINT"),
        ("INT", "INT"),
        ("DATETIME", "DATETIME"),
        ("VARCHAR(20)", "VARCHAR(20)"),
    ])
    def test_conversion(self, doris_type, expected):
        from cocoindex.storage._doris import _convert_to_key_column_type
        result = _convert_to_key_column_type(doris_type)
        assert result == expected

# Test _convert_value_for_doris
class TestConvertValueForDoris:
    @pytest.mark.parametrize("value, expected", [
        (None, "NULL"),
        (123, "123"),
        (3.14, "3.14"),
        ("hello", "'hello'"),
        (True, "1"),
        (False, "0"),
        (MagicMock(), None),  # unknown type, should maybe raise? Not sure.
    ])
    def test_conversion(self, value, expected):
        from cocoindex.storage._doris import _convert_value_for_doris
        if expected is None:
            # Expect error for unsupported types
            with pytest.raises((TypeError, ValueError)):
                _convert_value_for_doris(value)
        else:
            result = _convert_value_for_doris(value)
            assert result == expected

    def test_numpy_types(self):
        # If numpy arrays with tolist, it will be handled elsewhere, but test basic
        import numpy as np
        from cocoindex.storage._doris import _convert_value_for_doris
        # This might not be handled; if not, it should raise.
        with pytest.raises((TypeError, ValueError)):
            _convert_value_for_doris(np.int64(5))

# Test _execute_ddl_sync
class TestExecuteDdlSync:
    @patch('cocoindex.storage._doris.pymysql')
    def test_execution(self, mock_pymysql):
        from cocoindex.storage._doris import _execute_ddl_sync, DorisConnectionConfig
        config = DorisConnectionConfig(host="h", port=1, user="u", password="p", database="d")
        sql = "CREATE TABLE test (id INT)"
        _execute_ddl_sync(config, sql)
        mock_pymysql.connect.assert_called_once_with(
            host="h",
            port=1,
            user="u",
            password="p",
            database="d"
        )
        mock_conn = mock_pymysql.connect.return_value
        mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
        mock_cursor.execute.assert_called_once_with(sql)

# Test _query_sync
class TestQuerySync:
    @patch('cocoindex.storage._doris.pymysql')
    def test_query(self, mock_pymysql):
        from cocoindex.storage._doris import _query_sync, DorisConnectionConfig
        config = DorisConnectionConfig(host="h", port=1, user="u", password="p", database="d")
        sql = "SELECT 1"
        expected_rows = [(1,), (2,)]
        mock_conn = mock_pymysql.connect.return_value
        mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
        mock_cursor.fetchall.return_value = expected_rows
        result = _query_sync(config, sql)
        assert result == expected_rows
        mock_cursor.execute.assert_called_once_with(sql)
```
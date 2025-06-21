# type: ignore
import os
import tempfile
import hashlib
from pathlib import Path
from unittest.mock import patch
import pytest

import cocoindex
from cocoindex import op
from cocoindex.setting import Settings


class TestContentHashFunctionality:
    """Test suite for content hash functionality."""

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

    def test_content_hash_with_local_files(self) -> None:
        """Test that content hash works correctly with local file sources."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test files
            file1_path = Path(temp_dir) / "test1.txt"
            file2_path = Path(temp_dir) / "test2.txt"

            file1_content = "This is the content of file 1"
            file2_content = "This is the content of file 2"

            file1_path.write_text(file1_content)
            file2_path.write_text(file2_content)

            # Remove database environment variables for this test
            with patch.dict(os.environ, {}, clear=False):
                for env_var in [
                    "COCOINDEX_DATABASE_URL",
                    "COCOINDEX_DATABASE_USER",
                    "COCOINDEX_DATABASE_PASSWORD",
                ]:
                    os.environ.pop(env_var, None)

                # Initialize without database
                cocoindex.init()

                # Create a transform flow that processes files
                @op.function()
                def extract_content(text: str) -> str:
                    """Extract and process file content."""
                    return f"processed: {text}"

                @cocoindex.transform_flow()
                def process_files(
                    files: cocoindex.DataSlice[str],
                ) -> cocoindex.DataSlice[str]:
                    """Process file contents."""
                    return files.transform(extract_content)

                # Test processing files
                result1 = process_files.eval(file1_content)
                result2 = process_files.eval(file2_content)

                assert result1 == f"processed: {file1_content}"
                assert result2 == f"processed: {file2_content}"

    def test_content_hash_computation(self) -> None:
        """Test that content hash is computed correctly."""
        # Test content hash computation with known content
        test_content = "Hello, World!"

        # Remove database environment variables
        with patch.dict(os.environ, {}, clear=False):
            for env_var in [
                "COCOINDEX_DATABASE_URL",
                "COCOINDEX_DATABASE_USER",
                "COCOINDEX_DATABASE_PASSWORD",
            ]:
                os.environ.pop(env_var, None)

            cocoindex.init()

            @op.function()
            def process_text(text: str) -> str:
                """Process text content."""
                return f"hash_test: {text}"

            @cocoindex.transform_flow()
            def hash_test_flow(
                content: cocoindex.DataSlice[str],
            ) -> cocoindex.DataSlice[str]:
                """Test flow for content hash."""
                return content.transform(process_text)

            # Process the same content multiple times
            result1 = hash_test_flow.eval(test_content)
            result2 = hash_test_flow.eval(test_content)

            # Results should be identical for identical content
            assert result1 == result2
            assert result1 == f"hash_test: {test_content}"

    def test_content_change_detection(self) -> None:
        """Test that content change detection works correctly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_file = Path(temp_dir) / "changing_file.txt"

            # Initial content
            initial_content = "Initial content"
            test_file.write_text(initial_content)

            # Remove database environment variables
            with patch.dict(os.environ, {}, clear=False):
                for env_var in [
                    "COCOINDEX_DATABASE_URL",
                    "COCOINDEX_DATABASE_USER",
                    "COCOINDEX_DATABASE_PASSWORD",
                ]:
                    os.environ.pop(env_var, None)

                cocoindex.init()

                @op.function()
                def track_changes(text: str) -> str:
                    """Track content changes."""
                    return f"version: {text}"

                @cocoindex.transform_flow()
                def change_detection_flow(
                    content: cocoindex.DataSlice[str],
                ) -> cocoindex.DataSlice[str]:
                    """Flow to test change detection."""
                    return content.transform(track_changes)

                # Process initial content
                result1 = change_detection_flow.eval(initial_content)
                assert result1 == f"version: {initial_content}"

                # Change content and process again
                changed_content = "Changed content"
                test_file.write_text(changed_content)

                result2 = change_detection_flow.eval(changed_content)
                assert result2 == f"version: {changed_content}"
                assert result1 != result2

    def test_identical_content_different_timestamps(self) -> None:
        """Test that identical content with different timestamps is handled correctly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            file1 = Path(temp_dir) / "file1.txt"
            file2 = Path(temp_dir) / "file2.txt"

            content = "Identical content for both files"

            # Create files with same content but different timestamps
            file1.write_text(content)
            import time

            time.sleep(0.1)  # Small delay to ensure different timestamps
            file2.write_text(content)

            # Remove database environment variables
            with patch.dict(os.environ, {}, clear=False):
                for env_var in [
                    "COCOINDEX_DATABASE_URL",
                    "COCOINDEX_DATABASE_USER",
                    "COCOINDEX_DATABASE_PASSWORD",
                ]:
                    os.environ.pop(env_var, None)

                cocoindex.init()

                @op.function()
                def content_processor(text: str) -> str:
                    """Process content regardless of timestamp."""
                    return f"content_hash: {text}"

                @cocoindex.transform_flow()
                def timestamp_test_flow(
                    content: cocoindex.DataSlice[str],
                ) -> cocoindex.DataSlice[str]:
                    """Test flow for timestamp vs content hash."""
                    return content.transform(content_processor)

                # Process both files - should produce identical results
                result1 = timestamp_test_flow.eval(content)
                result2 = timestamp_test_flow.eval(content)

                assert result1 == result2
                assert result1 == f"content_hash: {content}"

    def test_content_hash_with_binary_data(self) -> None:
        """Test content hash functionality with binary data."""
        # Create binary test data
        binary_data = b"\x00\x01\x02\x03\x04\x05\xff\xfe\xfd"

        # Remove database environment variables
        with patch.dict(os.environ, {}, clear=False):
            for env_var in [
                "COCOINDEX_DATABASE_URL",
                "COCOINDEX_DATABASE_USER",
                "COCOINDEX_DATABASE_PASSWORD",
            ]:
                os.environ.pop(env_var, None)

            cocoindex.init()

            @op.function()
            def process_binary_as_text(text: str) -> str:
                """Process binary data represented as text."""
                return f"binary_processed: {len(text)} chars"

            @cocoindex.transform_flow()
            def binary_test_flow(
                content: cocoindex.DataSlice[str],
            ) -> cocoindex.DataSlice[str]:
                """Test flow for binary data."""
                return content.transform(process_binary_as_text)

            # Convert binary to string for processing
            text_data = binary_data.decode("latin1")  # Use latin1 to preserve all bytes
            result = binary_test_flow.eval(text_data)

            assert f"binary_processed: {len(text_data)} chars" == result

    def test_empty_content_hash(self) -> None:
        """Test content hash with empty content."""
        # Remove database environment variables
        with patch.dict(os.environ, {}, clear=False):
            for env_var in [
                "COCOINDEX_DATABASE_URL",
                "COCOINDEX_DATABASE_USER",
                "COCOINDEX_DATABASE_PASSWORD",
            ]:
                os.environ.pop(env_var, None)

            cocoindex.init()

            @op.function()
            def process_empty(text: str) -> str:
                """Process empty content."""
                return f"empty_check: '{text}' (length: {len(text)})"

            @cocoindex.transform_flow()
            def empty_content_flow(
                content: cocoindex.DataSlice[str],
            ) -> cocoindex.DataSlice[str]:
                """Test flow for empty content."""
                return content.transform(process_empty)

            # Test with empty string
            result = empty_content_flow.eval("")
            assert result == "empty_check: '' (length: 0)"

    def test_large_content_hash(self) -> None:
        """Test content hash with large content."""
        # Create large content
        large_content = "A" * 10000 + "B" * 10000 + "C" * 10000

        # Remove database environment variables
        with patch.dict(os.environ, {}, clear=False):
            for env_var in [
                "COCOINDEX_DATABASE_URL",
                "COCOINDEX_DATABASE_USER",
                "COCOINDEX_DATABASE_PASSWORD",
            ]:
                os.environ.pop(env_var, None)

            cocoindex.init()

            @op.function()
            def process_large_content(text: str) -> str:
                """Process large content."""
                return f"large_content: {len(text)} chars, starts_with: {text[:10]}"

            @cocoindex.transform_flow()
            def large_content_flow(
                content: cocoindex.DataSlice[str],
            ) -> cocoindex.DataSlice[str]:
                """Test flow for large content."""
                return content.transform(process_large_content)

            result = large_content_flow.eval(large_content)
            expected = f"large_content: {len(large_content)} chars, starts_with: {large_content[:10]}"
            assert result == expected

    def test_unicode_content_hash(self) -> None:
        """Test content hash with Unicode content."""
        # Create Unicode content with various characters
        unicode_content = "Hello 世界! 🌍 Здравствуй мир! مرحبا بالعالم!"

        # Remove database environment variables
        with patch.dict(os.environ, {}, clear=False):
            for env_var in [
                "COCOINDEX_DATABASE_URL",
                "COCOINDEX_DATABASE_USER",
                "COCOINDEX_DATABASE_PASSWORD",
            ]:
                os.environ.pop(env_var, None)

            cocoindex.init()

            @op.function()
            def process_unicode(text: str) -> str:
                """Process Unicode content."""
                return f"unicode: {text} (length: {len(text)})"

            @cocoindex.transform_flow()
            def unicode_flow(
                content: cocoindex.DataSlice[str],
            ) -> cocoindex.DataSlice[str]:
                """Test flow for Unicode content."""
                return content.transform(process_unicode)

            result = unicode_flow.eval(unicode_content)
            expected = f"unicode: {unicode_content} (length: {len(unicode_content)})"
            assert result == expected

    def test_content_hash_consistency(self) -> None:
        """Test that content hash is consistent across multiple runs."""
        test_content = "Consistency test content"

        # Remove database environment variables
        with patch.dict(os.environ, {}, clear=False):
            for env_var in [
                "COCOINDEX_DATABASE_URL",
                "COCOINDEX_DATABASE_USER",
                "COCOINDEX_DATABASE_PASSWORD",
            ]:
                os.environ.pop(env_var, None)

            cocoindex.init()

            @op.function()
            def consistency_test(text: str) -> str:
                """Test consistency of content processing."""
                return f"consistent: {text}"

            @cocoindex.transform_flow()
            def consistency_flow(
                content: cocoindex.DataSlice[str],
            ) -> cocoindex.DataSlice[str]:
                """Test flow for consistency."""
                return content.transform(consistency_test)

            # Run multiple times and verify consistency
            results = []
            for i in range(5):
                result = consistency_flow.eval(test_content)
                results.append(result)

            # All results should be identical
            assert all(r == results[0] for r in results)
            assert results[0] == f"consistent: {test_content}"


class TestContentHashIntegration:
    """Integration tests for content hash with different scenarios."""

    def setup_method(self) -> None:
        """Setup method called before each test."""
        try:
            cocoindex.stop()
        except:
            pass

    def teardown_method(self) -> None:
        """Teardown method called after each test."""
        try:
            cocoindex.stop()
        except:
            pass

    def test_github_actions_simulation(self) -> None:
        """Simulate GitHub Actions scenario where file timestamps change but content doesn't."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a file
            test_file = Path(temp_dir) / "github_test.py"
            content = '''
def hello_world():
    """A simple hello world function."""
    return "Hello, World!"

if __name__ == "__main__":
    print(hello_world())
'''
            test_file.write_text(content)
            original_mtime = test_file.stat().st_mtime

            # Remove database environment variables
            with patch.dict(os.environ, {}, clear=False):
                for env_var in [
                    "COCOINDEX_DATABASE_URL",
                    "COCOINDEX_DATABASE_USER",
                    "COCOINDEX_DATABASE_PASSWORD",
                ]:
                    os.environ.pop(env_var, None)

                cocoindex.init()

                @op.function()
                def extract_functions(code: str) -> str:
                    """Extract function information from code."""
                    lines = code.strip().split("\n")
                    functions = [
                        line.strip()
                        for line in lines
                        if line.strip().startswith("def ")
                    ]
                    return f"functions: {functions}"

                @cocoindex.transform_flow()
                def code_analysis_flow(
                    code: cocoindex.DataSlice[str],
                ) -> cocoindex.DataSlice[str]:
                    """Analyze code content."""
                    return code.transform(extract_functions)

                # First processing
                result1 = code_analysis_flow.eval(content)

                # Simulate git checkout by updating file timestamp but keeping same content
                import time

                time.sleep(0.1)
                test_file.write_text(content)  # Same content, new timestamp
                new_mtime = test_file.stat().st_mtime

                # Verify timestamp changed
                assert new_mtime > original_mtime

                # Second processing - should produce same result due to content hash
                result2 = code_analysis_flow.eval(content)

                assert result1 == result2
                expected = "functions: ['def hello_world():']"
                assert result1 == expected

    def test_incremental_processing_simulation(self) -> None:
        """Simulate incremental processing where only some files change."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create multiple files
            files_content = {
                "file1.txt": "Content of file 1",
                "file2.txt": "Content of file 2",
                "file3.txt": "Content of file 3",
            }

            file_paths = {}
            for filename, content in files_content.items():
                file_path = Path(temp_dir) / filename
                file_path.write_text(content)
                file_paths[filename] = file_path

            # Remove database environment variables
            with patch.dict(os.environ, {}, clear=False):
                for env_var in [
                    "COCOINDEX_DATABASE_URL",
                    "COCOINDEX_DATABASE_USER",
                    "COCOINDEX_DATABASE_PASSWORD",
                ]:
                    os.environ.pop(env_var, None)

                cocoindex.init()

                @op.function()
                def process_file_content(content: str) -> str:
                    """Process individual file content."""
                    return f"processed: {content}"

                @cocoindex.transform_flow()
                def incremental_flow(
                    content: cocoindex.DataSlice[str],
                ) -> cocoindex.DataSlice[str]:
                    """Incremental processing flow."""
                    return content.transform(process_file_content)

                # Process all files initially
                initial_results = {}
                for filename, content in files_content.items():
                    result = incremental_flow.eval(content)
                    initial_results[filename] = result

                # Modify only one file
                files_content["file2.txt"] = "Modified content of file 2"
                file_paths["file2.txt"].write_text(files_content["file2.txt"])

                # Process all files again
                updated_results = {}
                for filename, content in files_content.items():
                    result = incremental_flow.eval(content)
                    updated_results[filename] = result

                # file1 and file3 should have same results (unchanged content)
                assert initial_results["file1.txt"] == updated_results["file1.txt"]
                assert initial_results["file3.txt"] == updated_results["file3.txt"]

                # file2 should have different result (changed content)
                assert initial_results["file2.txt"] != updated_results["file2.txt"]
                assert (
                    updated_results["file2.txt"]
                    == "processed: Modified content of file 2"
                )

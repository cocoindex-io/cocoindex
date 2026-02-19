"""Automated CLI tests using subprocess.

These tests run CLI commands and verify outputs match expected behavior.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

from typing import Generator
import pytest

# Directory containing test modules
TEST_DIR = Path(__file__).resolve().parent

# Artifacts to clean up
CLEANUP_PATTERNS = [
    "cocoindex*.db",
    "db1",
    "db2",
    "db_alpha",
    "out_*",
    "cocoindex_unbound.db",
    "cli_init_*",
    "default_db_test.db",
]


def run_cli(
    *args: str, check: bool = True, input: str | None = None
) -> subprocess.CompletedProcess[str]:
    """Run a cocoindex CLI command and return the result."""
    cmd = ["cocoindex", *args]
    result = subprocess.run(
        cmd,
        cwd=TEST_DIR,
        capture_output=True,
        text=True,
        check=False,
        encoding="utf-8",
        input=input,
    )
    if check and result.returncode != 0:
        raise AssertionError(
            f"Command failed: {cmd}\n"
            f"returncode={result.returncode}\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}\n"
        )
    return result


def cleanup_artifacts() -> None:
    """Remove all test artifacts."""
    import glob

    for pattern in CLEANUP_PATTERNS:
        for path in glob.glob(str(TEST_DIR / pattern)):
            if os.path.isdir(path):
                shutil.rmtree(path, ignore_errors=True)
            elif os.path.isfile(path):
                os.remove(path)


@pytest.fixture(autouse=True)
def clean_before_and_after(
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[None, None, None]:
    """Clean up test artifacts and environment before and after each test."""
    cleanup_artifacts()
    for key in list(os.environ):
        if key.startswith("COCOINDEX_"):
            monkeypatch.delenv(key)
    yield
    cleanup_artifacts()


# =============================================================================
# Test 1: No Apps Defined (Edge Case)
# =============================================================================


class TestNoAppsDefined:
    """Tests error messages when a module has no apps."""

    def test_ls_no_apps(self) -> None:
        """cocoindex ls ./no_apps.py should show 'No apps are defined'."""
        result = run_cli("ls", "./no_apps.py")
        assert "No apps are defined" in result.stdout

    def test_update_no_apps(self) -> None:
        """cocoindex update ./no_apps.py should error."""
        result = run_cli("update", "./no_apps.py", check=False)
        assert result.returncode != 0
        assert "No apps found" in result.stderr


# =============================================================================
# Test 2: Single App (Auto-Select)
# =============================================================================


class TestSingleApp:
    """Tests that a single app is automatically selected."""

    def test_ls_shows_app_with_plus(self) -> None:
        """List should show SingleApp with [+] indicator before update."""
        result = run_cli("ls", "./single_app.py")
        assert "SingleApp" in result.stdout
        assert "[+]" in result.stdout

    def test_update_auto_selects(self) -> None:
        """Update without app name should auto-select the only app."""
        run_cli("update", "./single_app.py")

        # Verify output file was created
        out_file = TEST_DIR / "out_single" / "single.txt"
        assert out_file.exists()
        assert "Hello from SingleApp" in out_file.read_text()

    def test_ls_after_update_no_plus(self) -> None:
        """List after update should not show [+] indicator."""
        run_cli("update", "./single_app.py")

        result = run_cli("ls", "./single_app.py")
        assert "SingleApp" in result.stdout
        assert "[+]" not in result.stdout

    def test_drop_removes_app(self) -> None:
        """Drop should remove the app's target states."""
        run_cli("update", "./single_app.py")

        result = run_cli("drop", "./single_app.py", "-f")
        assert "Dropped app" in result.stdout

        # After drop, ls should show [+] again
        result = run_cli("ls", "./single_app.py")
        assert "[+]" in result.stdout


# =============================================================================
# Test 3: Multiple Apps (Requires Specifier)
# =============================================================================


class TestMultipleApps:
    """Tests that multiple apps require explicit :app_name specifier."""

    def test_ls_shows_both_apps(self) -> None:
        """List should show both apps."""
        result = run_cli("ls", "./multi_app.py")
        assert "MultiApp1" in result.stdout
        assert "MultiApp2" in result.stdout

    def test_update_without_specifier_errors(self) -> None:
        """Update without specifier should error with multiple apps."""
        result = run_cli("update", "./multi_app.py", check=False)
        assert result.returncode != 0
        assert "Multiple apps found" in result.stderr

    def test_update_with_specifier_works(self) -> None:
        """Update with explicit app name should work."""
        run_cli("update", "./multi_app.py:MultiApp1")

        # Verify output
        out_file = TEST_DIR / "out_multi_1" / "hello.txt"
        assert out_file.exists()

    def test_update_both_apps(self) -> None:
        """Can update both apps with explicit specifiers."""
        run_cli("update", "./multi_app.py:MultiApp1")
        run_cli("update", "./multi_app.py:MultiApp2")

        # Both output dirs should exist
        assert (TEST_DIR / "out_multi_1" / "hello.txt").exists()
        assert (TEST_DIR / "out_multi_2" / "world.txt").exists()

    def test_drop_one_app(self) -> None:
        """Drop one app, other should remain persisted."""
        run_cli("update", "./multi_app.py:MultiApp1")
        run_cli("update", "./multi_app.py:MultiApp2")

        # Drop only MultiApp1
        run_cli("drop", "./multi_app.py:MultiApp1", "-f")

        # List should show MultiApp1 with [+], MultiApp2 without
        result = run_cli("ls", "./multi_app.py")
        lines = result.stdout.split("\n")

        # Find lines with app names
        app1_line = next((l for l in lines if "MultiApp1" in l), "")
        app2_line = next((l for l in lines if "MultiApp2" in l), "")

        assert "[+]" in app1_line
        assert "[+]" not in app2_line


# =============================================================================
# Test 4: App NOT Bound to Module-Level Variable
# =============================================================================


class TestAppNotBound:
    """Tests that apps created via factory functions are discoverable."""

    def test_ls_finds_unbound_app(self) -> None:
        """List should find UnboundApp even via factory function."""
        result = run_cli("ls", "./app_not_bound.py")
        assert "UnboundApp" in result.stdout

    def test_update_works(self) -> None:
        """Update should work for factory-created app."""
        run_cli("update", "./app_not_bound.py")

        # Verify output
        out_file = TEST_DIR / "out_unbound" / "unbound.txt"
        assert out_file.exists()


# =============================================================================
# Test 5: Multiple Environments (Different Databases)
# =============================================================================


class TestMultipleEnvironments:
    """Tests apps in different environments are grouped correctly."""

    def test_ls_shows_two_groups(self) -> None:
        """List should show two groups with different db paths."""
        result = run_cli("ls", "./multi_env.py")
        assert "DB1App" in result.stdout
        assert "DB2App" in result.stdout
        # Should have two different db paths
        assert "db1" in result.stdout
        assert "db2" in result.stdout

    def test_update_both_environments(self) -> None:
        """Can update apps in different environments."""
        run_cli("update", "./multi_env.py:DB1App")
        run_cli("update", "./multi_env.py:DB2App")

        # Both output dirs should have files
        assert (TEST_DIR / "out_db1" / "db1.txt").exists()
        assert (TEST_DIR / "out_db2" / "db2.txt").exists()

    def test_drop_in_different_envs(self) -> None:
        """Can drop apps in different environments independently."""
        run_cli("update", "./multi_env.py:DB1App")
        run_cli("update", "./multi_env.py:DB2App")

        # Drop only DB1App
        run_cli("drop", "./multi_env.py:DB1App", "-f")

        # List should show DB1App with [+], DB2App without
        result = run_cli("ls", "./multi_env.py")
        lines = result.stdout.split("\n")

        db1_line = next((l for l in lines if "DB1App" in l), "")
        db2_line = next((l for l in lines if "DB2App" in l), "")

        assert "[+]" in db1_line
        assert "[+]" not in db2_line


# =============================================================================
# Test 6: Same App Name in Different Environments
# =============================================================================


class TestSameNameDifferentEnv:
    """Tests that same-named apps in different environments are tracked separately."""

    def test_ls_shows_both_myapp_with_env_names(self) -> None:
        """List should show MyApp in both environments with env names."""
        result = run_cli("ls", "./same_name_diff_env.py")

        # Should show MyApp twice (once per environment)
        assert result.stdout.count("MyApp") == 2

        # Should show both environment names
        assert "alpha" in result.stdout
        assert "default" in result.stdout

        # Should show alpha db path
        assert "db_alpha" in result.stdout

    def test_update_without_env_specifier_errors(self) -> None:
        """Update without env specifier should error when same name in multiple envs."""
        result = run_cli("update", "./same_name_diff_env.py:MyApp", check=False)
        assert result.returncode != 0
        assert "Multiple apps named 'MyApp'" in result.stderr
        assert "@env_name" in result.stderr

    def test_update_with_env_specifier_works(self) -> None:
        """Update with @env_name specifier should work."""
        # Update alpha env
        run_cli("update", "./same_name_diff_env.py:MyApp@alpha")

        # Verify only alpha output was created
        assert (TEST_DIR / "out_alpha" / "output.txt").exists()
        assert not (TEST_DIR / "out_default" / "output.txt").exists()

        # Update default env
        run_cli("update", "./same_name_diff_env.py:MyApp@default")

        # Now both should exist
        assert (TEST_DIR / "out_alpha" / "output.txt").exists()
        assert (TEST_DIR / "out_default" / "output.txt").exists()

    def test_drop_with_env_specifier(self) -> None:
        """Drop with @env_name specifier should only drop that env's app."""
        # Update both
        run_cli("update", "./same_name_diff_env.py:MyApp@alpha")
        run_cli("update", "./same_name_diff_env.py:MyApp@default")

        # Drop only alpha
        run_cli("drop", "./same_name_diff_env.py:MyApp@alpha", "-f")

        # List should show alpha with [+], default without
        result = run_cli("ls", "./same_name_diff_env.py")

        # Find the lines for each environment
        lines = result.stdout.split("\n")
        alpha_section = False
        default_section = False
        alpha_has_plus = False
        default_has_plus = False

        for line in lines:
            if "alpha" in line and "db_alpha" in line:
                alpha_section = True
                default_section = False
            elif "default" in line:
                alpha_section = False
                default_section = True
            elif "MyApp" in line:
                if alpha_section:
                    alpha_has_plus = "[+]" in line
                elif default_section:
                    default_has_plus = "[+]" in line

        assert alpha_has_plus, "Alpha MyApp should have [+]"
        assert not default_has_plus, "Default MyApp should not have [+]"

    def test_invalid_env_name_errors(self) -> None:
        """Update with non-existent env name should error."""
        result = run_cli(
            "update", "./same_name_diff_env.py:MyApp@nonexistent", check=False
        )
        assert result.returncode != 0
        assert "No environment named 'nonexistent'" in result.stderr


# =============================================================================
# Test 7: Invalid App Name (Error Handling)
# =============================================================================


class TestInvalidAppName:
    """Tests error handling for invalid app names."""

    def test_update_nonexistent_app(self) -> None:
        """Update with non-existent app name should error."""
        result = run_cli("update", "./single_app.py:NonExistent", check=False)
        assert result.returncode != 0
        assert "No app named 'NonExistent'" in result.stderr


# =============================================================================
# Test: List from Database with --db option
# =============================================================================


class TestListFromDatabase:
    """Tests listing apps directly from a database file."""

    def test_ls_db_shows_persisted_apps(self) -> None:
        """List with --db should show persisted apps from the database."""
        # First, run an app to persist it
        run_cli("update", "./app1.py")

        # List using --db option
        result = run_cli("ls", "--db", "./cocoindex.db")
        assert "TestApp1" in result.stdout

    def test_ls_db_nonexistent_errors(self) -> None:
        """List with --db on non-existent file should error."""
        result = run_cli("ls", "--db", "./nonexistent.db", check=False)
        assert result.returncode != 0
        assert "does not exist" in result.stderr

    def test_ls_without_args_errors(self) -> None:
        """List without arguments should show usage help."""
        result = run_cli("ls", check=False)
        assert result.returncode != 0
        assert "Please specify" in result.stderr


# =============================================================================
# Test: Drop without persisted state
# =============================================================================


class TestDropNoPersisted:
    """Tests drop behavior when app has no persisted state."""

    def test_drop_app_not_run(self) -> None:
        """Drop on app that was never run should indicate nothing to drop."""
        result = run_cli("drop", "./single_app.py", "-f")
        assert "no persisted state" in result.stdout.lower()


# =============================================================================
# Test: Init command
# =============================================================================


# =============================================================================
# Test: Default DB path from COCOINDEX_DB environment variable
# =============================================================================


class TestDefaultDbPath:
    """Tests for the default db path from COCOINDEX_DB environment variable."""

    def test_ls_uses_default_db_from_env(self) -> None:
        """cocoindex ls without args should use COCOINDEX_DB if set."""
        db_path = TEST_DIR / "default_db_test.db"

        # First, run an app to create the database with persisted state
        run_cli("update", "./app1.py")

        # Copy the db directory to our test db path (LMDB uses directory)
        shutil.copytree(TEST_DIR / "cocoindex.db", db_path)

        # Now run ls without args but with COCOINDEX_DB set
        env = os.environ.copy()
        env["COCOINDEX_DB"] = str(db_path)
        cmd = ["cocoindex", "ls"]
        result = subprocess.run(
            cmd,
            cwd=TEST_DIR,
            capture_output=True,
            text=True,
            check=False,
            encoding="utf-8",
            env=env,
        )
        assert result.returncode == 0, f"Failed: {result.stderr}"
        assert "TestApp1" in result.stdout

    def test_ls_without_args_errors_when_no_env_var(self) -> None:
        """cocoindex ls without args should error when COCOINDEX_DB is not set."""
        # Ensure COCOINDEX_DB is not set
        env = os.environ.copy()
        env.pop("COCOINDEX_DB", None)
        cmd = ["cocoindex", "ls"]
        result = subprocess.run(
            cmd,
            cwd=TEST_DIR,
            capture_output=True,
            text=True,
            check=False,
            encoding="utf-8",
            env=env,
        )
        assert result.returncode != 0
        assert "COCOINDEX_DB" in result.stderr

    def test_update_app_with_default_db_from_env(self) -> None:
        """cocoindex update should work when app uses COCOINDEX_DB for db_path."""
        db_path = TEST_DIR / "default_db_test.db"

        # Set COCOINDEX_DB and run update
        env = os.environ.copy()
        env["COCOINDEX_DB"] = str(db_path)
        cmd = ["cocoindex", "update", "./app_default_db.py"]
        result = subprocess.run(
            cmd,
            cwd=TEST_DIR,
            capture_output=True,
            text=True,
            check=False,
            encoding="utf-8",
            env=env,
        )
        assert result.returncode == 0, f"Failed: {result.stderr}"

        # Verify output file was created
        out_file = TEST_DIR / "out_default_db" / "default_db.txt"
        assert out_file.exists()
        assert "Hello from DefaultDbApp" in out_file.read_text()

        # Verify app is in the database using ls with --db
        result = run_cli("ls", "--db", str(db_path))
        assert "DefaultDbApp" in result.stdout


class TestInitCommand:
    """Tests for the cocoindex init command."""

    def test_init_creates_project_structure(self) -> None:
        """cocoindex init MyProject should create basic project files."""
        project_dir = TEST_DIR / "cli_init_project"

        # Sanity: ensure directory does not exist before running
        if project_dir.exists():
            shutil.rmtree(project_dir)

        run_cli("init", "cli_init_project")

        assert project_dir.exists()
        assert (project_dir / "main.py").exists()
        assert (project_dir / "pyproject.toml").exists()
        assert (project_dir / "README.md").exists()

        # pyproject.toml should use the project name
        pyproject_text = (project_dir / "pyproject.toml").read_text(encoding="utf-8")
        assert 'name = "cli_init_project"' in pyproject_text

        # Smoke test: verify generated files work
        # Run ls to verify the app is discoverable (use relative path from TEST_DIR)
        result = run_cli("ls", "cli_init_project/main.py")
        assert "cli_init_project" in result.stdout

        # Run update to verify the app can execute
        run_cli("update", "cli_init_project/main.py")

    def test_init_defaults_project_name_from_dir(self) -> None:
        """When PROJECT_NAME is omitted, name defaults to the target directory name."""
        project_dir = TEST_DIR / "cli_init_dir_only"

        if project_dir.exists():
            shutil.rmtree(project_dir)

        # PROJECT_NAME omitted, only --dir provided
        run_cli("init", "--dir", "cli_init_dir_only")

        assert project_dir.exists()
        pyproject_text = (project_dir / "pyproject.toml").read_text(encoding="utf-8")
        # Project name should match directory name
        assert 'name = "cli_init_dir_only"' in pyproject_text


class TestUpdateFlags:
    """Tests for update-related flags (reset, full-reprocess)."""

    def test_update_requires_confirmation_without_force(self) -> None:
        """Update --reset should prompt unless --force is provided."""
        # Say "no" to the reset confirmation prompt.
        result = run_cli(
            "update", "./single_app.py", "--reset", check=False, input="no\n"
        )
        assert result.returncode == 0
        assert "aborted" in (result.stdout + result.stderr).lower()

        out_file = TEST_DIR / "out_single" / "single.txt"
        assert not out_file.exists()

    def test_update_confirmation_yes_runs(self) -> None:
        """Update --reset prompt should accept 'yes' and proceed."""
        result = run_cli(
            "update", "./single_app.py", "--reset", check=False, input="yes\n"
        )
        assert result.returncode == 0

        out_file = TEST_DIR / "out_single" / "single.txt"
        assert out_file.exists()

    def test_full_reprocess_force_rewrite_unchanged(self) -> None:
        """Test that --full-reprocess forces rewrite even if targets are unchanged."""
        app_path = "./memo_app.py"
        stamp_path = TEST_DIR / "out_memo" / "stamp.txt"

        # First run: create the target
        run_cli("update", app_path)
        assert stamp_path.exists()
        first = stamp_path.read_text()

        # Second run: should skip write (unchanged)
        run_cli("update", app_path)
        second = stamp_path.read_text()
        assert second == first, "Second run should skip write when unchanged"

        # Third run with --full-reprocess: should force rewrite
        run_cli("update", app_path, "--full-reprocess")
        third = stamp_path.read_text()
        assert third != first, "--full-reprocess should force rewrite even if unchanged"

    def test_full_reprocess_deleted_target_not_resurrected(self) -> None:
        """Test that --full-reprocess doesn't keep deleted targets alive via memo reuse."""
        app_path = "./full_reprocess_app.py"
        target_a_path = TEST_DIR / "out_full_reprocess" / "target_a.txt"
        target_b_path = TEST_DIR / "out_full_reprocess" / "target_b.txt"

        # First run: create both targets A and B
        run_cli("update", app_path)
        assert target_a_path.exists(), "target_a.txt should exist after first run"
        assert target_b_path.exists(), "target_b.txt should exist after first run"

        # Modify the app to only create A (remove B)
        # We'll do this by creating a modified version of the app
        original_content = (TEST_DIR / app_path).read_text()
        modified_content = original_content.replace(
            "create_b: bool = True", "create_b: bool = False"
        )
        (TEST_DIR / app_path).write_text(modified_content)

        try:
            # Run with --full-reprocess: B should be deleted, not kept alive by old memos
            run_cli("update", app_path, "--full-reprocess")
            assert target_a_path.exists(), "target_a.txt should still exist"
            assert not target_b_path.exists(), (
                "target_b.txt should be deleted, not kept alive by old memos"
            )
        finally:
            # Restore original content
            (TEST_DIR / app_path).write_text(original_content)


class TestFullReprocess:
    """Tests for --full-reprocess flag behavior."""

    def test_full_reprocess_force_rewrite_unchanged(self) -> None:
        """Test that --full-reprocess forces rewrite even if targets are unchanged."""
        app_path = "./memo_app.py"
        stamp_path = TEST_DIR / "out_memo" / "stamp.txt"

        # First run: create the target
        run_cli("update", app_path)
        first = stamp_path.read_text()

        # Second run: should skip write (unchanged)
        run_cli("update", app_path)
        second = stamp_path.read_text()
        assert second == first, "Second run should skip write when unchanged"

        # Third run with --full-reprocess: should force rewrite
        run_cli("update", app_path, "--full-reprocess")
        third = stamp_path.read_text()
        assert third != first, "--full-reprocess should force rewrite even if unchanged"

    def test_full_reprocess_deleted_target_not_resurrected(self) -> None:
        """Test that --full-reprocess doesn't keep deleted targets alive via memo reuse."""
        app_path = "./full_reprocess_app.py"
        target_a_path = TEST_DIR / "out_full_reprocess" / "target_a.txt"
        target_b_path = TEST_DIR / "out_full_reprocess" / "target_b.txt"

        # First run: create both targets A and B
        run_cli("update", app_path)
        assert target_a_path.exists(), "target_a.txt should exist after first run"
        assert target_b_path.exists(), "target_b.txt should exist after first run"

        # Modify the app to only create A (remove B)
        # We'll do this by creating a modified version of the app
        original_content = (TEST_DIR / app_path).read_text()
        modified_content = original_content.replace(
            "create_b: bool = True", "create_b: bool = False"
        )
        (TEST_DIR / app_path).write_text(modified_content)

        try:
            # Run with --full-reprocess: B should be deleted, not kept alive by old memos
            run_cli("update", app_path, "--full-reprocess")
            assert target_a_path.exists(), "target_a.txt should still exist"
            assert not target_b_path.exists(), (
                "target_b.txt should be deleted, not kept alive by old memos"
            )
        finally:
            # Restore original content
            (TEST_DIR / app_path).write_text(original_content)


class TestDropQuiet:
    """Tests for drop --quiet behavior."""

    def test_drop_quiet_suppresses_informational_output(self) -> None:
        """drop --quiet should not print informational messages (only errors/prompts)."""
        run_cli("update", "./single_app.py")
        result = run_cli("drop", "./single_app.py", "-f", "--quiet")
        assert "Preparing to drop" not in result.stdout
        assert "Dropped app" not in result.stdout


# =============================================================================
# Test: Show command with --tree flag
# =============================================================================


class TestShowTree:
    """Tests for the show command with --tree flag."""

    def test_show_tree_displays_tree_structure(self) -> None:
        """show --tree should display stable paths as a tree."""
        # First, run an app to create stable paths
        run_cli("update", "./single_app.py")

        # Run show with --tree flag
        result = run_cli("show", "./single_app.py", "--tree")

        # Should contain tree structure elements
        assert "Found" in result.stdout
        assert "stable paths:" in result.stdout
        assert "/" in result.stdout
        # Should contain tree structure (accept both Unicode and ASCII)
        has_tree_chars = (
            "├──" in result.stdout
            or "└──" in result.stdout
            or "|--" in result.stdout
            or "\\--" in result.stdout
        )
        assert has_tree_chars, "Should contain tree structure characters"

    def test_show_tree_annotates_components(self) -> None:
        """show --tree should annotate component nodes with [component]."""
        # First, run an app to create stable paths
        run_cli("update", "./single_app.py")

        # Run show with --tree flag
        result = run_cli("show", "./single_app.py", "--tree")

        # Should contain component annotations
        assert "[component]" in result.stdout

    def test_show_tree_with_nested_structure(self) -> None:
        """show --tree should correctly display nested tree structures with proper annotations."""
        # First, run an app that creates a nested tree structure
        run_cli("update", "./tree_test_app.py")

        # Run show with --tree flag
        result = run_cli("show", "./tree_test_app.py", "--tree")

        # Should contain tree structure
        assert "Found" in result.stdout
        assert "stable paths:" in result.stdout
        assert "/" in result.stdout

        # Parse the output to verify structure
        lines = result.stdout.split("\n")
        output_text = result.stdout

        # Find the root line - should be annotated as component
        root_line = next(
            (
                l
                for l in lines
                if l.strip() == "/" or l.strip().startswith("/ [component]")
            ),
            None,
        )
        assert root_line is not None, "Root path should be present"
        assert "[component]" in root_line, "Root should be annotated as [component]"

        # Should have "files" node as an intermediate node (NOT a component)
        assert "files" in output_text, "Should have 'files' node in output"
        # Find the "files" line and verify it's NOT annotated as a component
        files_line = None
        tree_connectors = ("├──", "└──", "|--", "\\--")
        for line in lines:
            if (
                any(connector in line for connector in tree_connectors)
                and "files" in line
            ):
                parts = line.split()
                connector_idx = None
                for i, part in enumerate(parts):
                    if part in tree_connectors:
                        connector_idx = i
                        break
                if connector_idx is not None and connector_idx + 1 < len(parts):
                    node_name = parts[connector_idx + 1]
                    if node_name == "files":
                        files_line = line
                        break

        assert files_line is not None, "Should have 'files' intermediate node line"
        assert "[component]" not in files_line, (
            f"'files' should NOT be annotated as [component] (it's an intermediate node). "
            f"Line: {files_line}"
        )

        # Should have "file1.txt" and "file2.txt" as components under "files"
        assert "file1.txt" in output_text, "Should have 'file1.txt' node"
        assert "file2.txt" in output_text, "Should have 'file2.txt' node"
        # Both should be annotated as components
        file1_line = next((l for l in lines if "file1.txt" in l), None)
        file2_line = next((l for l in lines if "file2.txt" in l), None)
        assert file1_line is not None, "Should have 'file1.txt' line"
        assert file2_line is not None, "Should have 'file2.txt' line"
        assert "[component]" in file1_line, (
            "file1.txt should be annotated as [component]"
        )
        assert "[component]" in file2_line, (
            "file2.txt should be annotated as [component]"
        )

        # Should have "direct" as a component (direct child of root)
        assert "direct" in output_text, "Should have 'direct' node"
        direct_line = next((l for l in lines if "direct" in l), None)
        assert direct_line is not None, "Should have 'direct' line"
        assert "[component]" in direct_line, "direct should be annotated as [component]"

        # Should have "setup" as a component
        assert "setup" in output_text, "Should have 'setup' node"
        setup_line = next((l for l in lines if "setup" in l), None)
        assert setup_line is not None, "Should have 'setup' line"
        assert "[component]" in setup_line, "setup should be annotated as [component]"

        # Verify tree structure: file1.txt and file2.txt should be nested under files
        # Find the line index for "files" and "file1.txt"
        files_idx = next(
            (
                i
                for i, l in enumerate(lines)
                if "files" in l
                and any(connector in l for connector in tree_connectors)
                and "[component]" not in l
            ),
            None,
        )
        file1_idx = next(
            (
                i
                for i, l in enumerate(lines)
                if "file1.txt" in l
                and any(connector in l for connector in tree_connectors)
            ),
            None,
        )

        assert file1_idx is not None, "Should find 'file1.txt' line"
        # file1.txt should come after files (they're nested)
        assert file1_idx is not None and files_idx is not None
        assert file1_idx > files_idx, (
            "file1.txt should appear after files in nested structure"
        )
        # file1.txt line should have indentation indicating it's a child of files
        # Accept both Unicode (│   ) and ASCII (|   ) vertical lines, or spaces
        has_indentation = (
            "│   " in lines[file1_idx]
            or "|   " in lines[file1_idx]
            or "    " in lines[file1_idx]
        )
        assert has_indentation, f"file1.txt should be indented as child of files"

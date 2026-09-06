import pytest

from cocoindex.cli import AppSpecifier, _parse_app_target


@pytest.mark.parametrize(
    ("target", "expected"),
    [
        (r"C:\project\main.py", AppSpecifier(r"C:\project\main.py")),
        (
            r"C:\project\main.py:app2",
            AppSpecifier(r"C:\project\main.py", "app2"),
        ),
        (
            r"C:\project\main.py:app2@alpha",
            AppSpecifier(r"C:\project\main.py", "app2", "alpha"),
        ),
    ],
)
def test_parse_app_target_preserves_windows_drive(
    target: str, expected: AppSpecifier
) -> None:
    assert _parse_app_target(target) == expected

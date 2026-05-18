"""Tests for `Settings` / `LmdbSettings` API, including backward compatibility
for the legacy `lmdb_max_dbs` / `lmdb_map_size` keyword arguments and attribute
access that pre-date the `db_settings` encapsulation."""

import pathlib

import pytest

import cocoindex as coco
from cocoindex._internal.setting import LmdbSettings, Settings


def test_default_settings_uses_default_lmdb_settings() -> None:
    settings = Settings()
    assert settings.db_settings == LmdbSettings()
    assert settings.db_settings.max_dbs == 1024
    assert settings.db_settings.map_size == 0x1_0000_0000


def test_new_form_with_db_settings() -> None:
    settings = Settings(
        db_settings=LmdbSettings(max_dbs=2048, map_size=8 * 1024 * 1024 * 1024)
    )
    assert settings.db_settings.max_dbs == 2048
    assert settings.db_settings.map_size == 8 * 1024 * 1024 * 1024


def test_legacy_kwargs_still_construct_settings() -> None:
    settings = Settings(lmdb_max_dbs=2048, lmdb_map_size=8 * 1024 * 1024 * 1024)
    assert settings.db_settings.max_dbs == 2048
    assert settings.db_settings.map_size == 8 * 1024 * 1024 * 1024


def test_legacy_kwargs_partial_keeps_other_default() -> None:
    settings = Settings(lmdb_max_dbs=2048)
    assert settings.db_settings.max_dbs == 2048
    assert settings.db_settings.map_size == 0x1_0000_0000  # default


def test_legacy_attribute_read_proxies_to_db_settings() -> None:
    settings = Settings(db_settings=LmdbSettings(max_dbs=2048, map_size=4096))
    assert settings.lmdb_max_dbs == 2048
    assert settings.lmdb_map_size == 4096


def test_legacy_attribute_write_proxies_to_db_settings() -> None:
    settings = Settings()
    settings.lmdb_max_dbs = 4096
    settings.lmdb_map_size = 2 * 1024 * 1024 * 1024
    assert settings.db_settings.max_dbs == 4096
    assert settings.db_settings.map_size == 2 * 1024 * 1024 * 1024


def test_conflict_between_db_settings_and_legacy_kwargs_raises() -> None:
    with pytest.raises(ValueError, match="not both"):
        Settings(
            db_settings=LmdbSettings(max_dbs=2048),
            lmdb_max_dbs=4096,
        )
    with pytest.raises(ValueError, match="not both"):
        Settings(
            db_settings=LmdbSettings(),
            lmdb_map_size=4096,
        )


def test_engine_wire_format_is_flat() -> None:
    """The Rust engine expects flat `lmdb_max_dbs` / `lmdb_map_size` keys; the
    Python encapsulation must not change that wire format."""
    db_path = pathlib.Path("/tmp/cocoindex_test")
    settings = Settings(
        db_path=db_path,
        db_settings=LmdbSettings(max_dbs=2048, map_size=4096),
    )
    wire = settings._to_engine_dict()
    assert wire["lmdb_max_dbs"] == 2048
    assert wire["lmdb_map_size"] == 4096
    # Use `str(pathlib.Path(...))` rather than a literal so the comparison works
    # on Windows, where `str(Path("/tmp/x"))` is `"\\tmp\\x"`.
    assert wire["db_path"] == str(db_path)
    assert "db_settings" not in wire
    assert "global_execution_options" not in wire


def test_legacy_global_execution_options_kwarg_accepted() -> None:
    """v0 leftover: passing `global_execution_options=None` must remain valid for
    backward compatibility, and the attribute must read back as `None`."""
    settings = Settings(global_execution_options=None)
    assert settings.global_execution_options is None


def test_from_env_reads_lmdb_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COCOINDEX_LMDB_MAX_DBS", "512")
    monkeypatch.setenv("COCOINDEX_LMDB_MAP_SIZE", str(2 * 1024 * 1024 * 1024))
    settings = Settings.from_env(db_path=pathlib.Path("/tmp/cocoindex_test"))
    assert settings.db_settings.max_dbs == 512
    assert settings.db_settings.map_size == 2 * 1024 * 1024 * 1024


def test_from_env_falls_back_to_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("COCOINDEX_LMDB_MAX_DBS", raising=False)
    monkeypatch.delenv("COCOINDEX_LMDB_MAP_SIZE", raising=False)
    settings = Settings.from_env(db_path=pathlib.Path("/tmp/cocoindex_test"))
    assert settings.db_settings.max_dbs == 1024
    assert settings.db_settings.map_size == 0x1_0000_0000


def test_lmdb_settings_re_exported_from_top_level() -> None:
    assert coco.LmdbSettings is LmdbSettings

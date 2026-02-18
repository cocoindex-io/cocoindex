default:
    @just --list

rebuild:
    uvx maturin develop -E all,dev

format:
    # TODO: apply this on a chore PR
    # uv run ruff check --fix --select I
    uv run ruff format

check:
    # TODO: apply this on a chore PR
    # uv run ruff check python/
    uv run mypy python/

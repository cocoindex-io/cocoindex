#!/bin/bash -e

maturin develop
mypy

cargo test
pytest

cargo fmt
ruff format

echo "All checks passed"
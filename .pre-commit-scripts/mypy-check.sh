#!/bin/sh
# Run mypy checks on python and examples directories

set -e

mypy python
mypy --namespace-packages --explicit-package-bases --ignore-missing-imports examples

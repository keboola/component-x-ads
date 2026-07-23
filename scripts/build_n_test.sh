#!/bin/sh
# Sync note: tracks cookiecutter-python-component — update when template changes
# Do not remove set -e. ruff check runs before pytest intentionally — lint failures block tests.
set -e

ruff check
python -m pytest tests/ --tb=short -q

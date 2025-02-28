#!/usr/bin/env bash

set -ex

uv run ruff check . --fix --extend-select I
uv run ruff format .
uv run mypy src/

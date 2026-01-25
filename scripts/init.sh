#!/bin/bash

set -euo pipefail
trap "exit 1" ERR

# Sync dependencies and install dev dependencies
echo "Installing dependencies with uv..."
uv sync --dev

# Install pre-commit hooks
pre-commit install
pre-commit autoupdate

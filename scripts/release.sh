#!/bin/bash
set -e

# Run from repo root (so build/ and dist/ are at top level)
cd "$(dirname "$0")/.."

# Remove any prior builds
rm -rf build/* dist/* 2>/dev/null || true
mkdir -p build dist

# Build (uv is used elsewhere in this project; python3 -m build also works)
uv build

# Sanity-check before upload
twine check dist/*

# Upload; uses API token from ~/.pypirc
twine upload dist/*

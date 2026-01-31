#!/bin/bash

target=${*:-tests.simple tests.proto tests.errorformat}
timeout_sec=100

# Run full suite in multiprocessing mode, then in single-process mode
timeout $timeout_sec env PARPROC_TEST_MODE=mp uv run python -m unittest $target || exit 1
timeout $timeout_sec env PARPROC_TEST_MODE=single uv run python -m unittest $target || exit 1

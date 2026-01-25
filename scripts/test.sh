#!/bin/bash

target=${*:-tests.simple tests.proto tests.errorformat}
uv run python -m unittest $target

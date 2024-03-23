#!/bin/bash

target=${*:-tests.simple tests.proto tests.errorformat}
python3 -m unittest $target

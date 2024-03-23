#!/bin/bash

#Remove any prior builds
rm -rf ./build/*
rm -rf ./dist/*

python3 -m build

#Pulls api token from ~/.pypirc
twine upload dist/*

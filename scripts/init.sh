#!/bin/bash

set -euo pipefail
trap "exit 1" ERR


PYTHON_VERSION=3.12.1
pipEnvName=parproc
pipEnvPath=~/.pyenv/versions/${pipEnvName}

if [ ! -d $pipEnvPath ] || ! pip -V; then
		#Just in case
		pyenv update
		pyenv install -s $PYTHON_VERSION

		#Create new environment
		echo "Creating new virtualenv $pipEnvName for `whoami`"
		rm -rf $pipEnvPath
		pyenv virtualenv $PYTHON_VERSION $pipEnvName
		pyenv local $pipEnvName
fi

pip install -r requirements.txt -U

pre-commit install
pre-commit autoupdate

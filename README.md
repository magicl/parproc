![Python package workflow status](https://github.com/magicl/parproc/actions/workflows/python-package.yml/badge.svg)

# PARPROC

A library for parallelizing scripts, by allowing different script elements to be grouped in
processes, and allowing each group to depend on the completion of one or more other groups


# Installation and Usage

## Installation

```sh
pip install parproc
```

## Usage

See ```examples/success.py``` for an example. Different jobs can be defined and dependencies between them established, e.g.:

```python
import parproc as pp

@pp.Proc(now=True)
def func0(context):
    time.sleep(1)


@pp.Proc(now=True)
def func1(context):
    time.sleep(3)


@pp.Proc(now=True)
def func2(context, deps=['func0', 'func1']):
    time.sleep(2)
```

The ```now=True``` argument kicks off the jobs as soon as they are defined, which is optional. The example then waits for all jobs to finish

```python
pp.wait_for_all()
```


# Contributing

Feel free to send me PRs

## Setting up the development environment

1. Install [uv](https://github.com/astral-sh/uv) if you haven't already
2. Clone the repository
3. Initialize the development environment:
   ```sh
   uv sync --dev
   ```
   This will install all dependencies and development tools.

## Running tests

Run all tests using the test script:
```sh
./scripts/test.sh
```

Or run specific test modules:
```sh
./scripts/test.sh tests.simple tests.proto
```

You can also run tests directly with uv:
```sh
uv run python -m unittest tests.simple tests.proto tests.errorformat
```

## Running examples

Run any of the example scripts:
```sh
uv run python examples/success.py
uv run python examples/error.py
uv run python examples/failed_deps.py
```


# Change Log

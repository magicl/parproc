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

## Argument choices and glob expansion

Protos can define allowed values for arguments via `arg_choices`. When set, only those values are accepted when creating procs, and you can use glob patterns (`*`, `?`) in filled-out names to create multiple procs at once:

```python
@pp.Proto(name='build::[env]', arg_choices={'env': ['dev', 'prod']})
def build(context: pp.ProcContext, env: str) -> str:
    return f'built_{env}'

# Create one proc
pp.create('build::dev')

# Create all matching procs: build::dev and build::prod
names = pp.create('build::*')
pp.start(*names)
pp.wait(*names)
```

Using `*` or `?` in an argument when the proto has no `arg_choices` for that argument raises an error.


# Concepts

## Proc and Proto

parproc has two main building blocks: **Proc** and **Proto**.

A **Proc** is a single named unit of work. Decorating a function with `@pp.Proc` registers it immediately as a concrete process that can be scheduled:

```python
@pp.Proc(name='setup')
def setup(context: pp.ProcContext) -> str:
    return 'done'
```

A **Proto** (short for prototype) is a template for creating procs. Protos can be parameterized with `[placeholder]` patterns in their names. Each unique combination of parameters produces a distinct proc:

```python
@pp.Proto(name='build::[env]')
def build(context: pp.ProcContext, env: str) -> str:
    return f'built_{env}'
```

The separator between placeholders defaults to `::` and can be changed via `pp.set_options(name_param_separator='...')`.

### Creating procs from protos

Protos don't run directly -- you create concrete procs from them with `pp.create`:

```python
names = pp.create('build::prod')   # creates one proc named "build::prod"
pp.start(*names)
```

Parameters are automatically extracted from the filled-out name and passed to the function. Type casting is performed based on the function's type annotations.

### Proc vs Proto at a glance

| | Proc | Proto |
|---|---|---|
| What it is | A concrete process | A template for processes |
| Parameterized | No | Yes, via `[placeholder]` patterns |
| Created when | At decoration time | Procs are created later via `pp.create` |
| Typical use | One-off tasks | Repeated tasks with different parameters |

## Lifecycle and state machine

Every proc goes through a state machine:

```
IDLE  -->  WANTED  -->  RUNNING  -->  SUCCEEDED
                                 \->  FAILED
                                 \->  SKIPPED
```

- **IDLE** -- defined but not yet scheduled. Will not run until something triggers it.
- **WANTED** -- scheduled to run. Waiting for dependencies, locks, or wave ordering.
- **RUNNING** -- currently executing in a subprocess (or in-process in single mode).
- **SUCCEEDED** -- completed successfully.
- **FAILED** -- completed with an error (exception, timeout, dep failure, etc.).
- **SKIPPED** -- the proc raised `ProcSkippedError`; counts as success for downstream deps.

## Scheduling and execution

There are several ways to move a proc from IDLE to WANTED:

```python
# 1. now=True: auto-start at definition time
@pp.Proc(name='early', now=True)
def early(context): ...

# 2. Explicit start
pp.create('build::prod')
pp.start('build::prod')

# 3. Shorthand: create + start in one call
pp.run('build::prod')

# 4. Pulled in as a dependency of another proc (see below)
```

Once a proc is WANTED, the scheduler checks (in order):

1. **Wave ordering** -- lower-wave procs run first.
2. **Locks** -- named locks must be available.
3. **Dependencies** -- all deps must be in a succeeded state.
4. **Special dependencies** -- global conditions like `pp.NO_FAILURES`.
5. **Parallel limit** -- the number of concurrently running procs must not exceed the limit.

When all conditions are met, the proc begins executing.

### Waiting for completion

```python
pp.wait_for_all()            # wait for everything
pp.wait('build::prod')       # wait for specific procs
results = pp.results()       # dict of proc_name -> return value
```

## Dependencies (deps)

Dependencies define what must complete **before** a proc can run. They are specified with the `deps` parameter:

```python
@pp.Proc(name='deploy', deps=['build', 'test'])
def deploy(context):
    # runs only after both 'build' and 'test' have succeeded
    ...
```

When a proc becomes WANTED, its dependencies are recursively scheduled too (transitioned from IDLE to WANTED if needed).

### Dependency types

Dependencies can take several forms:

```python
@pp.Proto(name='deploy::[env]', deps=[
    'build::[env]',                              # pattern -- [env] is filled from parent
    'lint::all',                                  # filled-out name -- created from proto
    lambda ctx, env: f'provision::{env}',         # lambda -- called at creation time
    pp.NO_FAILURES,                               # special dep -- global condition
])
def deploy(context, env): ...
```

- **String patterns** (`'build::[env]'`): placeholders are filled with matching args from the parent proc.
- **Filled-out names** (`'lint::all'`): matched against protos and auto-created if a proto matches.
- **Lambdas**: called with a `DepProcContext` (or just the matching keyword args) and must return a proc name or list of names.
- **Special deps** (`pp.NO_FAILURES`): global conditions that must hold. `NO_FAILURES` is satisfied only when no proc in the run has failed.

## Reverse dependencies (rdeps)

Reverse dependencies flip the direction: instead of "I depend on X", an rdep says "I should be injected as a dependency of X". The declaring proc runs **before** the target.

```python
@pp.Proc(name='setup', rdeps=['deploy::[env]'])
def setup(context):
    return 'initialized'

@pp.Proto(name='deploy::[env]')
def deploy(context, env):
    # 'setup' is automatically injected as a dep of deploy
    ...
```

When `deploy::prod` is scheduled, the framework scans all protos and procs for rdeps matching `deploy::prod`. It finds `setup`'s rdep pattern matches, so `setup` is injected as a dependency of `deploy::prod`. The result: `setup` runs first.

Rdeps support the same `[placeholder]` pattern matching as proto names. Placeholders in the rdep pattern are matched against the target proc name.

### Conditional reverse dependencies (WhenScheduled)

A `WhenScheduled` rdep is a conditional variant: it only activates when the declaring proc is itself scheduled. It also pulls in (schedules) the target.

```python
@pp.Proto(name='migrate', rdeps=[pp.WhenScheduled('deploy::[env]')])
def migrate(context, env):
    return f'migrated_{env}'

@pp.Proto(name='deploy::[env]')
def deploy(context, env):
    return f'deployed_{env}'
```

The behavior:

- **Only `deploy::prod` is scheduled** -- `migrate` is not involved. `deploy::prod` runs alone.
- **`migrate::prod` is scheduled** (explicitly or as a dependency) -- `migrate::prod` is injected as a dep of `deploy::prod`, and `deploy::prod` is also scheduled. `migrate` runs first, then `deploy`.
- **Both are scheduled** -- same as above: `migrate` before `deploy`.

If the target has already started running or completed before the declaring proc is scheduled, a `UserError` is raised -- the ordering contract cannot be satisfied.

### rdeps vs WhenScheduled at a glance

| | rdeps (plain string) | WhenScheduled |
|---|---|---|
| Trigger | Target is scheduled | Declaring proc is scheduled |
| Direction | Target pulls in declarer | Declarer pushes into target |
| Target scheduling | Target must be independently scheduled | Target is auto-scheduled |
| Execution order | Declarer before target | Declarer before target |

### Combining deps and rdeps

Deps and rdeps can be mixed freely. A proc can have both regular deps and be injected via rdeps from other procs. Plain string rdeps and `WhenScheduled` entries can coexist in the same `rdeps` list:

```python
@pp.Proc(name='init', rdeps=['app::[x]', pp.WhenScheduled('cleanup')])
def init(context): ...
```

## Waves

Waves provide coarse-grained ordering across groups of procs. A proc with a lower wave number runs before procs with higher wave numbers, regardless of explicit dependencies:

```python
@pp.Proc(name='A', wave=0, now=True)
def a(context): ...

@pp.Proc(name='B', wave=1, now=True)
def b(context): ...

@pp.Proc(name='C', wave=2, now=True)
def c(context): ...
```

All wave-0 procs complete before any wave-1 proc starts, and so on. Within the same wave, procs run in parallel (subject to dependency and lock constraints). A dependency from a lower wave to a higher wave is an error (would cause deadlock).

## Locks

Locks provide mutual exclusion. Only one proc holding a given lock name can run at a time:

```python
@pp.Proto(name='deploy::[env]', locks=['deploy-lock'])
def deploy(context, env): ...
```

If `deploy::dev` and `deploy::prod` are both WANTED, only one runs at a time because they share the `deploy-lock`.

## ProcContext

Every proc function receives a `ProcContext` as its first argument. It provides:

- `context.results` -- dict of completed proc results (name -> return value).
- `context.params` -- global parameters set via `pp.set_params(...)`.
- `context.args` -- arguments specific to this proc (from the proto pattern).
- `context.proc_name` -- the name of the current proc.
- `context.create(...)` / `context.start(...)` / `context.run(...)` / `context.wait(...)` -- create, start, run, or wait for other procs from within a running proc.

## Options

Global options are configured via `pp.set_options(...)`:

```python
pp.set_options(
    parallel=4,                   # max concurrent procs (default: 100)
    mode='mp',                    # 'mp' (multiprocess) or 'single' (in-process)
    dynamic=True,                 # live terminal output (default: True if TTY)
    name_param_separator='::',    # separator between proto pattern segments
    allow_missing_deps=True,      # don't error on deps that don't exist yet
)
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

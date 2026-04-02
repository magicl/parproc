# Changelog

All notable user-facing changes to parproc will be documented in this file.


## [0.6.3]

### Added
- New `global_inputs_ignore` option on `set_options(...)` to define ignore specs (paths/globs/callables) once and apply them to all proc/proto input resolution.

### Changed
- Input resolution now combines per-proc `inputs_ignore` with global `global_inputs_ignore` for both incremental fingerprinting and watch-path derivation.
- Type stubs in `parproc/__init__.pyi` were refined as the consumer-facing public typing interface, with explicit exports for type checkers.

### Internal
- Test mypy overrides were updated so internal test assertions against private runtime attributes do not force those internals into the public stub surface.


## [0.6.2]

### Added
- Extensible log-ignore policy rules for `Proc`/`Proto` via `log_ignore`, including `IgnoreLogAlways` and `IgnoreLogIfSucceeded`.
- PyPI distributions now include typing support files (`parproc/__init__.pyi` and `parproc/py.typed`) so installed `parproc` is recognized by `mypy`.

### Changed
- `ProcFailedError("message")` now surfaces the message as a clean failure line (`more_info`) while still preserving full logs.
- The same `ProcFailedError` message is now also written to the proc log file, so full-log mode and log links include it.


## [0.6.1]

### Added
- Option to show full log output for failed tasks (`full_log_on_failure`) instead of extracted snippets.


## [0.6.0]

### Added
- Python 3.14 support.

### Changed
- Watch mode now debounces file-change bursts before triggering rebuilds.

### Fixed
- Watcher no longer retriggers on noisy directory `modified` events.


## [0.5.1]

### Fixed
- Incremental behavior now correctly treats tasks with no dependencies, avoiding incorrect up-to-date detection on subsequent runs.


## [0.5.0]

### Added
- Watch mode support with file monitoring and examples.
- Ignore-path support for watcher inputs.

### Fixed
- Error formatting behavior when a task is not in a failed state.
- Robustness for large task output handling in terminal rendering.


## [0.4.3]

### Added
- New conditional reverse-dependency rule type (`WhenTargetScheduled`).


## [0.4.2]

### Fixed
- Error extraction/visibility is now more reliable, including cases where a proc later succeeds.


## [0.4.1]

### Added
- Conditional reverse dependencies (`WhenScheduled`) for scheduling-dependent injection behavior.


## [0.4.0]

### Added
- Reverse dependencies (`rdeps`) and improved proto/dependency matching.
- Dependency lambdas now receive `DepProcContext`; proto dependencies can be generated dynamically.
- Wave-based scheduling and better progress display with rich live task output.
- `wait_for_all()` now returns `bool`; `start()`/`run()` usability for both protos and procs improved.
- Glob matching support and lambda-based `arg_choices`.

### Changed
- Name/param separator default changed to `::`.
- `SKIPPED` became a first-class success state for dependency flow.

### Fixed
- Log capture and error-snippet extraction robustness.
- Dependency-failure state handling and related scheduler/terminal behavior.


## [0.3.2]

### Internal
- Packaging/tooling baseline for the modern development workflow (including uv-based setup).

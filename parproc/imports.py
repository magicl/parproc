"""Static resolution of first-party Python import dependencies.

Used by incremental builds to follow ``import`` statements: when a proc declares a
``.py`` file as an input, parproc can also treat the first-party modules that file
imports (transitively) as inputs, so editing an imported helper marks the proc stale.

The resolver is intentionally lightweight and dependency-free: it parses each file
with the standard-library :mod:`ast` module and resolves imported module names to
files on disk, keeping only files located under the configured project roots
(everything in the standard library / site-packages / outside the roots is ignored).

Limitations: only statically-visible imports are followed. Dynamic imports
(``importlib.import_module``, ``__import__``) are not detected.
"""

import ast
import logging
import os
import sys

logger = logging.getLogger('par')

# Directory names that always indicate third-party/installed code, never first-party sources.
_VENDORED_DIR_NAMES = frozenset({'site-packages', 'dist-packages'})


class PythonImportResolver:
    """Resolve the transitive set of first-party Python files imported by a file.

    Results are cached per file keyed by modification time, so repeated staleness
    checks within a run only re-parse files that actually changed.
    """

    def __init__(self, roots: list[str]) -> None:
        self.roots: list[str] = [os.path.abspath(r) for r in roots]
        # abspath -> (mtime_ns, [direct first-party import abspaths])
        self._direct_cache: dict[str, tuple[int, list[str]]] = {}
        # Installation/venv prefixes whose contents are never treated as first-party.
        self._excluded_prefixes: list[str] = []
        for prefix in (sys.prefix, sys.base_prefix, sys.exec_prefix):
            if prefix:
                abs_prefix = os.path.abspath(prefix)
                if abs_prefix not in self._excluded_prefixes:
                    self._excluded_prefixes.append(abs_prefix)

    def transitive_deps(self, path: str) -> list[str]:
        """Return sorted first-party ``.py`` files transitively imported by ``path``.

        The input file itself is never included in the result.
        """
        start = os.path.abspath(path)
        visited: set[str] = {start}
        result: list[str] = []
        stack: list[str] = [start]
        while stack:
            current = stack.pop()
            for dep in self._direct_deps(current):
                if dep not in visited:
                    visited.add(dep)
                    result.append(dep)
                    stack.append(dep)
        return sorted(result)

    def _direct_deps(self, file_path: str) -> list[str]:
        """First-party files directly imported by ``file_path`` (mtime-cached)."""
        try:
            mtime = os.stat(file_path).st_mtime_ns
        except OSError:
            return []
        cached = self._direct_cache.get(file_path)
        if cached is not None and cached[0] == mtime:
            return cached[1]
        deps = self._compute_direct_deps(file_path)
        self._direct_cache[file_path] = (mtime, deps)
        return deps

    def _compute_direct_deps(self, file_path: str) -> list[str]:
        try:
            with open(file_path, encoding='utf-8') as f:
                source = f.read()
            tree = ast.parse(source, filename=file_path)
        except (OSError, SyntaxError, ValueError) as e:
            # Be conservative: an unreadable/unparseable file simply yields no deps.
            logger.debug(f'PythonImportResolver: could not parse {file_path!r}: {e}')
            return []

        base_dirs = self._base_dirs(file_path)
        package_base = self._package_base(file_path)
        module_parts = self._module_parts(file_path, package_base)
        current_package = module_parts[:-1] if not self._is_package_init(file_path) else module_parts

        found: list[str] = []
        seen: set[str] = set()

        def add(resolved: str | None) -> None:
            if resolved is None:
                return
            if resolved == os.path.abspath(file_path):
                return
            if resolved in seen:
                return
            seen.add(resolved)
            found.append(resolved)

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    for dotted in self._module_prefixes(alias.name):
                        add(self._module_to_file(dotted, base_dirs))
            elif isinstance(node, ast.ImportFrom):
                base_parts = self._resolve_from_base(node, current_package)
                if base_parts is None:
                    continue
                if base_parts:
                    for dotted in self._module_prefixes('.'.join(base_parts)):
                        add(self._module_to_file(dotted, base_dirs))
                for alias in node.names:
                    if alias.name == '*':
                        continue
                    submodule = '.'.join([*base_parts, alias.name])
                    add(self._module_to_file(submodule, base_dirs))
        return sorted(found)

    @staticmethod
    def _module_prefixes(dotted: str) -> list[str]:
        """For ``a.b.c`` return ``[a, a.b, a.b.c]`` (parents are executed on import)."""
        parts = [p for p in dotted.split('.') if p]
        return ['.'.join(parts[: i + 1]) for i in range(len(parts))]

    def _resolve_from_base(self, node: ast.ImportFrom, current_package: list[str]) -> list[str] | None:
        """Compute the dotted base package of a ``from ... import`` as a list of parts."""
        if node.level == 0:
            return node.module.split('.') if node.module else []
        # Relative import: level 1 == current package, level 2 == parent, etc.
        ups = node.level - 1
        if ups > len(current_package):
            return None
        anchor = current_package[: len(current_package) - ups]
        if node.module:
            anchor = [*anchor, *node.module.split('.')]
        return anchor

    def _base_dirs(self, file_path: str) -> list[str]:
        """Directories to search for absolute imports, most specific first."""
        candidates = [self._package_base(file_path), os.path.dirname(os.path.abspath(file_path)), *self.roots]
        ordered: list[str] = []
        for d in candidates:
            ad = os.path.abspath(d)
            if ad not in ordered:
                ordered.append(ad)
        return ordered

    @staticmethod
    def _package_base(file_path: str) -> str:
        """Topmost directory from which the file's package is importable."""
        d = os.path.dirname(os.path.abspath(file_path))
        while os.path.isfile(os.path.join(d, '__init__.py')):
            parent = os.path.dirname(d)
            if parent == d:
                break
            d = parent
        return d

    @staticmethod
    def _is_package_init(file_path: str) -> bool:
        return os.path.basename(file_path) == '__init__.py'

    def _module_parts(self, file_path: str, package_base: str) -> list[str]:
        rel = os.path.relpath(os.path.abspath(file_path), package_base)
        rel_no_ext = os.path.splitext(rel)[0]
        parts = [p for p in rel_no_ext.split(os.sep) if p and p != '.']
        if parts and parts[-1] == '__init__':
            parts = parts[:-1]
        return parts

    def _module_to_file(self, module: str, base_dirs: list[str]) -> str | None:
        """Resolve a dotted module name to a first-party file under the roots."""
        if not module:
            return None
        rel = module.replace('.', os.sep)
        for base in base_dirs:
            for candidate in (os.path.join(base, rel + '.py'), os.path.join(base, rel, '__init__.py')):
                if os.path.isfile(candidate):
                    resolved = os.path.abspath(candidate)
                    if self._is_first_party(resolved):
                        return resolved
        return None

    def _is_first_party(self, path: str) -> bool:
        ap = os.path.abspath(path)
        parts = ap.split(os.sep)
        if any(name in _VENDORED_DIR_NAMES for name in parts):
            return False
        for prefix in self._excluded_prefixes:
            if self._is_within(ap, prefix):
                return False
        return any(self._is_within(ap, root) for root in self.roots)

    @staticmethod
    def _is_within(path: str, parent: str) -> bool:
        try:
            return os.path.commonpath([path, parent]) == parent
        except ValueError:
            return False

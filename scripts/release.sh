#!/bin/bash
set -euo pipefail

# Run from repo root (so build/ and dist/ are at top level)
cd "$(dirname "$0")/.."

require_clean_git_state() {
  # No staged/unstaged/untracked changes.
  if [[ -n "$(git status --porcelain)" ]]; then
    echo "Error: repo has uncommitted changes. Commit/stash first." >&2
    git status --short
    exit 1
  fi

  # Ensure branch has an upstream and is not ahead (unpushed commits).
  if ! git rev-parse --abbrev-ref --symbolic-full-name "@{upstream}" >/dev/null 2>&1; then
    echo "Error: current branch has no upstream. Set upstream before release." >&2
    exit 1
  fi

  git fetch --quiet
  ahead_count="$(git rev-list --count "@{upstream}..HEAD")"
  if [[ "${ahead_count}" != "0" ]]; then
    echo "Error: branch has ${ahead_count} unpushed commit(s)." >&2
    exit 1
  fi
}

get_project_version() {
  uv run python - <<'PY'
import tomllib
from pathlib import Path

data = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
print(data["project"]["version"])
PY
}

verify_changelog_newer_than_last_tag() {
  uv run python - <<'PY'
import re
import subprocess
import sys
from pathlib import Path

changelog = Path("CHANGELOG.md")
if not changelog.exists():
    print("Error: CHANGELOG.md not found", file=sys.stderr)
    raise SystemExit(1)

text = changelog.read_text(encoding="utf-8")
versions = re.findall(r"^## \[(\d+\.\d+\.\d+)\]\s*$", text, flags=re.MULTILINE)
if not versions:
    print("Error: no release versions found in CHANGELOG.md", file=sys.stderr)
    raise SystemExit(1)

latest_changelog = versions[0]

tags = subprocess.run(["git", "tag", "--list"], capture_output=True, text=True, check=True).stdout.splitlines()
semver_tags: list[tuple[str, str]] = []
for tag in tags:
    match = re.fullmatch(r"v?(\d+\.\d+\.\d+)", tag.strip())
    if match:
        semver_tags.append((tag.strip(), match.group(1)))

if not semver_tags:
    print(f"Changelog latest release is {latest_changelog}; no prior semver tags found.")
    raise SystemExit(0)

def semver_key(v: str) -> tuple[int, int, int]:
    major, minor, patch = v.split(".")
    return int(major), int(minor), int(patch)

_last_tag_name, last_tag_version = max(semver_tags, key=lambda item: semver_key(item[1]))
if semver_key(latest_changelog) <= semver_key(last_tag_version):
    print(
        "Error: latest changelog version "
        f"({latest_changelog}) is not newer than last tagged version ({last_tag_version}).",
        file=sys.stderr,
    )
    raise SystemExit(1)

print(
    "Changelog check passed: "
    f"{latest_changelog} is newer than last tagged version {last_tag_version}."
)
PY
}

tag_uploaded_version() {
  local version="$1"
  if git rev-parse --verify --quiet "refs/tags/${version}" >/dev/null; then
    echo "Error: tag '${version}' already exists." >&2
    exit 1
  fi

  git tag "${version}"
  echo "Created tag ${version}"
  echo "Remember to push: git push origin ${version}"
}

require_clean_git_state
verify_changelog_newer_than_last_tag

# Remove any prior builds
rm -rf build/* dist/* 2>/dev/null || true
mkdir -p build dist

# Build (uv is used elsewhere in this project; python3 -m build also works)
uv build

# Sanity-check before upload
uv run twine check dist/*

# Upload; uses API token from ~/.pypirc
uv run twine upload dist/*

uploaded_version="$(get_project_version)"
tag_uploaded_version "${uploaded_version}"

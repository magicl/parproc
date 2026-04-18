---
name: release-file-prep
description: Prepares release files only (changelog and version files) for semantic version bumps. Use when the user asks to bump major/minor/patch versions, update release notes, or prep a release without performing any git mutations.
---
# Release File Prep

## Scope

Prepare release metadata in files only:
- `CHANGELOG.md`
- `pyproject.toml`
- `uv.lock` (when present and version is pinned there)

Do not perform release publishing or git history operations.

## Hard Safety Rule

Never run git-mutating commands. Forbidden commands include:
- `git add`
- `git commit`
- `git tag` (create/delete)
- `git push`
- `git pull`
- `git fetch`
- `git merge`
- `git rebase`
- `git reset`
- `git checkout` / `git switch` that changes state

Read-only git commands are allowed (for example `git tag --list` or `git status`).

## Inputs

Collect/confirm:
1. Bump type: `major`, `minor`, or `patch`
2. Release-note summary bullets for the new version

If bump type is not provided, ask for it.

## Version Selection Algorithm

1. Read local tags (no fetch): `git tag --list`
2. Keep only semver tags matching:
   - `X.Y.Z`
   - `vX.Y.Z`
3. Pick the highest semantic version.
4. Compute next version:
   - `major`: `X+1.0.0`
   - `minor`: `X.Y+1.0`
   - `patch`: `X.Y.Z+1`
5. If no semver tags exist, use current `pyproject.toml` version as base.

## File Update Steps

1. Update `pyproject.toml`:
   - Set `[project].version` to the computed next version.
2. Update `CHANGELOG.md`:
   - Insert a new top section: `## [<next-version>]`
   - Add concise `Added` / `Changed` / `Fixed` bullets from user-provided summary.
3. If `uv.lock` contains a `parproc` package entry version, update it to match.

## Validation

After edits, verify consistency:
- `pyproject.toml` version matches changelog heading version.
- If updated, `uv.lock` `parproc` version matches too.

Recommended checks:
- `rg '^## \\\\[<version>\\\\]' CHANGELOG.md`
- `rg 'version = \"<version>\"' pyproject.toml uv.lock`

## Output to User

Report:
- Chosen base version and bump type
- Computed next version
- Files updated
- Explicit note: no git mutation or release publish performed

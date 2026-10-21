---
name: qkafka-release
description: >-
  Release procedure for qKafka NuGet packages (qKafka, plus qKafka.EntityFrameworkCore and qKafka.Dashboard once they exist): version bump, changelog,
  Release build with zero warnings, tests, pack via Makefile, tag, publish. Activate when asked to release, version or publish qKafka.
---

# qKafka Release

Mode: `ai-method/modes/04-release-mode.md`. Packages: `src/qKafka` (single package; MIT). Branch: `main`. Publishing and pushing tags are outward-facing: **confirm with the user before each of them.**

## Checklist

1. **Clean tree on `main`**: `git status`, `git pull`.
2. **ROADMAP**: the phase being released has its checkboxes ticked only for what is really done.
3. **Version**: set the version in `Directory.Build.props` (`VersionPrefix`, `VersionSuffix` for previews). Semver: patch = bug fixes only, minor = features, major = breaking API.
4. **CHANGELOG.md**: move `## [Unreleased]` to `## [X.Y.Z] - YYYY-MM-DD` (Added / Changed / Fixed / Security). Create the file if missing.
5. **Docs truth**: run `qkafka-docs-truth`; README install snippet and options match the code.
6. **Gate**:
   ```bash
   dotnet format --verify-no-changes
   dotnet build -c Release     # zero warnings
   dotnet test -c Release
   ```
7. **Pack**: `make pack` (or `make pack VERSION_PREFIX=X.Y.Z`). Output is `./nupkg/` (git-ignored). Check that the three `.nupkg` exist, with README, license and XML docs.
8. **Commit and tag** (after user confirmation): `chore(release): vX.Y.Z`, `git tag vX.Y.Z`.
9. **Publish** (after user confirmation): `NUGET_API_KEY` must be set in the environment; `make publish`. Never print or store the key.
10. **Push** the commit and tag, then create the GitHub release from the changelog entry.
11. **Verify**: packages visible on NuGet, a fresh project can `dotnet add package qKafka`.

## Rules

- A failing gate blocks the release; do not skip tests or warnings.
- Packages share one version number.
- Never republish an existing version (`--skip-duplicate` hides it; bump instead).

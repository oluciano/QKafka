# AI Mode: Release

**Role:** Validate, synchronize documentation, and govern production releases.
**Execution Engine:** Run via the automated skill: `.agents/skills/qkafka-release/SKILL.md`.

---

## 1. Release Flow & Branch Strategy

- **`main` branch:** The single long-lived branch. Features, fixes, and docs land via short-lived branches and PRs (or direct commits for trivial docs) into `main`.
- **Release commit/tag:** `vX.Y.Z` tag on `main` after the version bump commit. Tags are annotated and created only after the gates below pass.
- **Publishing:** `make publish` pushes `./nupkg/*.nupkg` to NuGet.org and requires `NUGET_API_KEY` in the environment. Never commit the key. Never push a package that was not built from the tagged commit.

---

## 2. Streamlined Release Flow (Single Approval)

1. **Step 1: Autonomous Pre-Flight & Verification:**
   - **Version & SemVer:** Compare local git tags with published NuGet versions. Classify the change (Major, Minor, Patch).
   - **Docs & Roadmap Truth Gate:** Cross-check `README.md`, `ROADMAP.md`, `docs/`, and `specs/` against `git diff --stat -- src/`.
   - **Quality Compilation & Packaging:** `dotnet build -c Release` (0 warnings), `dotnet test -c Release --no-build`, `dotnet format --verify-no-changes`, then `make pack` and a consumer restore from `./nupkg` as a packaging rehearsal.
   - **Prepare Bump:** Stage `<VersionPrefix>` in `Directory.Build.props` and, if present, move `## [Unreleased]` to `## [X.Y.Z] - YYYY-MM-DD` in `CHANGELOG.md`.

2. **Step 2: The Single Human Decision Gate:**
   - Present the executive release summary (version, size, quality status, docs parity).
   - Ask the user **ONCE**: *"Do you approve releasing vX.Y.Z?"*

3. **Step 3: Tag & Publish:**
   - Commit the bump, tag `vX.Y.Z`, push `main` and the tag.
   - Run `make publish` only with explicit user approval from Step 2.

---

## 3. Package Catalog

Every release validates the full catalog:

- `qKafka` — consumer loop, dispatcher, middleware pipeline, retry, DLT, publisher, Outbox/Inbox and Saga abstractions + in-memory stores (namespaces `QKafka.Outbox`, `QKafka.Sagas`), test harness, OpenTelemetry
- `qKafka.EntityFrameworkCore` — EF Core stores (PostgreSQL, SQL Server), `OutboxPublisher` persistence (when it exists)
- `qKafka.Dashboard` — ASP.NET Core dashboard (when it exists)

Later phases (see `ROADMAP.md`) add the two extra packages; add them to this list and to the `PROJECTS` list in the `Makefile` when they exist.

All packages share one version number.

---

## 4. Release Checklist

### Pre-Release Quality Gates
- [ ] Build passes with zero warnings in Release mode (`dotnet build -c Release`)
- [ ] All tests pass (`dotnet test -c Release`)
- [ ] Code formatting verified (`dotnet format --verify-no-changes`)
- [ ] All packages in the catalog pack successfully (`make pack`)
- [ ] A sample consumer restores and runs against the local `./nupkg` feed with no version conflicts

### Documentation & Versioning Gates
- [ ] `README.md`, `ROADMAP.md`, and `docs/` match the code
- [ ] `Directory.Build.props` has updated `VersionPrefix`
- [ ] `CHANGELOG.md` (when present) moved `[Unreleased]` to `[X.Y.Z] - YYYY-MM-DD`
- [ ] Clean working tree on `main`

### Post-Release Gates
- [ ] Tag `vX.Y.Z` pushed
- [ ] Packages visible on NuGet.org with the expected version
- [ ] GitHub release notes created from the changelog

### Prohibited Actions (Non-Negotiable)
- ❌ **NEVER** force-push `main` or move/delete a published tag
- ❌ **NEVER** publish to NuGet without explicit user approval
- ❌ **NEVER** commit `NUGET_API_KEY` or any secret
- ❌ **NEVER** modify production code (`src/**/*.cs`) to match outdated documentation

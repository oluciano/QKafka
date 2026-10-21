# Workflow: Documentation & Changelog Truth

**When:** Updating documentation (`docs/`, `README.md`, `ARCHITECTURE.md`, `specs/`), updating package `README.md` files, adding `CHANGELOG.md` entries, or preparing a release truth gate.
**Execution Skills:** Governed by `.agents/skills/qkafka-wiki-guardian/SKILL.md` (curation & architecture) and `.agents/skills/qkafka-docs-truth/SKILL.md` (truth gate).

---

## 1. Golden Rules of Documentation

1. **Code is the Single Source of Truth:** Documentation describes what the code actually does, not what was intended or imagined.
2. **Compile What You Document:** Every C# snippet in `docs/` or READMEs must be valid, modern C# that compiles against real packages without phantom APIs.
3. **Verify Claims in Code First:** Grep `src/qKafka` (including `Outbox/` and `Sagas/`) and `src/qKafka.EntityFrameworkCore` before writing claims into docs or issues.
4. **Single Canonical Changelog:** Maintain root `CHANGELOG.md` only; do not duplicate it elsewhere in `docs/`.
5. **No Repo-Relative Links in CHANGELOG:** Use plain text for paths (e.g. `src/qKafka/KafkaConsumerLoop.cs`) in `CHANGELOG.md`. Relative links like `[path](src/...)` break future site builds.
6. **Always Record Dependency Bumps:** Every library dependency bump (e.g. `Confluent.Kafka`, `Npgsql`, `Microsoft.EntityFrameworkCore`) must be listed under `### Dependencies` in `CHANGELOG.md`.
7. **SemVer & Milestone Alignment:**
   - **PATCH (`vX.Y.Z`):** Only bug fixes under `### Fixed`.
   - **MINOR (`vX.Y.0`):** New features, options, or behavior changes under `### Added` or `### Changed`.

---

## 2. Pre-Flight & Scope Determination

Identify the scope of the documentation task:
- **PR Scope (`--scope diff`):** Validating changes introduced in the current branch against `main`.
- **Release Scope (`--scope full`):** Complete release gate auditing all touched packages, docs pages, and full `CHANGELOG.md` against `<last-tag>...HEAD`.

---

## 3. Changelog ↔ Diff Cross-Check (Auditoria Cruzada)

Before finalizing docs, run a bidirectional audit between `git diff` and `CHANGELOG.md`:

```bash
# View all modified files across src/ and packages
git diff origin/main...HEAD --stat -- src/
```

### Checks to Perform:
1. **Omissions Check (Diff → Changelog):**
   - Did any public API, option, behavior, or package configuration change in `src/` without a corresponding bullet in `CHANGELOG.md`?
   - Were `.csproj` dependencies bumped without being listed under `### Dependencies`?
2. **Ghost / Duplicate Check (Changelog → Diff):**
   - Does every bullet point in `## [Unreleased]` correspond to an actual change in the diff?
   - Are any items duplicated across different categories (e.g. mentioned in both `### Added` and `### Fixed`)?
3. **SemVer Guard:**
   - Are any new features or breaking behavioral changes listed under a patch release?
4. **Markdown Link Linting:**
   - Ensure zero repo-relative markdown links (`[text](src/...)` or `[text](../...)`) exist in `CHANGELOG.md`.

---

## 4. Docs Truth Gate

No automated tool exists yet (see ROADMAP "Quality, Testing & Docs"). Verify manually:
- Every public option in `QKafkaOptions`, `ConsumerOptions`, `RetryOptions` is documented, with the real default.
- Every metric/activity emitted is documented once OTel lands.
- Every C# snippet compiles against the current packages.
- No stale terms (job, dispatcher-as-queue, MassTransit-style queue wording) remain.

*Exit criteria:* zero drift found.

---

## 5. Semantic Review & Doc Mapping

For each changed component, verify and update the corresponding documentation:

| Component Touched | Primary Doc | Package README |
| :--- | :--- | :--- |
| Consumer loop / Dispatcher / Pipeline | `docs/mental-model.md`, `ARCHITECTURE.md` | `src/qKafka/README.md` |
| Producer / Outbox / Inbox | `docs/database-schema.md`, `docs/getting-started.md` | `src/qKafka/Outbox/README.md` |
| Sagas | `docs/mental-model.md`, `specs/03-sagas.md` | `src/qKafka/Sagas/README.md` |
| Dashboard & UI | `specs/04-dashboard.md` | `src/qKafka.Dashboard/README.md` |
| Test Harness | `specs/05-test-harness.md` | `src/qKafka.Testing/README.md` |

---

## 6. Link Check

Verify no broken relative links in `README.md`, `ROADMAP.md`, `docs/` and `specs/` (a MkDocs site is planned; until then check by hand).

---

## 7. Exit Criteria

- [ ] All code snippets compile and reflect modern C# (.NET 8 and .NET 10)
- [ ] Bidirectional audit complete: no omitted changes, no ghost/duplicate entries
- [ ] No repo-relative links in `CHANGELOG.md`
- [ ] All dependency bumps listed under `### Dependencies`
- [ ] Docs truth gate: 0 drift
- [ ] No broken links

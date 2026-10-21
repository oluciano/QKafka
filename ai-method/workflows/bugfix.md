# Workflow: Bugfix

**When:** Fixing a reported bug, unexpected exception, race condition, rebalance problem, lost/duplicated message, or behavioral defect.

---

## 1. Pre-Flight
- **WIP Limit = 1.**
- **Warmup Check:** confirm on `main` that the defect still reproduces.
- **SemVer:** bug fixes go to a PATCH (`vX.Y.Z`). If the fix needs a new public API or a behavior change, escalate to a MINOR.
- **Branch Isolation:** `git checkout -b bugfix/<id>-<description>` off `main`.

---

## 2. Root Cause Analysis & Red-First Proof
- Identify the exact root cause: logic error, invariant breach (see foundation), race between poll thread and workers, commit-order bug, deadlock.
- **Mandatory reproduction test** before touching production code.
- **Red-First Proof:** watch it fail with the specific error; commit the red test alone (`test(scope): reproduce <bug>, red`).
- **No Fixed Sleeps** (`Task.Delay` banned; use `TestWait.UntilAsync`, `TaskCompletionSource`).
- **3N Matrix:**
  - **N1 — Positive:** normal flow still works.
  - **N2 — Negative:** the bug scenario is handled without crash, lost offset or state corruption.
  - **N3 — Boundary/Input:** null, empty, cancellation, extreme bounds handled cleanly.

---

## 3. Minimal Atomic Fix
- Mode: `ai-method/modes/02-execution-mode.md`.
- Smallest safe change. No refactoring of unrelated code.
- Respect invariants: storage as truth, heartbeat never delayed, key ordering, atomic saga+outbox.
- **Test Contract Rule:** never weaken, invert or delete an assertion.

---

## 4. Verification
- `dotnet format --verify-no-changes`
- `dotnet build -c Release` (0 warnings)
- `dotnet test tests/<TouchedProject> --no-build`
- **Self-audit:**
  ```bash
  git diff main -- tests | grep -E '^-' | grep -E 'Should\(|Assert\.|Verify\(|Times\.|\[Fact|\[Theory|Skip ='
  ```
  Every removed/changed assertion must be justified.
- **Changelog:** `## [Unreleased]` → `### Fixed`.
- **Docs:** update if documented defaults or behavior changed.

---

## 5. Exit Criteria
- [ ] Root cause documented
- [ ] Red-first test committed, green after fix
- [ ] 3N matrix satisfied, no fixed sleeps
- [ ] All tests pass, no existing test modified
- [ ] Zero warnings; `dotnet format` verified
- [ ] Test contract self-audit clean
- [ ] `CHANGELOG.md` updated

# Validation Report Template

Use this to document validation results.

---

## Task

**[Reference the task being validated]**

---

## Overall Status

- [ ] **PASS** — All checks passed, ready to merge
- [ ] **PASS WITH NOTES** — Passed, minor improvements suggested
- [ ] **CONDITIONAL PASS** — Passes if issues fixed
- [ ] **FAIL** — Blocking issues found

---

## Architecture Compliance

- [ ] Storage is source of truth (all state transitions persisted)
- [ ] Poll loop never runs business logic (heartbeat never delayed)
- [ ] DI scope isolated per message
- [ ] Order per partition key preserved
- [ ] Offset committed only after durable outcome
- [ ] Poison pills routed to DLT, offset committed
- [ ] Saga state + outbox rows committed atomically
- [ ] DLT publisher failures do not crash the loop

**Issues:** None

---

## Code Quality

- [ ] Zero compiler warnings (Release build)
- [ ] TreatWarningsAsErrors respected
- [ ] StyleCop compliant (member order, trailing commas, etc.)
- [ ] All public types and members documented (XML docs)
- [ ] No `.Result` or `.Wait()` calls (async/await used correctly)
- [ ] CancellationToken propagated everywhere
- [ ] Classes sealed by default

**Issues:** None

---

## Test Coverage

### Unit Tests
- [ ] All pass
- [ ] Deterministic
- [ ] Real behavior (no inappropriate mocks)
- [ ] Edge cases covered

**Count:** [X] tests, all pass

---

### Integration Tests
- [ ] All pass
- [ ] Real storage used
- [ ] End-to-end scenarios covered

**Count:** [X] tests, all pass

---

### Reliability Tests [if applicable]
- [ ] Real broker and DB used (Testcontainers)
- [ ] Rebalance and crash recovery verified
- [ ] Duplicate delivery handled (Inbox)
- [ ] Outbox concurrency verified

**Count:** [X] tests across [Y] databases

---

## Hidden Redesign Check

- [ ] No in-memory caches disguised as optimization
- [ ] No consumer state hiding between polls
- [ ] No storage bypass via application caching
- [ ] No synchronous blocking in async code
- [ ] No magic behavior making system unpredictable

**Findings:** None

---

## Risk Assessment

### Critical Risks (Block Merge)
- [ ] Compiler warnings
- [ ] Test failures
- [ ] StyleCop violations
- [ ] Storage bypass, commit-before-durable, or blocked poll loop

**Status:** None found

---

## Compliance Summary

| Check | Status | Notes |
|-------|--------|-------|
| Build (Release) | [ ] Pass | |
| StyleCop | [ ] Pass | |
| Architecture | [ ] Pass | |
| Tests | [ ] Pass | |
| Public API Docs | [ ] Pass | |
| Hidden Redesign | [ ] Pass | |

---

## Validator Comments

**[General observations and feedback]**

---

## Approval

- [ ] Ready to merge
- [ ] Merge with suggested improvements
- [ ] Hold for changes

**Validated by:** [AI or human validator]
**Date:** [Date]

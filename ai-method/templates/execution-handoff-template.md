# Execution Handoff Template

Use this to communicate results from execution phase to validation phase.

---

## Task

**[Reference the task executed]**

---

## Workflow Used

- [x] Feature
- [ ] Bugfix
- [ ] Test
- [ ] Refactor
- [ ] Consumer/Producer
- [ ] Saga
- [ ] Reliability
- [ ] Documentation
- [ ] Release

---

## Changes Summary

### Files Created

**src/qKafka/ClassName.cs**
- Description of what was added
- Key responsibilities

**tests/qKafka.Tests/<same path as the production file>/ClassNameUnitTests.cs**
- Test scenarios covered

### Files Modified

**src/qKafka/ExistingClass.cs**
- Line X-Y: Description of change
- Why: Reason for modification

---

## Deliverables Checklist

### Code
- [ ] Production-ready
- [ ] Zero compiler warnings
- [ ] StyleCop compliant
- [ ] Sealed classes by default
- [ ] XML docs on public APIs

### Tests
- [ ] All pass locally
- [ ] Deterministic
- [ ] Real behavior (no inappropriate mocks)
- [ ] Coverage complete

### Documentation
- [ ] CHANGELOG updated
- [ ] Public APIs documented
- [ ] Architecture alignment verified

---

## Build Results

**Build command:** `dotnet build --configuration Release`

**Status:** ✓ Passed / ✗ Failed

**Warnings:** 0

**Errors:** 0

**Notes:** (if any)

---

## Test Results

**Unit tests:** ✓ Passed (X tests)

**Integration tests:** ✓ Passed (X tests) [if applicable]

**Reliability tests:** ✓ Passed (X tests) [if applicable]

**Flaky tests:** None

---

## Architecture Compliance

### Verified
- [ ] Storage is source of truth
- [ ] Poll loop never blocks
- [ ] Order per partition key preserved
- [ ] Offset committed only after durable outcome
- [ ] Saga state + outbox committed atomically
- [ ] Poison pills routed to DLT
- [ ] No Confluent types leaked in public API
- [ ] Execution is deterministic

### Notes
[Any concerns or notes about compliance]

---

## Code Quality Verification

### StyleCop (SA Rules)
- [ ] Member Ordering (SA1202, SA1204)
- [ ] Trailing Commas (SA1413)
- [ ] Blank Lines (SA1508)

### Sonar Rules
- [ ] Variable Usage (S1481)
- [ ] Exception Handling (S2139)

### Public API
- [ ] All public types documented
- [ ] All public members documented

### Async & Concurrency
- [ ] No `.Result` or `.Wait()`
- [ ] CancellationToken propagated
- [ ] Async/await used consistently

---

## Known Issues

[List any issues, limitations, or concerns]

- None identified

---

## Ready for Validation

**Status:** ✓ YES / ✗ NO

**Notes:** [If NO, explain what's needed]

---

## Validation Checklist (for validator)

Use 03-validation-mode.md to check:

- [ ] Architecture compliance
- [ ] Code quality
- [ ] Test coverage
- [ ] Hidden redesign check
- [ ] Invariant preservation

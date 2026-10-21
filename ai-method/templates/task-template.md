# Task Template

Use this to clearly specify work for an AI execution engine.

---

## Title

**[Descriptive task title]**

---

## Context

**What is this task?**
- Feature, bugfix, consumer/producer, saga, test, refactoring, reliability test, or release

**Why are we doing this?**
- Roadmap phase, spec reference, reliability requirement, technical debt, etc.

**Scope:**
- What will change
- What will NOT change

---

## Requirements

**Must do:**
- Explicit requirement 1
- Explicit requirement 2
- Explicit requirement 3

**Must NOT do:**
- Constraint 1
- Constraint 2

**Assumptions:**
- Assumption 1
- Assumption 2

---

## Acceptance Criteria

- [ ] Criterion 1 (testable, observable)
- [ ] Criterion 2
- [ ] Criterion 3

---

## Workflow

**Use this workflow:** [feature | bugfix | consumer | saga | test | refactor | reliability | documentation | release]

**Reference architecture:** ARCHITECTURE.md, GEMINI.md, `specs/NN-*.md`, `ROADMAP.md`, `ai-method/core/00-foundation-minimal.md`

---

## Deliverables

1. Code (if applicable)
   - File path: src/qKafka/... (Outbox/, Sagas/), src/qKafka.EntityFrameworkCore/... or src/qKafka.Dashboard/...
   - Production-ready

2. Tests (if applicable)
   - File path: tests/qKafka.Tests/... (or appropriate)
   - All pass

3. Documentation (if applicable)
   - CHANGELOG update (Added / Fixed / Changed)
   - Public API docs (if new public APIs)

---

## Success Definition

Task is DONE when:
- [ ] All acceptance criteria met
- [ ] All deliverables complete
- [ ] Zero compiler warnings
- [ ] All tests pass
- [ ] Code reviewed and approved
- [ ] Ready to merge

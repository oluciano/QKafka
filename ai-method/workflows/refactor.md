# Workflow: Refactor

**When:** Improving internal code structure, maintainability, or readability without changing any observable behavior.

---

## 1. Absolute Constraint: Strict Behavioral Parity
A refactor **MUST NOT** alter observable behavior or public API contracts:
- Zero changes to public method signatures or types.
- Zero changes to database schemas (outbox/inbox/saga tables), serialization formats, header names or DLT envelope.
- All existing tests MUST pass completely unmodified.
- **Prefer Simplification Over Accumulation:** Simplify and improve existing methods instead of adding `*WithX` / `*V2` sibling methods. Leave the code with fewer lines than you found it.

---

## 2. Pre-Flight & Context Warmup Gate (Phase 1)
- **WIP Limit = 1.**
- **Branch Isolation:** `git checkout -b refactor/<id>-<description>` off `main`.

---

## 3. Execution (Phase 3)
- Mode: `ai-method/modes/02-execution-mode.md`.
- Refactor code in small, incremental steps.
- Maintain StyleCop compliance, XML documentation, and async conventions (`.ConfigureAwait(false)` in `src/qKafka*`).
- Do NOT rewrite or modify any existing passing tests.

---

## 4. Stop Conditions (Mandatory Escalation)
- An existing test breaks and requires modification → **STOP**. (This indicates behavior changed).
- A public API signature needs to change → **STOP**. (Requires architect approval).
- An outbox/inbox/saga table, Kafka header or message key format needs alteration → **STOP**.
- The change touches ordering, offset-commit or poll-loop behavior → **STOP**, use the `reliability` workflow.

---

## 5. Automated Verification & Exit Criteria
- [ ] All existing unit, integration, and reliability tests pass 100% unmodified
- [ ] Test contract self-audit verified (`git diff main -- tests` shows no dropped assertions)
- [ ] Zero compiler warnings in Release build (`TreatWarningsAsErrors = true`) on `net10.0`
- [ ] `dotnet format --verify-no-changes` passes cleanly
- [ ] Public API surface completely unchanged
